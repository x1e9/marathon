package mesosphere.marathon
package core.health.impl

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.core.health.{HealthCheckShield, HealthCheckShieldConf}
import mesosphere.marathon.storage.repository.HealthCheckShieldRepository
import mesosphere.marathon.util.KeyedLock

import scala.concurrent.duration._
import scala.collection.concurrent.TrieMap
import scala.async.Async.{async, await}
import akka.stream.scaladsl.{Sink}
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import akka.{Done}
import scala.concurrent.ExecutionContext

// This class provides an API to create/delete/check HealthCheckShields
// 1. Shields are stored in the persistent storage and in the local cache
//    - the storage is always the source of truth
//    - there's no versioning, each shield either exists or not
// 2. isShielded(taskId) is a read path checked from HealthCheckActors
//    - it is thread-safe, fully syncrhonous, and only consults the cache
// 3. enable/disable/get shield are used form the http api (HealthCheckShieldResource)
//    - there's a fine-grained lock to allow concurrent requests on the same shield
//    - getShields returns the "storage" view of the shields
// 4. fillCacheFromStorage is used by HealthCheckShieldActor
//    - every time we become a leader the shields are re-loaded from the storage
//    - this operation is not thread-safe
// 5. purgeExpiredShields consults the storage and deletes expired shields
//    - shields will be stored until their ttl expires (we don't check if the corresponding task is dead)
//    - this operation is thread-safe
//    - it is run once after we become a leader, and periodically from HealthCheckShieldActor
class HealthCheckShieldManager(
    repository: HealthCheckShieldRepository,
    conf: HealthCheckShieldConf
)(implicit mat: ActorMaterializer, ec: ExecutionContext) extends StrictLogging {

  val shields = TrieMap.empty[Task.Id, HealthCheckShield]
  val lock = KeyedLock[String]("HealthCheckShieldManager", Int.MaxValue)

  def fillCacheFromStorage(): Future[Done] = async {
    if (conf.healthCheckShieldFeatureEnabled) {
      logger.info("[health-check-shield] Clearing and preloading the cache...")
      shields.clear()
      var count = 0
      val cacheFillSink = Sink.foreach((shield: HealthCheckShield) => {
        shields.update(shield.taskId, shield)
        count = count + 1
      })
      val done = await (repository.all().runWith(cacheFillSink))
      logger.info(s"[health-check-shield] Preloaded ${count} shields")
      done
    } else {
      Done
    }
  }

  private def tryPurgeShield(taskId: Task.Id): Future[Boolean] = {
    lock(taskId.idString) {
      repository
        .get(taskId)
        .map(_.exists(_.until < Timestamp.now()))
        .flatMap((shouldPurge: Boolean) => {
          if (shouldPurge) {
            shields.remove(taskId)
            repository.delete(taskId).map((x) => true)
          } else {
            Future.successful(false)
          }
        })
    }
  }

  def purgeExpiredShields(): Future[Done] = async {
    if (conf.healthCheckShieldFeatureEnabled) {
      logger.info("[health-check-shield] Purging expired shields...")
      var purgedCount = 0
      val parallelismDegree = 2
      val purgeFuture = repository.ids() // we will read the value later, under the same lock as delete
        .mapAsync(parallelismDegree)((taskId: Task.Id) => {
          tryPurgeShield(taskId)
        })
        .runForeach((purged: Boolean) => {
          if (purged) {
            purgedCount = purgedCount + 1
          }
        })
      await (purgeFuture)
      logger.info(s"[health-check-shield] Purged ${purgedCount} shields")
      Done
    } else {
      Done
    }
  }

  def isShielded(taskId: Task.Id): Boolean = {
    if (conf.healthCheckShieldFeatureEnabled) {
      shields.get(taskId)
        .exists(shield => {
          if (Timestamp.now() < shield.until) {
            true
          } else {
            logger.info(s"[health-check-shield] Shield expired for ${taskId}")
            shields.remove(shield.taskId, shield)
            false
          }
        })
    } else {
      false
    }
  }

  def enableShield(taskId: Task.Id, duration: FiniteDuration): Future[Done] = {
    if (conf.healthCheckShieldFeatureEnabled) {
      lock(taskId.idString) {
        val shield = HealthCheckShield(taskId, Timestamp.now() + duration)
        shields.update(taskId, shield)
        val storeFuture = repository.store(shield)
        logger.info(s"[health-check-shield] Shield updated for ${taskId}")
        storeFuture
      }
    } else {
      Future.successful(Done)
    }
  }

  def disableShield(taskId: Task.Id): Future[Done] = {
    if (conf.healthCheckShieldFeatureEnabled) {
      lock(taskId.idString) {
        shields.remove(taskId)
        val deleteFuture = repository.delete(taskId)
        logger.info(s"[health-check-shield] Shield deleted for ${taskId}")
        deleteFuture
      }
    } else {
      Future.successful(Done)
    }
  }

  def getShields(): Future[Seq[HealthCheckShield]] = {
    if (conf.healthCheckShieldFeatureEnabled) {
      repository.all().runWith(Sink.seq)
    } else {
      Future.successful(Seq.empty[HealthCheckShield])
    }
  }
}
