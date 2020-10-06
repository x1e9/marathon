package mesosphere.marathon
package storage.repository

import mesosphere.marathon.core.health.HealthCheckShield
import mesosphere.marathon.core.task.Task
import akka.util.ByteString
import java.nio.charset.StandardCharsets
import java.nio.ByteOrder
import java.time.Instant
import java.time.OffsetDateTime

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.unmarshalling.Unmarshaller
import mesosphere.marathon.core.storage.repository._
import mesosphere.marathon.core.storage.repository.impl.PersistenceStoreRepository
import mesosphere.marathon.core.storage.store.impl.memory.{Identity, RamId}
import mesosphere.marathon.core.storage.store.impl.zk.{ZkId, ZkSerialized}
import mesosphere.marathon.core.storage.store.{IdResolver, PersistenceStore}
import mesosphere.marathon.state.Timestamp

trait HealthCheckShieldRepository
  extends Repository[Task.Id, HealthCheckShield]

object HealthCheckShieldRepository {
  implicit val category = "HealthCheckShield"

  implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

  implicit val zkIdResolver = new IdResolver[Task.Id, HealthCheckShield, String, ZkId] {
    def toStorageId(id: Task.Id, version: Option[OffsetDateTime]): ZkId =
      ZkId(category, id.idString, version)
    def fromStorageId(key: ZkId): Task.Id = Task.Id.parse(key.id)
    val hasVersions: Boolean = false
    val category: String = HealthCheckShieldRepository.category
    def version(v: HealthCheckShield): OffsetDateTime =
      Timestamp.now().toOffsetDateTime
  }

  implicit val zkMarshaller: Marshaller[HealthCheckShield, ZkSerialized] =
    Marshaller.opaque { (shield: HealthCheckShield) =>
      val bytes = ByteString.newBuilder
      val serializationVersion = 1
      bytes.putInt(serializationVersion)
      val taskIdStrBytes = shield.taskId.idString.getBytes(StandardCharsets.UTF_8)
      bytes.putInt(taskIdStrBytes.length)
      bytes.putBytes(taskIdStrBytes)
      bytes.putLong(shield.until.toInstant.toEpochMilli)
      ZkSerialized(bytes.result)
    }

  implicit val zkUnmarshaller = Unmarshaller.strict { (zk: ZkSerialized) =>
    val it = zk.bytes.iterator
    val serializationVersion = it.getInt
    val taskIdLength = it.getInt
    val taskId = new String(it.getBytes(taskIdLength), StandardCharsets.UTF_8)
    val until = Instant.ofEpochMilli(it.getLong)
    HealthCheckShield(Task.Id.parse(taskId), Timestamp(until))
  }

  def zkRepository(persistenceStore: PersistenceStore[ZkId, String, ZkSerialized]): HealthCheckShieldRepository = {
    new HealthCheckShieldRepositoryImpl(persistenceStore)
  }

  implicit val inMemIdResolver = new IdResolver[Task.Id, HealthCheckShield, String, RamId] {
    def toStorageId(id: Task.Id, version: Option[OffsetDateTime]): RamId =
      RamId(category, id.idString, version)
    def fromStorageId(key: RamId): Task.Id = Task.Id.parse(key.id)
    val hasVersions: Boolean = false
    val category: String = HealthCheckShieldRepository.category
    def version(v: HealthCheckShield): OffsetDateTime =
      Timestamp.now().toOffsetDateTime
  }

  def inMemRepository(persistenceStore: PersistenceStore[RamId, String, Identity]): HealthCheckShieldRepository = {
    import mesosphere.marathon.storage.store.InMemoryStoreSerialization._
    new HealthCheckShieldRepositoryImpl(persistenceStore)
  }
}

class HealthCheckShieldRepositoryImpl[K, C, S](persistenceStore: PersistenceStore[K, C, S])(
    implicit
    ir: IdResolver[Task.Id, HealthCheckShield, C, K],
    marshaller: Marshaller[HealthCheckShield, S],
    unmarshaller: Unmarshaller[S, HealthCheckShield]
) extends PersistenceStoreRepository[Task.Id, HealthCheckShield, K, C, S](
  persistenceStore,
  _.taskId) with HealthCheckShieldRepository