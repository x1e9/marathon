package mesosphere.mesos

import java.time.Clock

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.RichClock
import mesosphere.marathon.state.Timestamp
import org.apache.mesos.Protos.{DurationInfo, Offer}

import scala.concurrent.duration._

object Availability extends StrictLogging {

  def offerAvailable(offer: Offer, drainingTime: FiniteDuration)(implicit clock: Clock): Boolean = {
    logger.trace(s"Studiying offer [${offer.getId.getValue}] availability: start available check\n")
    val now = clock.now()
    if (offerHasUnavailability(offer)) {
      logger.trace(s"Studiying offer [${offer.getId.getValue}] availability: offer has unavailability info\n")
      val start: Timestamp = offer.getUnavailability.getStart

      if (currentlyInDrainingState(now, start, drainingTime)) {
        logger.trace(s"Studiying offer [${offer.getId.getValue}] availability: offer is about a slave currently in draining state\n")
        isAgentOutsideUnavailabilityWindow(offer, start, now)
      } else true
    } else true
  }

  private def currentlyInDrainingState(now: Timestamp, start: Timestamp, drainingTime: FiniteDuration) = {
    now.after(start - drainingTime)
  }

  private def offerHasUnavailability(offer: Offer) = {
    logger.trace(s"Studiying offer [${offer.getId.getValue}] availability: hasUnavailability: ${offer.hasUnavailability}\n")
    offer.hasUnavailability && offer.getUnavailability.hasStart
  }

  private def isAgentOutsideUnavailabilityWindow(offer: Offer, start: Timestamp, now: Timestamp) = {
    offer.getUnavailability.hasDuration && now.after(start + offer.getUnavailability.getDuration.toDuration)
  }

  /**
    * Convert Mesos DurationInfo to FiniteDuration.
    *
    * @return FiniteDuration for DurationInfo
    */
  implicit class DurationInfoHelper(val di: DurationInfo) extends AnyVal {
    def toDuration: FiniteDuration = FiniteDuration(di.getNanoseconds, NANOSECONDS)
  }

}
