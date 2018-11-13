package mesosphere.marathon
package core.launchqueue.impl

import java.util.concurrent.TimeUnit

import mesosphere.UnitTest
import mesosphere.marathon.test.SettableClock
import mesosphere.marathon.state.{AbsolutePathId, AppDefinition, BackoffStrategy}

import scala.concurrent.duration._

class RateLimiterTest extends UnitTest {

  val clock = SettableClock.ofNow()

  "RateLimiter" should {
    "addDelay" in {
      val limiter = new RateLimiter(clock)
      val app = AppDefinition(id = AbsolutePathId("/test"), role = "*", backoffStrategy = BackoffStrategy(backoff = 10.seconds))

      limiter.addDelay(app)

      limiter.getDelay(app.configRef).value.deadline should be (clock.now() + 10.seconds)
    }

    "addDelay for existing delay" in {
      val limiter = new RateLimiter(clock)
      val app = AppDefinition(id = AbsolutePathId("/test"), role = "*", backoffStrategy = BackoffStrategy(backoff = 10.seconds, factor = 2.0))

      limiter.addDelay(app) // linter:ignore:IdenticalStatements
      limiter.addDelay(app)
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + 20.seconds)
    }

    "backoff delay can reach maximum backoff when below 2 and above 1.1" in {
      val limiter = new RateLimiter(clock)
      val backoffStrategy = BackoffStrategy(backoff = 100.seconds, factor = 1.2, maxLaunchDelay = 1.hour)
      val app = AppDefinition(id = AbsolutePathId("/test"), role = "*", backoffStrategy = backoffStrategy)
      limiter.decreaseDelay(app) // linter:ignore:IdenticalStatements
      // if no delay has been added at first, it should keep the delay as it is
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoffStrategy.backoff)
      for (_ <- 1 to 1000) {
        limiter.decreaseDelay(app)
        limiter.addDelay(app)
      }
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoffStrategy.maxLaunchDelay)
    }

    "maximum delay should be reached when factor under 1.1 and above 1 " in {
      val limiter = new RateLimiter(clock)
      val backoffStrategy = BackoffStrategy(backoff = 100.seconds, factor = 1.05, maxLaunchDelay = 1.hour)
      val app = AppDefinition(id = AbsolutePathId("/test"), role = "*", backoffStrategy = backoffStrategy)
      limiter.decreaseDelay(app) // linter:ignore:IdenticalStatements
      // if no delay has been added at first, it should keep the delay as it is
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoffStrategy.backoff)
      for (_ <- 1 to 1000) {
        limiter.decreaseDelay(app)
        limiter.addDelay(app)
      }
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoffStrategy.maxLaunchDelay)
    }

    "maximum delay should be never reached when factor 1" in {
      val limiter = new RateLimiter(clock)
      val backoffStrategy = BackoffStrategy(backoff = 100.seconds, factor = 1, maxLaunchDelay = 1.hour)
      val app = AppDefinition(id = AbsolutePathId("/test"), role = "*", backoffStrategy = backoffStrategy)
      limiter.decreaseDelay(app) // linter:ignore:IdenticalStatements
      // if no delay has been added at first, it should keep the delay as it is
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoffStrategy.backoff)
      for (_ <- 1 to 1000) {
        limiter.decreaseDelay(app)
        limiter.addDelay(app)
      }
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoffStrategy.backoff)
    }

    "backoff delay can reach maximum backoff when under 2" in {
      val limiter = new RateLimiter(clock)
      val backoffStrategy = BackoffStrategy(backoff = 100.seconds, factor = 2, maxLaunchDelay = 1.hour)
      val app = AppDefinition(id = AbsolutePathId("/test"), role = "*", backoffStrategy = backoffStrategy)
      limiter.decreaseDelay(app) // linter:ignore:IdenticalStatements
      // if no delay has been added at first, it should keep the delay as it is
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoffStrategy.backoff)
      for (_ <- 1 to 1000) {
        limiter.decreaseDelay(app)
        limiter.addDelay(app)
      }
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoffStrategy.maxLaunchDelay)
    }

    "reduceDelay for existing delay" in {
      val limiter = new RateLimiter(clock)
      val backoff = 100.seconds
      val factor = 2.0
      val app = AppDefinition(id = AbsolutePathId("/test"), role = "*", backoffStrategy = BackoffStrategy(backoff = backoff, factor = factor))
      limiter.decreaseDelay(app) // linter:ignore:IdenticalStatements
      // if no delay has been added at first, it should keep the delay as it is
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoff)
      limiter.addDelay(app)
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + (backoff * factor.toInt))
      limiter.decreaseDelay(app)
      val time = FiniteDuration(((backoff * factor).toNanos * ((1 - 1 / factor.toDouble) * 1.1)).toLong, TimeUnit.NANOSECONDS)
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + time)
    }

    "reduceDelay never goes under minimum backoff" in {
      val limiter = new RateLimiter(clock)
      val backoff = 100.seconds
      val factor = 5.0
      val app = AppDefinition(id = AbsolutePathId("/test"), role = "*", backoffStrategy = BackoffStrategy(backoff = backoff, factor = factor))
      limiter.decreaseDelay(app) // linter:ignore:IdenticalStatements
      // if no delay has been added at first, it should keep the delay as it is
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoff)
      for (_ <- 1 to 1000) {
        limiter.decreaseDelay(app)
      }
      limiter.getDelay(app.configRef).value.deadline should be(clock.now() + backoff)
    }

    "cleanUpOverdueDelays" in {
      val time_origin = clock.now()
      val limiter = new RateLimiter(clock)
      val threshold = 60.seconds

      val appWithOverdueDelay = AppDefinition(
        id = AbsolutePathId("/overdue"),
        role = "*",
        backoffStrategy = BackoffStrategy(backoff = 10.seconds, maxLaunchDelay = threshold))
      limiter.addDelay(appWithOverdueDelay)

      val appWithValidDelay = AppDefinition(
        id = AbsolutePathId("/valid"),
        role = "*",
        backoffStrategy = BackoffStrategy(backoff = 20.seconds, maxLaunchDelay = threshold + 10.seconds))
      limiter.addDelay(appWithValidDelay)

      // after advancing the clock by (threshold + 1), the existing delays
      // with maxLaunchDelay < (threshold + 1) should be gone
      clock.advanceBy(threshold + 1.seconds)
      limiter.cleanUpOverdueDelays()
      limiter.getDelay(appWithOverdueDelay.configRef) shouldBe empty
      limiter.getDelay(appWithValidDelay.configRef).value.deadline should be(time_origin + 20.seconds)
    }

    "resetDelay" in {
      val limiter = new RateLimiter(clock)
      val app = AppDefinition(id = AbsolutePathId("/test"), role = "*", backoffStrategy = BackoffStrategy(backoff = 10.seconds))

      limiter.addDelay(app)
      limiter.resetDelay(app)

      limiter.getDelay(app.configRef) shouldBe empty
    }
  }
}
