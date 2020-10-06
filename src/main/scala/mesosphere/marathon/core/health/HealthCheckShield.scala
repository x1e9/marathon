package mesosphere.marathon
package core.health

import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.core.task.Task

case class HealthCheckShield(taskId: Task.Id, until: Timestamp) {
}