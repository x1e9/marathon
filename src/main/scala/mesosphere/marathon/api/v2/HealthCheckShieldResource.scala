package mesosphere.marathon
package api.v2

import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.container.{AsyncResponse, Suspended}
import javax.ws.rs.core.Response.Status
import javax.ws.rs.core.Response
import javax.ws.rs.core.{Context, MediaType}
import mesosphere.marathon.api._
import mesosphere.marathon.core.health.HealthCheckManager
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.plugin.auth.{Authenticator, Authorizer}
import mesosphere.marathon.raml.Raml
import mesosphere.marathon.raml.HealthConversion._
import mesosphere.marathon.api.RestResource.RestStreamingBody
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.plugin.auth._

import scala.async.Async._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

@Path("v2/shield")
@Produces(Array(MediaType.APPLICATION_JSON))
class HealthCheckShieldResource @Inject() (
    val config: MarathonConf,
    val healthCheckManager: HealthCheckManager,
    val groupManager: GroupManager,
    val authenticator: Authenticator,
    val authorizer: Authorizer)(implicit val executionContext: ExecutionContext) extends AuthResource with StrictLogging {

  @GET
  def index(
    @Context req: HttpServletRequest, @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      if (config.healthCheckShieldFeatureEnabled) {
        implicit val identity = await(authenticatedAsync(req))
        val shields = await (healthCheckManager.listShields())
        ok(Raml.toRaml(shields))
      } else {
        featureDisabledError()
      }
    }
  }

  @PUT
  @Path("{taskId}")
  def putHealthShield(
    @PathParam("taskId") taskId: String,
    @QueryParam("duration")@DefaultValue("30 minutes") duration: String,
    @Context req: HttpServletRequest, @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      if (config.healthCheckShieldFeatureEnabled) {
        val parsedTaskId = Task.Id.parse(taskId)
        val maybeRunSpec = groupManager.runSpec(parsedTaskId.runSpecId)

        if (maybeRunSpec.isDefined) {
          implicit val identity = await(authenticatedAsync(req))
          if (config.healthCheckShieldAuthorizationEnabled) {
            checkAuthorization(UpdateRunSpec, maybeRunSpec.get)
          }

          val parsedDuration = Duration(duration)
          if (parsedDuration.isFinite() && parsedDuration < config.healthCheckShieldMaxDuration) {
            val ttl = parsedDuration.asInstanceOf[FiniteDuration]
            await (healthCheckManager.enableShield(parsedTaskId, ttl))
            ok()
          } else {
            Response.status(Status.BAD_REQUEST).entity(new RestStreamingBody(raml.Error(s"The duration should be finite and less than ${config.healthCheckShieldMaxDuration.toMinutes} minutes"))).build()
          }
        } else {
          unknownApp(parsedTaskId.runSpecId)
        }
      } else {
        featureDisabledError()
      }
    }
  }

  @DELETE
  @Path("{taskId}")
  def removeHealthShield(
    @PathParam("taskId") taskId: String,
    @Context req: HttpServletRequest, @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      if (config.healthCheckShieldFeatureEnabled) {
        val parsedTaskId = Task.Id.parse(taskId)

        // We should be able to delete the shield with the API even after the app and its runspec are deleted
        // As we can't authorize something that doesn't exist, delete the shield of unexistent app is allowed after authentication
        val maybeRunSpec = groupManager.runSpec(parsedTaskId.runSpecId)
        implicit val identity = await(authenticatedAsync(req))
        if (maybeRunSpec.isDefined && config.healthCheckShieldAuthorizationEnabled) {
          checkAuthorization(UpdateRunSpec, maybeRunSpec.get)
        }

        await (healthCheckManager.disableShield(parsedTaskId))
        ok()
      } else {
        featureDisabledError()
      }
    }
  }

  def featureDisabledError(): Response = {
    Response.status(Status.FORBIDDEN).entity(new RestStreamingBody(raml.Error("HealthCheckShield feature is disabled in Marathon config"))).build()
  }
}
