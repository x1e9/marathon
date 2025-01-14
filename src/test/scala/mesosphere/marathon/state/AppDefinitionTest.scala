package mesosphere.marathon
package state

import mesosphere.UnitTest
import mesosphere.marathon.Protos.ServiceDefinition
import mesosphere.marathon.core.pod.{BridgeNetwork, ContainerNetwork}
import mesosphere.marathon.raml.Resources
import mesosphere.marathon.state.EnvVarValue._
import mesosphere.marathon.stream.Implicits._
import org.apache.mesos.{Protos => mesos}

import scala.concurrent.duration._

class AppDefinitionTest extends UnitTest {

  val fullVersion = VersionInfo.forNewConfig(Timestamp(1))
  val runSpecId = AbsolutePathId("/test")

  "AppDefinition" should {
    "ToProto with port definitions" in {
      val app1 = AppDefinition(
        id = AbsolutePathId("/play"),
        role = "*",
        cmd = Some("bash foo-*/start -Dhttp.port=$PORT"),
        resources = Resources(cpus = 4.0, mem = 256.0),
        instances = 5,
        portDefinitions = PortDefinitions(8080, 8081),
        executor = "//cmd",
        acceptedResourceRoles = Set("a", "b")
      )

      val proto1 = app1.toProto
      assert("/play" == proto1.getId)
      assert(proto1.getCmd.hasValue)
      assert(proto1.getCmd.getShell)
      assert("bash foo-*/start -Dhttp.port=$PORT" == proto1.getCmd.getValue)
      assert(5 == proto1.getInstances)
      assert(Seq(8080, 8081) == proto1.getPortDefinitionsList.map(_.getNumber))
      assert("//cmd" == proto1.getExecutor)
      assert(4 == getScalarResourceValue(proto1, "cpus"), 1e-6)
      assert(256 == getScalarResourceValue(proto1, "mem"), 1e-6)
      assert("bash foo-*/start -Dhttp.port=$PORT" == proto1.getCmd.getValue)
      assert(!proto1.hasContainer)
      assert(1.0 == proto1.getUpgradeStrategy.getMinimumHealthCapacity)
      assert(1.0 == proto1.getUpgradeStrategy.getMaximumOverCapacity)
      assert(proto1.hasAcceptedResourceRoles)
      assert(proto1.getAcceptedResourceRoles == Protos.ResourceRoles.newBuilder().addRole("a").addRole("b").build())

      val app2 = AppDefinition(
        id = AbsolutePathId("/play"),
        role = "*",
        cmd = None,
        args = Seq("a", "b", "c"),
        container = Some(Container.Docker(image = "group/image")),
        resources = Resources(cpus = 4.0, mem = 256.0),
        instances = 5,
        portDefinitions = PortDefinitions(8080, 8081),
        executor = "//cmd",
        upgradeStrategy = UpgradeStrategy(0.7, 0.4)
      )

      val proto2 = app2.toProto
      assert("/play" == proto2.getId)
      assert(!proto2.getCmd.hasValue)
      assert(!proto2.getCmd.getShell)
      proto2.getCmd.getArgumentsList should contain theSameElementsInOrderAs Seq("a", "b", "c")
      assert(5 == proto2.getInstances)
      assert(Seq(8080, 8081) == proto2.getPortDefinitionsList.map(_.getNumber))
      assert("//cmd" == proto2.getExecutor)
      assert(4 == getScalarResourceValue(proto2, "cpus"), 1e-6)
      assert(256 == getScalarResourceValue(proto2, "mem"), 1e-6)
      assert(proto2.hasContainer)
      assert(0.7 == proto2.getUpgradeStrategy.getMinimumHealthCapacity)
      assert(0.4 == proto2.getUpgradeStrategy.getMaximumOverCapacity)
      assert(0 == proto2.getAcceptedResourceRoles.getRoleCount)
    }

    "CMD to proto and back again" in {
      val app = AppDefinition(
        id = AbsolutePathId("/play"),
        role = "*",
        cmd = Some("bash foo-*/start -Dhttp.port=$PORT"),
        versionInfo = fullVersion
      )

      val proto = app.toProto
      proto.getId should be("/play")
      proto.getCmd.hasValue should be(true)
      proto.getCmd.getShell should be(true)
      proto.getCmd.getValue should be("bash foo-*/start -Dhttp.port=$PORT")

      val read = AppDefinition(id = runSpecId, role = "*").mergeFromProto(proto)
      read should be(app)
    }

    "ARGS to proto and back again" in {
      val app = AppDefinition(
        id = AbsolutePathId("/play"),
        role = "*",
        args = Seq("bash", "foo-*/start", "-Dhttp.port=$PORT"),
        versionInfo = fullVersion
      )

      val proto = app.toProto
      proto.getId should be("/play")
      proto.getCmd.hasValue should be(true)
      proto.getCmd.getShell should be(false)
      proto.getCmd.getValue should be("bash")
      proto.getCmd.getArgumentsList should contain theSameElementsInOrderAs Seq("bash", "foo-*/start", "-Dhttp.port=$PORT")

      val read = AppDefinition(id = runSpecId, role = "*").mergeFromProto(proto)
      read should be(app)
    }

    "app w/ basic container network to proto and back again" in {
      val app = AppDefinition(
        id = AbsolutePathId("/app-with-ip-address"),
        role = "*",
        cmd = Some("sleep 30"),
        portDefinitions = Nil,
        networks = Seq(
          ContainerNetwork(
            name = "whatever",
            labels = Map(
              "foo" -> "bar",
              "baz" -> "buzz"
            )
          )
        )
      )

      val proto = app.toProto
      proto.getId should be("/app-with-ip-address")
      assert(proto.getNetworksCount > 0)

      val read = AppDefinition(id = runSpecId, role = "*").mergeFromProto(proto)
      read should be(app)
    }

    "app to proto and back again w/ Docker container w/ virtual networking" in {
      val app = AppDefinition(
        id = AbsolutePathId("/app-with-port-mappings"),
        role = "*",
        cmd = Some("sleep 30"),
        portDefinitions = Nil,
        networks = Seq(
          ContainerNetwork(
            labels = Map(
              "foo" -> "bar",
              "baz" -> "buzz"
            ),
            name = "blahze"
          )),

        container = Some(Container.Docker(
          image = "jdef/foo",

          portMappings = Seq(
            Container.PortMapping(hostPort = None),
            Container.PortMapping(hostPort = Some(123)),
            Container.PortMapping(
              containerPort = 1, hostPort = Some(234), protocol = "udp", networkNames = List("blahze"))
          )
        ))
      )

      val proto = app.toProto
      proto.getId should be("/app-with-port-mappings")
      assert(proto.getNetworksCount > 0)

      val read = AppDefinition(id = runSpecId, role = "*").mergeFromProto(proto)
      read should be(app)
    }

    "ipAddress to proto and back again w/ Docker container w/ bridge" in {
      val app = AppDefinition(
        id = AbsolutePathId("/app-with-ip-address"),
        role = "*",
        cmd = Some("sleep 30"),
        portDefinitions = Nil,
        networks = Seq(BridgeNetwork()), container = Some(Container.Docker(
          image = "jdef/foo",

          portMappings = Seq(
            Container.PortMapping(hostPort = Some(0)),
            Container.PortMapping(hostPort = Some(123)),
            Container.PortMapping(containerPort = 1, hostPort = Some(234), protocol = "udp")
          )
        ))
      )

      val proto = app.toProto
      proto.getId should be("/app-with-ip-address")

      val read = AppDefinition(id = runSpecId, role = "*").mergeFromProto(proto)
      read should be(app)
    }

    "ipAddress discovery to proto and back again" in {
      val app = AppDefinition(
        id = AbsolutePathId("/app-with-ip-address"),
        role = "*",
        cmd = Some("sleep 30"),
        portDefinitions = Nil,
        networks = Seq(ContainerNetwork(
          name = "whatever",
          labels = Map(
            "foo" -> "bar",
            "baz" -> "buzz"
          )
        )),
        container = Some(Container.Mesos(
          portMappings = Seq(Container.PortMapping(name = Some("http"), containerPort = 80, protocol = "tcp"))
        ))
      )

      val proto: Protos.ServiceDefinition = app.toProto
      assert(proto.getNetworksCount > 0)
      assert(proto.hasContainer)

      val network = proto.getNetworks(0)
      assert(network.getLabelsCount > 0)

      val container = proto.getContainer
      assert(container.getPortMappingsCount > 0)
      val read = AppDefinition(id = runSpecId, role = "*").mergeFromProto(proto)
      read should equal(app)
    }

    "MergeFromProto" in {
      val cmd = mesos.CommandInfo.newBuilder
        .setValue("bash foo-*/start -Dhttp.port=$PORT")

      val proto1 = ServiceDefinition.newBuilder
        .setId("/play")
        .setCmd(cmd)
        .setInstances(3)
        .setExecutor("//cmd")
        .setVersion(Timestamp.now().toString)
        .build

      val app1 = AppDefinition(id = runSpecId, role = "*").mergeFromProto(proto1)

      assert("/play" == app1.id.toString)
      assert(3 == app1.instances)
      assert("//cmd" == app1.executor)
      assert(app1.cmd.contains("bash foo-*/start -Dhttp.port=$PORT"))
    }

    "Read obsolete ports from proto" in {
      val cmd = mesos.CommandInfo.newBuilder.setValue("bash foo-*/start -Dhttp.port=$PORT")

      val proto1 = ServiceDefinition.newBuilder
        .setId("/app")
        .setCmd(cmd)
        .setInstances(1)
        .setExecutor("//cmd")
        .setVersion(Timestamp.now().toString)
        .addPorts(1000)
        .addPorts(1001)
        .build

      val app = AppDefinition(id = runSpecId, role = "*").mergeFromProto(proto1)

      assert(PortDefinitions(1000, 1001) == app.portDefinitions)
    }

    "ProtoRoundtrip" in {
      val app1 = AppDefinition(
        id = AbsolutePathId("/play"),
        role = "*",
        cmd = Some("bash foo-*/start -Dhttp.port=$PORT"),
        resources = Resources(cpus = 4.0, mem = 256.0),
        instances = 5,
        portDefinitions = PortDefinitions(8080, 8081),
        executor = "//cmd",
        labels = Map(
          "one" -> "aaa",
          "two" -> "bbb",
          "three" -> "ccc"
        ),
        versionInfo = fullVersion,
        unreachableStrategy = UnreachableEnabled(inactiveAfter = 998.seconds, expungeAfter = 999.seconds),
        killSelection = KillSelection.OldestFirst
      )
      val result1 = AppDefinition(id = runSpecId, role = "*").mergeFromProto(app1.toProto)
      assert(result1 == app1)

      val app2 = AppDefinition(
        id = runSpecId,
        role = "*",
        cmd = None,
        args = Seq("a", "b", "c"),
        versionInfo = fullVersion
      )
      val result2 = AppDefinition(id = runSpecId, role = "*").mergeFromProto(app2.toProto)
      assert(result2 == app2)
    }

    "ProtoRoundtrip for secrets" in {
      val app = AppDefinition(
        id = runSpecId,
        role = "*",
        cmd = None,
        secrets = Map[String, Secret](
          "psst" -> Secret("/something/secret")
        ),
        env = Map[String, EnvVarValue](
          "foo" -> "bar".toEnvVar,
          "ssh" -> EnvVarSecretRef("psst")
        ),
        versionInfo = fullVersion
      )
      val result = AppDefinition(id = runSpecId, role = "*").mergeFromProto(app.toProto)
      assert(result == app, s"expected $app instead of $result")
    }

    "Proto round trip for tty" in {
      val app = AppDefinition(
        id = runSpecId,
        role = "*",
        cmd = Some("true"),
        tty = Some(true),
        versionInfo = fullVersion
      )
      val result = AppDefinition(id = runSpecId, role = "*").mergeFromProto(app.toProto)
      assert(result == app, s"expected $app instead of $result")
    }

    "Proto round trip for executor resources added" in {
      val app = AppDefinition(
        id = runSpecId,
        role = "*",
        cmd = Some("true"),
        versionInfo = fullVersion,
        executorResources = Some(Resources(
          cpus = 0.1, mem = 32.0, disk = 10
        ))
      )
      val result = AppDefinition(id = runSpecId, role = "*").mergeFromProto(app.toProto)
      assert(result == app, s"expected $app instead of $result")
    }

    "Proto round trip for executor resources left untouched" in {
      val app = AppDefinition(
        id = runSpecId,
        role = "*",
        executorResources = Some(Resources(
          cpus = 0.1, mem = 32.0, disk = 10
        ))
      )
      val result = app.mergeFromProto(AppDefinition(id = runSpecId, role = "*").toProto)
      assert(result.executorResources == app.executorResources, s"expected $app instead of $result")
    }

    "Proto round trip for executor resources CPU upgrade" in {
      val app = AppDefinition(
        id = runSpecId,
        role = "*",
        executorResources = Some(Resources(
          cpus = 0.1, mem = 32.0, disk = 10
        ))
      )
      val update = AppDefinition(
        id = runSpecId,
        role = "*",
        executorResources = Some(Resources(
          cpus = 0.2
        ))
      )
      val result = app.mergeFromProto(update.toProto)
      assert(result == update, s"expected $update instead of $result")
    }
  }

  def getScalarResourceValue(proto: ServiceDefinition, name: String) = {
    proto.getResourcesList
      .find(_.getName == name)
      .get.getScalar.getValue
  }
}
