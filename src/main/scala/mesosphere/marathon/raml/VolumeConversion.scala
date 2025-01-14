package mesosphere.marathon
package raml

import mesosphere.marathon.state.{DiskType, Volume}
import mesosphere.marathon.stream.Implicits._
import mesosphere.mesos.protos.Implicits._
import org.apache.mesos.{Protos => Mesos}

trait VolumeConversion extends ConstraintConversion with DefaultConversions {

  /**
    * Will select the default disk type to use depending on whether or not a disk profileName is given.
    *
    * There are three types of disk currently supported by Marathon: Root, Path and Mount. These are the disk types
    * introduced by Mesos and documented as Multiple Disks:
    * http://mesos.apache.org/documentation/latest/multiple-disk/
    *
    * A Root disk usually maps to the storage on the main operating system drive. Root is the default type used when
    * none is provided by the user. Other volume types will only exist if an operator created those using the present
    * operating system drive. That means, when external services or tools carve up raw disks, they will produce disks
    * of types Mount or Path, which can be used by frameworks. The only such service we are currently aware of and
    * integrate with is the DC/OS Storage Service (DSS) which is in beta:
    * https://docs.mesosphere.com/services/beta-storage/
    *
    * There are additional two types of disk that Marathon currently does not support directly, since they are of no
    * direct value to the user: Raw and Block. For more information on these, see
    * http://mesos.apache.org/documentation/latest/csi/
    *
    * DSS will use Mesos Raw disks and create Mount or Block devices out of them. The mechanism for frameworks to select
    * such a Mount volume is the profileName, which will be populated by DSS. Therefore, if a disk profileName is set,
    * the disk type default to Mount.
    */
  def defaultDiskTypeForProfile(profileName: Option[String]): DiskType = profileName.map(_ => DiskType.Mount).getOrElse(DiskType.Root)

  implicit val volumeRamlReader: Reads[PodVolume, state.Volume] = Reads {
    case ev: PodEphemeralVolume => state.EphemeralVolume(name = Some(ev.name))
    case hv: PodHostVolume => state.HostVolume(name = Some(hv.name), hostPath = hv.host)
    case sv: PodSecretVolume => state.SecretVolume(name = Some(sv.name), secret = sv.secret)
    case pv: PodPersistentVolume =>
      val diskType = pv.persistent.`type`.fromRaml.getOrElse(defaultDiskTypeForProfile(pv.persistent.profileName))
      val persistentInfo = state.PersistentVolumeInfo(
        `type` = diskType,
        size = pv.persistent.size,
        maxSize = pv.persistent.maxSize,
        profileName = pv.persistent.profileName,
        constraints = pv.persistent.constraints.fromRaml
      )
      state.PersistentVolume(name = Some(pv.name), persistent = persistentInfo)
  }

  implicit val volumeRamlWriter: Writes[state.Volume, PodVolume] = Writes {
    case e: state.EphemeralVolume => raml.PodEphemeralVolume(
      name = e.name.getOrElse(throw new IllegalArgumentException("name must not be empty")))
    case h: state.HostVolume => raml.PodHostVolume(
      name = h.name.getOrElse(throw new IllegalArgumentException("name must not be empty")), host = h.hostPath)
    case s: state.SecretVolume => raml.PodSecretVolume(
      name = s.name.getOrElse(throw new IllegalArgumentException("name must not be empty")), secret = s.secret)
    case p: state.PersistentVolume => raml.PodPersistentVolume(
      name = p.name.getOrElse(throw new IllegalArgumentException("name must not be empty")),
      persistent = p.persistent.toRaml)
  }

  implicit val volumeModeWrites: Writes[Mesos.Volume.Mode, ReadMode] = Writes {
    case Mesos.Volume.Mode.RO => ReadMode.Ro
    case Mesos.Volume.Mode.RW => ReadMode.Rw
  }

  implicit val volumeModeReads: Reads[ReadMode, Mesos.Volume.Mode] = Reads {
    case ReadMode.Ro => Mesos.Volume.Mode.RO
    case ReadMode.Rw => Mesos.Volume.Mode.RW
  }

  implicit val readOnlyFlagReads: Reads[ReadMode, Boolean] = Reads {
    case ReadMode.Ro => true
    case ReadMode.Rw => false
  }

  implicit val readOnlyFlagWrites: Writes[Boolean, ReadMode] = Writes { readOnly =>
    if (readOnly) ReadMode.Ro else ReadMode.Rw
  }

  implicit val persistentVolumeInfoWrites: Writes[state.PersistentVolumeInfo, PersistentVolumeInfo] = Writes { pv =>
    val pvType = Option(pv.`type` match {
      case DiskType.Mount => PersistentVolumeType.Mount
      case DiskType.Path => PersistentVolumeType.Path
      case DiskType.Root => PersistentVolumeType.Root
    })
    PersistentVolumeInfo(`type` = pvType, size = pv.size, maxSize = pv.maxSize, profileName = pv.profileName,
      constraints = pv.constraints.toRaml[Set[Seq[String]]])
  }

  implicit val volumeWrites: Writes[state.VolumeWithMount[Volume], AppVolume] = Writes { volumeWithMount =>

    implicit val externalVolumeWrites: Writes[state.ExternalVolumeInfo, ExternalVolumeInfo] = Writes { ev =>
      ExternalVolumeInfo(size = ev.size, name = Some(ev.name), provider = Some(ev.provider), options = ev.options, shared = ev.shared)
    }

    val volume = volumeWithMount.volume
    val mount = volumeWithMount.mount
    volume match {
      case dv: state.HostVolume => AppHostVolume(
        containerPath = mount.mountPath,
        hostPath = dv.hostPath,
        mode = mount.readOnly.toRaml)
      case ev: state.ExternalVolume => AppExternalVolume(
        containerPath = mount.mountPath,
        external = ev.external.toRaml,
        mode = mount.readOnly.toRaml)
      case pv: state.PersistentVolume => AppPersistentVolume(
        containerPath = mount.mountPath,
        persistent = pv.persistent.toRaml,
        mode = mount.readOnly.toRaml)
      case sv: state.SecretVolume => AppSecretVolume(
        containerPath = mount.mountPath,
        secret = sv.secret
      )
    }
  }

  implicit val volumeReads: Reads[AppVolume, state.VolumeWithMount[Volume]] = Reads {
    case v: AppExternalVolume => volumeExternalReads.read(v)
    case v: AppPersistentVolume => volumePersistentReads.read(v)
    case v: AppHostVolume => volumeHostReads.read(v)
    case v: AppSecretVolume => volumeSecretReads.read(v)
    case unsupported => throw SerializationFailedException(s"unsupported app volume type $unsupported")
  }

  implicit val volumeExternalReads: Reads[AppExternalVolume, state.VolumeWithMount[Volume]] = Reads { volumeRaml =>
    val info = state.ExternalVolumeInfo(
      size = volumeRaml.external.size,
      name = volumeRaml.external.name.getOrElse(
        throw SerializationFailedException("external volume requires a name")),
      provider = volumeRaml.external.provider.getOrElse(
        throw SerializationFailedException("external volume requires a provider")),
      options = volumeRaml.external.options,
      shared = volumeRaml.external.shared
    )
    val volume = state.ExternalVolume(name = None, external = info)
    val mount = state.VolumeMount(
      volumeName = None, mountPath = volumeRaml.containerPath, readOnly = volumeRaml.mode.fromRaml)
    state.VolumeWithMount[Volume](volume = volume, mount = mount)
  }

  implicit val volumeTypeReads: Reads[Option[PersistentVolumeType], Option[DiskType]] = Reads { maybeType =>
    maybeType.flatMap {
      case PersistentVolumeType.Root => Some(DiskType.Root)
      case PersistentVolumeType.Mount => Some(DiskType.Mount)
      case PersistentVolumeType.Path => Some(DiskType.Path)
    }
  }

  implicit val volumeConstraintsReads: Reads[Set[Seq[String]], Set[Protos.Constraint]] = Reads { constraints =>
    constraints.map { constraint =>
      (constraint.headOption, constraint.lift(1), constraint.lift(2)) match {
        case (Some("path"), Some("LIKE"), Some(value)) =>
          Protos.Constraint.newBuilder()
            .setField("path")
            .setOperator(Protos.Constraint.Operator.LIKE)
            .setValue(value)
            .build()
        case _ =>
          throw SerializationFailedException(s"illegal volume constraint ${constraint.mkString(",")}")
      }
    }(collection.breakOut)
  }

  implicit val volumePersistentReads: Reads[AppPersistentVolume, state.VolumeWithMount[Volume]] = Reads { volumeRaml =>
    val diskType = volumeRaml.persistent.`type`.fromRaml.getOrElse(defaultDiskTypeForProfile(volumeRaml.persistent.profileName))
    val info = state.PersistentVolumeInfo(
      `type` = diskType,
      size = volumeRaml.persistent.size,
      maxSize = volumeRaml.persistent.maxSize,
      profileName = volumeRaml.persistent.profileName,
      constraints = volumeRaml.persistent.constraints.fromRaml
    )
    val volume = state.PersistentVolume(name = None, persistent = info)
    val mount = state.VolumeMount(
      volumeName = None, mountPath = volumeRaml.containerPath, readOnly = volumeRaml.mode.fromRaml)
    state.VolumeWithMount[Volume](volume = volume, mount = mount)
  }

  implicit val volumeHostReads: Reads[AppHostVolume, state.VolumeWithMount[Volume]] = Reads { volumeRaml =>
    val volume = state.HostVolume(name = None, hostPath = volumeRaml.hostPath)
    val mount = state.VolumeMount(
      volumeName = None, mountPath = volumeRaml.containerPath, readOnly = volumeRaml.mode.fromRaml)
    state.VolumeWithMount[Volume](volume = volume, mount = mount)
  }

  implicit val volumeSecretReads: Reads[AppSecretVolume, state.VolumeWithMount[Volume]] = Reads { volumeRaml =>
    val volume = state.SecretVolume(name = None, secret = volumeRaml.secret)
    val mount = state.VolumeMount(volumeName = None, mountPath = volumeRaml.containerPath, readOnly = true)
    state.VolumeWithMount[Volume](volume = volume, mount = mount)
  }

  implicit val appVolumeExternalProtoRamlWriter: Writes[Protos.Volume.ExternalVolumeInfo, ExternalVolumeInfo] =
    Writes { volume =>
      ExternalVolumeInfo(
        size = volume.when(_.hasSize, _.getSize).orElse(ExternalVolumeInfo.DefaultSize),
        name = volume.when(_.hasName, _.getName).orElse(ExternalVolumeInfo.DefaultName),
        provider = volume.when(_.hasProvider, _.getProvider).orElse(ExternalVolumeInfo.DefaultProvider),
        options = volume.whenOrElse(
          _.getOptionsCount > 0,
          _.getOptionsList.map { x => x.getKey -> x.getValue }(collection.breakOut),
          ExternalVolumeInfo.DefaultOptions),
        shared = volume.when(_.hasShared, _.getShared).getOrElse(ExternalVolumeInfo.DefaultShared)
      )
    }

  implicit val appPersistentVolTypeProtoRamlWriter: Writes[Mesos.Resource.DiskInfo.Source.Type, PersistentVolumeType] = Writes { typ =>
    import Mesos.Resource.DiskInfo.Source.Type._
    typ match {
      case MOUNT => PersistentVolumeType.Mount
      case PATH => PersistentVolumeType.Path
      case badType => throw new IllegalStateException(s"unsupported Mesos resource disk-info source type $badType")
    }
  }

  implicit val appVolumePersistentProtoRamlWriter: Writes[Protos.Volume.PersistentVolumeInfo, PersistentVolumeInfo] =
    Writes { volume =>
      PersistentVolumeInfo(
        `type` = volume.when(_.hasType, _.getType.toRaml).orElse(PersistentVolumeInfo.DefaultType),
        size = volume.getSize,
        // TODO(jdef) protobuf serialization is broken for this
        maxSize = volume.when(_.hasMaxSize, _.getMaxSize).orElse(PersistentVolumeInfo.DefaultMaxSize),
        profileName = volume.when(_.hasProfileName, _.getProfileName).orElse(PersistentVolumeInfo.DefaultProfileName),
        constraints = volume.whenOrElse(
          _.getConstraintsCount > 0,
          _.getConstraintsList.map(_.toRaml[Seq[String]])(collection.breakOut),
          PersistentVolumeInfo.DefaultConstraints)
      )
    }

  implicit val appVolumeProtoRamlWriter: Writes[Protos.Volume, AppVolume] = Writes {
    case vol if vol.hasExternal => AppExternalVolume(
      containerPath = vol.getContainerPath,
      external = vol.getExternal.toRaml,
      mode = vol.getMode.toRaml
    )
    case vol if vol.hasPersistent => AppPersistentVolume(
      containerPath = vol.getContainerPath,
      persistent = vol.getPersistent.toRaml,
      mode = vol.getMode.toRaml
    )
    case vol if vol.hasSecret => AppSecretVolume(
      containerPath = vol.getContainerPath,
      secret = vol.getSecret.getSecret
    )
    case vol => AppHostVolume(
      containerPath = vol.getContainerPath,
      hostPath = vol.getHostPath,
      mode = vol.getMode.toRaml
    )
  }
}

object VolumeConversion extends VolumeConversion
