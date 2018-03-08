/**
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.typed.internal

import java.util.concurrent.TimeUnit

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.event.Logging.LogLevel
import akka.persistence.Persistence
import com.typesafe.config.Config

import scala.concurrent.duration._

trait EventsourcedSettings {

  def stashCapacity: Int
  // def stashOverflowStrategyName: String // TODO not supported, the stash just throws for now
  def stashingLogLevel: LogLevel
  def journalPluginId: String
  def snapshotPluginId: String
  def recoveryEventTimeout: FiniteDuration

  def withJournalPluginId(id: String): EventsourcedSettings
  def withSnapshotPluginId(id: String): EventsourcedSettings
}

object EventsourcedSettings {

  def apply(system: ActorSystem[_]): EventsourcedSettings =
    apply(system.settings.config)

  def apply(config: Config): EventsourcedSettings = {
    val typedConfig = config.getConfig("akka.persistence.typed")
    val untypedConfig = config.getConfig("akka.persistence")

    // StashOverflowStrategy
    val internalStashOverflowStrategy =
      untypedConfig.getString("internal-stash-overflow-strategy") // FIXME or copy it to typed?

    val stashCapacity = typedConfig.getInt("stash-capacity")

    val stashingLogLevel = typedConfig.getString("log-stashing") match {
      case "off"         ⇒ Logging.OffLevel
      case "on" | "true" ⇒ Logging.DebugLevel
      case l             ⇒ Logging.levelFor(l).getOrElse(Logging.OffLevel)
    }

    EventsourcedSettingsImpl(
      config,
      stashCapacity = stashCapacity,
      internalStashOverflowStrategy,
      stashingLogLevel = stashingLogLevel,
      journalPluginId = "",
      snapshotPluginId = ""
    )
  }

  /**
   * INTERNAL API
   */
  private[akka] final def journalConfigFor(config: Config, journalPluginId: String): Config = {
    val defaultJournalPluginId = config.getString("akka.persistence.journal.plugin")
    val configPath = if (journalPluginId == "") defaultJournalPluginId else journalPluginId
    config.getConfig(configPath)
      .withFallback(config.getConfig(Persistence.JournalFallbackConfigPath))
  }

}

@InternalApi
private[persistence] final case class EventsourcedSettingsImpl(
  private val config:        Config,
  stashCapacity:             Int,
  stashOverflowStrategyName: String,
  stashingLogLevel:          LogLevel,
  journalPluginId:           String,
  snapshotPluginId:          String
) extends EventsourcedSettings {

  def withJournalPluginId(id: String): EventsourcedSettings =
    copy(journalPluginId = id)
  def withSnapshotPluginId(id: String): EventsourcedSettings =
    copy(snapshotPluginId = id)

  private val journalConfig = EventsourcedSettings.journalConfigFor(config, journalPluginId)
  val recoveryEventTimeout = journalConfig.getDuration("recovery-event-timeout", TimeUnit.MILLISECONDS).millis

}

