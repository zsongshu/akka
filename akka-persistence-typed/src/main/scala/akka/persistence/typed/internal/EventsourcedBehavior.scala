/**
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com/>
 */

package akka.persistence.typed.internal

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.NoSerializationVerificationNeeded
import akka.actor.typed.Behavior
import akka.actor.typed.Behavior.StoppedBehavior
import akka.actor.typed.scaladsl.{ ActorContext, TimerScheduler }
import akka.annotation.InternalApi
import akka.event.{ LogSource, Logging }
import akka.persistence.typed.scaladsl.PersistentBehaviors
import akka.persistence.{ JournalProtocol, Persistence, RecoveryPermitter, SnapshotProtocol }
import akka.{ actor ⇒ a }

/** INTERNAL API */
@InternalApi
private[akka] object EventsourcedBehavior {

  // ok to wrap around (2*Int.MaxValue restarts will not happen within a journal roundtrip)
  private[akka] val instanceIdCounter = new AtomicInteger(1)

  object WriterIdentity {
    def newIdentity(): WriterIdentity = {
      val instanceId: Int = EventsourcedBehavior.instanceIdCounter.getAndIncrement()
      val writerUuid: String = UUID.randomUUID.toString
      WriterIdentity(instanceId, writerUuid)
    }
  }
  final case class WriterIdentity(instanceId: Int, writerUuid: String)

  /** Protocol used internally by the eventsourced behaviors, never exposed to user-land */
  sealed trait InternalProtocol
  object InternalProtocol {
    case object RecoveryPermitGranted extends InternalProtocol
    final case class JournalResponse(msg: akka.persistence.JournalProtocol.Response) extends InternalProtocol
    final case class SnapshotterResponse(msg: akka.persistence.SnapshotProtocol.Response) extends InternalProtocol
    final case class RecoveryTickEvent(snapshot: Boolean) extends InternalProtocol
    final case class ReceiveTimeout(timeout: akka.actor.ReceiveTimeout) extends InternalProtocol
    final case class IncomingCommand[C](c: C) extends InternalProtocol
  }
}
