/**
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.persistence.typed.internal

import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.persistence.JournalProtocol.ReplayMessages
import akka.persistence.SnapshotProtocol.LoadSnapshot
import akka.persistence._

import scala.collection.immutable

@InternalApi
private[akka] trait EventsourcedJournalInteractions[C, E, S] {

  def setup: EventsourcedSetup[C, E, S]

  type EventOrTagged = Any // `Any` since can be `E` or `Tagged`

  // ---------- journal interactions ---------

  protected def returnRecoveryPermitOnlyOnFailure(cause: Throwable): Unit = {
    setup.log.debug("Returning recovery permit, on failure because: {}", cause.getMessage)
    // IMPORTANT to use selfUntyped, and not an adapter, since recovery permitter watches/unwatches those refs (and adapters are new refs)
    val permitter = setup.persistence.recoveryPermitter
    permitter.tell(RecoveryPermitter.ReturnRecoveryPermit, setup.selfUntyped)
  }

  protected def internalPersist(
    state: EventsourcedRunning.EventsourcedState[S],
    event: EventOrTagged): EventsourcedRunning.EventsourcedState[S] = {

    val newState = state.nextSequenceNr()

    val senderNotKnownBecauseAkkaTyped = null
    val repr = PersistentRepr(
      event,
      persistenceId = setup.persistenceId,
      sequenceNr = newState.seqNr,
      writerUuid = setup.writerIdentity.writerUuid,
      sender = senderNotKnownBecauseAkkaTyped
    )

    val eventBatch = AtomicWrite(repr) :: Nil // batching not used, since no persistAsync
    setup.journal.tell(JournalProtocol.WriteMessages(eventBatch, setup.selfUntyped, setup.writerIdentity.instanceId), setup.selfUntyped)

    newState
  }

  protected def internalPersistAll(
    events: immutable.Seq[EventOrTagged],
    state:  EventsourcedRunning.EventsourcedState[S]): EventsourcedRunning.EventsourcedState[S] = {
    if (events.nonEmpty) {
      var newState = state

      val writes = events.map { event ⇒
        newState = newState.nextSequenceNr()
        PersistentRepr(
          event,
          persistenceId = setup.persistenceId,
          sequenceNr = newState.seqNr,
          writerUuid = setup.writerIdentity.writerUuid,
          sender = ActorRef.noSender)
      }
      val write = AtomicWrite(writes)

      setup.journal.tell(JournalProtocol.WriteMessages(write :: Nil, setup.selfUntyped, setup.writerIdentity.instanceId), setup.selfUntyped)

      newState
    } else state
  }

  protected def replayEvents(fromSeqNr: Long, toSeqNr: Long): Unit = {
    setup.log.debug("Replaying messages: from: {}, to: {}", fromSeqNr, toSeqNr)
    // reply is sent to `selfUntypedAdapted`, it is important to target that one
    setup.journal ! ReplayMessages(fromSeqNr, toSeqNr, setup.recovery.replayMax, setup.persistenceId, setup.selfUntyped)
  }

  protected def returnRecoveryPermit(setup: EventsourcedSetup[_, _, _], reason: String): Unit = {
    setup.log.debug("Returning recovery permit, reason: " + reason)
    // IMPORTANT to use selfUntyped, and not an adapter, since recovery permitter watches/unwatches those refs (and adapters are new refs)
    setup.persistence.recoveryPermitter.tell(RecoveryPermitter.ReturnRecoveryPermit, setup.selfUntyped)
  }

  // ---------- snapshot store interactions ---------

  /**
   * Instructs the snapshot store to load the specified snapshot and send it via an [[SnapshotOffer]]
   * to the running [[PersistentActor]].
   */
  protected def loadSnapshot(criteria: SnapshotSelectionCriteria, toSequenceNr: Long): Unit = {
    setup.snapshotStore.tell(LoadSnapshot(setup.persistenceId, criteria, toSequenceNr), setup.selfUntyped)
  }

  protected def internalSaveSnapshot(state: EventsourcedRunning.EventsourcedState[S]): Unit = {
    setup.snapshotStore.tell(SnapshotProtocol.SaveSnapshot(SnapshotMetadata(setup.persistenceId, state.seqNr), state.state), setup.selfUntyped)
  }

}
