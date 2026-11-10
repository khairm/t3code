// fork:model-allowlist: the idle-gated restart's hold on run starts (gated in the Orchestrator).
// Drain: T3's own starts wait as queued runs marked heldForRestart; owner sends and clicks,
// provider wakes and starts caused by a still-active run go through. Cutover: every start waits.
// Release, expiry or the restarted server starts what waited; a generation ties cutover to hold.
import type { OrchestrationV2Run, ThreadId } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import {
  backgroundWorkHoldsCompletion,
  derivePendingBackgroundWork,
} from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { latestExecutedRun } from "@t3tools/shared/orchestrationV2ThreadError";

import { isActiveRun } from "./CommandPolicy.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import { isBufferedProviderNotification } from "./NotificationMailbox.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { isCutBackgroundItem } from "./ProviderRuntimeRecoveryService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

const DRAIN_EXPIRY = Duration.minutes(30);
const CUTOVER_EXPIRY = Duration.minutes(2);
const QUEUE_PROMOTION_HOLD_LOCK_WAIT = Duration.seconds(20);
export const QUEUE_PROMOTION_HELD_MESSAGE =
  "T3 is holding new runs for a server restart. Try again once it has restarted.";

const QueuePromotionHoldPhase = Schema.Literals(["drain", "cutover"]);
export type QueuePromotionHoldPhase = typeof QueuePromotionHoldPhase.Type;

const QueuePromotionHoldStatus = Schema.Union([
  Schema.Struct({ held: Schema.Literal(false), generation: Schema.Number }),
  Schema.Struct({
    held: Schema.Literal(true),
    phase: QueuePromotionHoldPhase,
    generation: Schema.Number,
    heldSince: Schema.DateTimeUtc,
    expiresAt: Schema.DateTimeUtc,
  }),
]);
export type QueuePromotionHoldStatus = typeof QueuePromotionHoldStatus.Type;

/** The cutover named a hold that is not the current one: not held, another generation, back in drain, or expired. */
export class QueuePromotionHoldStaleError extends Schema.TaggedError<QueuePromotionHoldStaleError>()(
  "QueuePromotionHoldStaleError",
  { generation: Schema.Number, status: QueuePromotionHoldStatus },
) {
  override get message(): string {
    const reason = !this.status.held
      ? "it is not held"
      : this.status.generation !== this.generation
        ? `the current hold is generation ${this.status.generation}`
        : this.status.phase === "drain"
          ? "the hold is back in drain"
          : "the hold expired";
    return `Queue promotion cutover for generation ${this.generation} refused: ${reason}.`;
  }
}

/** The hold is in `phase`, but a start decision taken before it has not committed within the limit. */
export class QueuePromotionHoldLockWaitError extends Schema.TaggedError<QueuePromotionHoldLockWaitError>()(
  "QueuePromotionHoldLockWaitError",
  { phase: QueuePromotionHoldPhase },
) {
  override get message(): string {
    return `Queue promotion ${this.phase} is set, but a run start decided before it is still committing after ${Duration.format(QUEUE_PROMOTION_HOLD_LOCK_WAIT)}; retry.`;
  }
}

export interface QueuePromotionHoldShape {
  readonly status: Effect.Effect<QueuePromotionHoldStatus>;
  /** Null when not held. Read under the thread's dispatch lock, so the hold can wait out a decision. */
  readonly phase: Effect.Effect<QueuePromotionHoldPhase | null>;
  /**
   * How many threads a T3 stop would cut: one with a live run, a provider wake waiting to start,
   * awaited background work or a background step still running, or an accepted response the
   * provider has not received.
   */
  readonly busy: Effect.Effect<number>;
  /** Sets a drain hold, or refreshes the current hold's expiry in its phase; replies once every earlier start decision has committed. */
  readonly hold: Effect.Effect<QueuePromotionHoldStatus, QueuePromotionHoldLockWaitError>;
  /** Like `hold`, and brings a cutover back to drain, starting what drain allows. */
  readonly drain: Effect.Effect<QueuePromotionHoldStatus, QueuePromotionHoldLockWaitError>;
  /**
   * Moves the hold of `generation` to cutover. Once earlier decisions have committed, every queued
   * run the owner does not hold is marked `heldForRestart`, then the hold is checked again. If they
   * have not committed within the wait, the hold goes back to drain and the cutover fails.
   */
  readonly cutover: (
    generation: number,
  ) => Effect.Effect<
    QueuePromotionHoldStatus,
    QueuePromotionHoldStaleError | QueuePromotionHoldLockWaitError
  >;
  readonly release: Effect.Effect<void>;
  /** Emits once runs the hold kept may start: after a release, an expiry, or a cutover brought back to drain. */
  readonly releases: Stream.Stream<void>;
  /** Marks the queued runs of `runs` the owner does not hold as kept by the hold. The caller holds the thread's lock. */
  readonly markQueuedRuns: (
    threadId: ThreadId,
    runs: ReadonlyArray<OrchestrationV2Run>,
  ) => Effect.Effect<void, EventSink.EventSinkV2Error | IdAllocator.IdAllocatorV2Error>;
  /** Clears the mark from the queued runs of `runs`, once no hold is in effect. The caller holds the thread's lock. */
  readonly unmarkQueuedRuns: (
    threadId: ThreadId,
    runs: ReadonlyArray<OrchestrationV2Run>,
  ) => Effect.Effect<void, EventSink.EventSinkV2Error | IdAllocator.IdAllocatorV2Error>;
}

export class QueuePromotionHold extends Context.Service<
  QueuePromotionHold,
  QueuePromotionHoldShape
>()("t3/orchestration-v2/QueuePromotionHold") {}

type Held = Extract<QueuePromotionHoldStatus, { held: true }>;
/**
 * The revision rises on every transition, so an expiry watcher clears only the state it read; the
 * watcher is installed in the same transition, so a stale one never outlives a newer state.
 */
interface State {
  readonly revision: number;
  readonly status: QueuePromotionHoldStatus;
  readonly watcher: Fiber.Fiber<void> | null;
}

const expired = (status: Held, now: DateTime.Utc) =>
  DateTime.isGreaterThanOrEqualTo(now, status.expiresAt);

const BACKGROUND_TURN_ITEM_TYPES = ["command_execution", "dynamic_tool", "subagent"] as const;
const ACTIVE_TURN_ITEM_STATUSES = ["pending", "running", "waiting"] as const;

const make = Effect.gen(function* () {
  const threadLocks = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const scope = yield* Effect.scope;
  // Seeded per process, so a cutover named before a restart is refused after it.
  const stateRef = yield* SynchronizedRef.make<State>({
    revision: 0,
    status: { held: false, generation: yield* Clock.currentTimeMillis },
    watcher: null,
  });
  const releasesPubSub = yield* PubSub.unbounded<void>();
  const status = Effect.map(SynchronizedRef.get(stateRef), (state) => state.status);

  /**
   * One atomic transition; `decide` returns the status to move to, or null to leave the state
   * alone. A held status gets its expiry watcher here, replacing the previous one. The watcher
   * only sleeps, and forks the expiry, so interrupting it never cuts an expiry short.
   */
  const transition = (
    decide: (state: State, now: DateTime.Utc) => QueuePromotionHoldStatus | null,
  ): Effect.Effect<{ readonly before: State; readonly after: State }> =>
    Effect.flatMap(DateTime.now, (now) =>
      SynchronizedRef.modifyEffect(stateRef, (before) =>
        Effect.gen(function* () {
          const next = decide(before, now);
          if (next === null) return [{ before, after: before }, before] as const;
          const revision = before.revision + 1;
          const watcher = next.held
            ? yield* Effect.sleep(DateTime.distance(now, next.expiresAt)).pipe(
                Effect.andThen(Effect.forkIn(clear(revision), scope)),
                Effect.asVoid,
                Effect.forkIn(scope),
              )
            : null;
          if (before.watcher !== null) yield* Fiber.interrupt(before.watcher);
          const after: State = { revision, status: next, watcher };
          return [{ before, after }, after] as const;
        }),
      ),
    );

  /** Releases the current hold; with `revision`, only the state the expiry watcher read. */
  const clear = Effect.fnUntraced(function* (revision?: number) {
    const { before, after } = yield* transition(({ status, revision: current }) =>
      status.held && (revision === undefined || current === revision)
        ? { held: false, generation: status.generation + 1 }
        : null,
    );
    if (after === before || !before.status.held) return;
    const fields = {
      generation: before.status.generation,
      phase: before.status.phase,
      heldSince: DateTime.formatIso(before.status.heldSince),
    };
    yield* revision === undefined
      ? Effect.logInfo("Queue promotion released", fields)
      : Effect.logWarning(
          "Queue promotion hold expired without a server restart; releasing it",
          fields,
        );
    yield* publishReleases;
  });

  // Decisions run under their thread's lock, so one taken before a transition has committed once
  // that lock is free.
  const earlierDecisions = Effect.flatMap(threadLocks.activeKeys, (lockedThreads) =>
    Effect.forEach(lockedThreads, (threadId) => threadLocks.withLock(threadId, Effect.void), {
      concurrency: "unbounded",
      discard: true,
    }),
  );
  const withinLockWait = <A, E1, E2>(
    effect: Effect.Effect<A, E1>,
    orElse: () => Effect.Effect<A, E2>,
  ) => effect.pipe(Effect.timeoutOrElse({ duration: QUEUE_PROMOTION_HOLD_LOCK_WAIT, orElse }));
  const awaitEarlierDecisions = <E>(orElse: () => Effect.Effect<void, E>) =>
    withinLockWait(earlierDecisions, orElse);
  const publish = PubSub.publish(releasesPubSub, undefined);
  /**
   * Published once decisions taken while held have committed, so the sweep finds their runs. A
   * decision still committing after the wait gets its own publish once it has.
   */
  const publishReleases = awaitEarlierDecisions(() =>
    Effect.logWarning(
      "A run start decided under the queue promotion hold is still committing",
    ).pipe(Effect.andThen(publish), Effect.andThen(earlierDecisions)),
  ).pipe(Effect.andThen(publish), Effect.forkIn(scope));

  const held = (
    status: QueuePromotionHoldStatus,
    phase: QueuePromotionHoldPhase,
    now: DateTime.Utc,
  ): Held => ({
    held: true,
    phase,
    generation: status.generation,
    heldSince: status.held ? status.heldSince : now,
    expiresAt: DateTime.addDuration(now, phase === "cutover" ? CUTOVER_EXPIRY : DRAIN_EXPIRY),
  });

  /** `current` keeps the phase of a held hold; `drain` also brings a cutover back to drain. */
  const holdIn = Effect.fn("QueuePromotionHold.hold")(function* (requested: "current" | "drain") {
    const { before, after } = yield* transition(({ status }, now) =>
      held(status, !status.held || requested === "drain" ? "drain" : status.phase, now),
    );
    const next = after.status as Held;
    if (!before.status.held) {
      yield* Effect.logInfo("Queue promotion held until the server restarts", {
        generation: next.generation,
      });
    } else if (before.status.phase !== next.phase) {
      yield* publishReleases;
    }
    yield* awaitEarlierDecisions(() =>
      Effect.fail(new QueuePromotionHoldLockWaitError({ phase: next.phase })),
    );
    return next;
  });

  const writeRunUpdates = Effect.fnUntraced(function* (
    threadId: ThreadId,
    runs: ReadonlyArray<OrchestrationV2Run>,
    update: (run: OrchestrationV2Run) => OrchestrationV2Run,
  ) {
    if (runs.length === 0) return;
    const now = yield* DateTime.now;
    const events = yield* Effect.forEach(runs, (run) =>
      Effect.map(ids.allocate.event({ threadId }), (id) => ({
        id,
        type: "run.updated" as const,
        threadId,
        runId: run.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: update(run),
      })),
    );
    yield* eventSink.write({ events });
  });
  const markQueuedRuns: QueuePromotionHoldShape["markQueuedRuns"] = (threadId, runs) =>
    writeRunUpdates(
      threadId,
      runs.filter(
        (run) => run.status === "queued" && run.queueHeld !== true && run.heldForRestart !== true,
      ),
      (run) => ({ ...run, heldForRestart: true }),
    );
  const unmarkQueuedRuns: QueuePromotionHoldShape["unmarkQueuedRuns"] = (threadId, runs) =>
    writeRunUpdates(
      threadId,
      runs.filter((run) => run.status === "queued" && run.heldForRestart === true),
      ({ heldForRestart: _heldForRestart, ...run }) => run,
    );
  // The runtime set is every thread with a queued run the owner does not hold, idle or not.
  const markEveryQueuedRun = Effect.flatMap(
    projections.getRecoveryThreadIds("runtime"),
    Effect.forEach(
      (threadId) =>
        threadLocks.withLock(
          threadId,
          Effect.flatMap(projections.getThreadRecords(threadId, ["runs"]), ({ runs }) =>
            markQueuedRuns(threadId, runs),
          ),
        ),
      { discard: true },
    ),
  ).pipe(Effect.orDie);

  const cutover = Effect.fn("QueuePromotionHold.cutover")(function* (generation: number) {
    const accepts = (status: QueuePromotionHoldStatus, now: DateTime.Utc): status is Held =>
      status.held && status.generation === generation && !expired(status, now);
    const inCutover = (status: QueuePromotionHoldStatus, now: DateTime.Utc) =>
      accepts(status, now) && status.phase === "cutover";
    const { before, after } = yield* transition(({ status }, now) =>
      accepts(status, now) ? held(status, "cutover", now) : null,
    );
    if (after === before) {
      return yield* new QueuePromotionHoldStaleError({ generation, status: after.status });
    }
    const backToDrain = Effect.gen(function* () {
      const { before, after } = yield* transition(({ status }, now) =>
        inCutover(status, now) ? held(status, "drain", now) : null,
      );
      if (after === before) {
        return yield* new QueuePromotionHoldStaleError({ generation, status: after.status });
      }
      yield* publishReleases;
      return yield* new QueuePromotionHoldLockWaitError({ phase: "drain" });
    });
    yield* withinLockWait(Effect.andThen(earlierDecisions, markEveryQueuedRun), () => backToDrain);
    // Checked again: a release, an expiry or a drain while the barrier waited refuses the cutover.
    const now = yield* DateTime.now;
    const current = yield* status;
    if (!inCutover(current, now)) {
      return yield* new QueuePromotionHoldStaleError({ generation, status: current });
    }
    return current;
  });

  const busyIn = Effect.fnUntraced(function* (threadId: ThreadId) {
    const { runs } = yield* projections.getThreadRecords(threadId, ["runs"]);
    // A waiting run's agent turn is over; what it still waits on is read below, as after any run.
    if (runs.some((run) => isActiveRun(run) && run.status !== "waiting")) return true;
    const { thread, providerThreads, turnItems, messages } = yield* projections.getThreadRecords(
      threadId,
      ["providerThreads", "turnItems", "messages"],
      {
        turnItemTypes: BACKGROUND_TURN_ITEM_TYPES,
        turnItemStatuses: ACTIVE_TURN_ITEM_STATUSES,
        messageIds: runs
          .filter((run) => run.status === "queued" && run.queueHeld !== true)
          .map((run) => run.userMessageId),
      },
    );
    return (
      messages.some(isBufferedProviderNotification) ||
      turnItems.some(isCutBackgroundItem) ||
      backgroundWorkHoldsCompletion(
        derivePendingBackgroundWork({
          latestRun: latestExecutedRun(runs),
          providerThreads,
          turnItems,
          activeProviderThreadId: thread.activeProviderThreadId,
          runs,
        }),
      )
    );
  });
  const busy = Effect.gen(function* () {
    const delivering = new Set(yield* outbox.threadIdsDelivering("runtime-request.respond"));
    const others = yield* Effect.forEach(
      (yield* projections.getRecoveryThreadIds("runtime")).filter(
        (threadId) => !delivering.has(threadId),
      ),
      busyIn,
    );
    return delivering.size + others.filter(Boolean).length;
  }).pipe(Effect.orDie);

  return QueuePromotionHold.of({
    status,
    phase: Effect.map(status, (current) => (current.held ? current.phase : null)),
    busy,
    hold: holdIn("current"),
    drain: holdIn("drain"),
    cutover,
    release: clear(),
    releases: Stream.fromPubSub(releasesPubSub),
    markQueuedRuns,
    unmarkQueuedRuns,
  });
});

export const layer = Layer.effect(QueuePromotionHold, make);
