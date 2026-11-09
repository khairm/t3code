// fork:model-allowlist: the idle-gated restart's hold on run starts (gated in the Orchestrator).
// Drain: T3's own starts wait as queued runs marked heldForRestart; client sends, and starts
// caused by a still-active run, go through. Cutover: client sends wait too. Release, expiry or
// the restarted server starts what waited. A generation ties a cutover to its hold.
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

const QUEUE_PROMOTION_HOLD_EXPIRY = Duration.minutes(30);
const QUEUE_PROMOTION_HOLD_LOCK_WAIT = Duration.seconds(20);
export const QUEUE_PROMOTION_HELD_MESSAGE =
  "T3 is holding new runs for a server restart. Try again once it has restarted.";

export type QueuePromotionHoldPhase = "drain" | "cutover";

export type QueuePromotionHoldStatus =
  | { readonly held: false; readonly generation: number }
  | {
      readonly held: true;
      readonly phase: QueuePromotionHoldPhase;
      readonly generation: number;
      readonly heldSince: DateTime.Utc;
      readonly expiresAt: DateTime.Utc;
    };

/** The cutover named a generation that is not the current hold's: it lapsed, or was never set. */
export class QueuePromotionHoldStaleError extends Data.TaggedError("QueuePromotionHoldStaleError")<{
  readonly generation: number;
  readonly status: QueuePromotionHoldStatus;
}> {
  override get message(): string {
    return this.status.held
      ? `Queue promotion hold generation ${this.generation} is stale; the current hold is generation ${this.status.generation}.`
      : `Queue promotion is not held (generation ${this.generation} lapsed).`;
  }
}

/** The hold is set, but a start decision taken before it has not committed within the limit. */
export class QueuePromotionHoldLockWaitError extends Data.TaggedError(
  "QueuePromotionHoldLockWaitError",
)<{ readonly phase: QueuePromotionHoldPhase }> {
  override get message(): string {
    return `Queue promotion ${this.phase} is set, but a run start decided before it is still committing after ${Duration.format(QUEUE_PROMOTION_HOLD_LOCK_WAIT)}; retry.`;
  }
}

export interface QueuePromotionHoldShape {
  readonly status: Effect.Effect<QueuePromotionHoldStatus>;
  /** Null when not held. Read under the thread's dispatch lock, so `hold` can wait out a decision. */
  readonly phase: Effect.Effect<QueuePromotionHoldPhase | null>;
  /** Sets the drain hold, or refreshes its expiry; replies once every earlier start decision has committed. */
  readonly hold: Effect.Effect<QueuePromotionHoldStatus, QueuePromotionHoldLockWaitError>;
  /** Moves the hold of `generation` to cutover; refused when that hold lapsed. */
  readonly cutover: (
    generation: number,
  ) => Effect.Effect<
    QueuePromotionHoldStatus,
    QueuePromotionHoldStaleError | QueuePromotionHoldLockWaitError
  >;
  readonly release: Effect.Effect<void>;
  /** Emits on each release or expiry, so runs held for the restart can start. */
  readonly releases: Stream.Stream<void>;
}

export class QueuePromotionHold extends Context.Service<
  QueuePromotionHold,
  QueuePromotionHoldShape
>()("t3/orchestration-v2/QueuePromotionHold") {}

type Held = Extract<QueuePromotionHoldStatus, { held: true }>;

const make = Effect.gen(function* () {
  const threadLocks = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const scope = yield* Effect.scope;
  const stateRef = yield* SynchronizedRef.make<QueuePromotionHoldStatus>({
    held: false,
    generation: 0,
  });
  const releasesPubSub = yield* PubSub.unbounded<void>();

  /** Releases the current hold; with `expiring`, only the hold of that generation. */
  const clear = Effect.fnUntraced(function* (expiring?: number) {
    const cleared = yield* SynchronizedRef.modify(
      stateRef,
      (state): [Held | null, QueuePromotionHoldStatus] =>
        !state.held || (expiring !== undefined && state.generation !== expiring)
          ? [null, state]
          : [state, { held: false, generation: state.generation + 1 }],
    );
    if (cleared === null) return;
    const fields = {
      generation: cleared.generation,
      phase: cleared.phase,
      heldSince: DateTime.formatIso(cleared.heldSince),
    };
    yield* expiring === undefined
      ? Effect.logInfo("Queue promotion released", fields)
      : Effect.logWarning(
          "Queue promotion hold expired without a server restart; releasing it",
          fields,
        );
    // After the state change, so runs resumed on this event see the hold cleared.
    yield* PubSub.publish(releasesPubSub, undefined);
  });

  // Refreshes move the expiry, so the watcher re-reads it each time it wakes.
  const watchExpiry = Effect.fn("QueuePromotionHold.watchExpiry")(function* (generation: number) {
    while (true) {
      const state = yield* SynchronizedRef.get(stateRef);
      if (!state.held || state.generation !== generation) return;
      const now = yield* DateTime.now;
      if (DateTime.isGreaterThanOrEqualTo(now, state.expiresAt)) return yield* clear(generation);
      yield* Effect.sleep(DateTime.distance(now, state.expiresAt));
    }
  }, Effect.forkIn(scope));

  // Decisions run under their thread's lock, so one taken before the flag was set has committed
  // once that lock is free.
  const awaitEarlierDecisions = (phase: QueuePromotionHoldPhase) =>
    Effect.flatMap(threadLocks.activeKeys, (lockedThreads) =>
      Effect.forEach(lockedThreads, (threadId) => threadLocks.withLock(threadId, Effect.void), {
        concurrency: "unbounded",
        discard: true,
      }),
    ).pipe(
      Effect.timeoutOrElse({
        duration: QUEUE_PROMOTION_HOLD_LOCK_WAIT,
        orElse: () => Effect.fail(new QueuePromotionHoldLockWaitError({ phase })),
      }),
    );

  const hold = Effect.gen(function* () {
    const now = yield* DateTime.now;
    const expiresAt = DateTime.addDuration(now, QUEUE_PROMOTION_HOLD_EXPIRY);
    const [started, status] = yield* SynchronizedRef.modify(
      stateRef,
      (state): [[boolean, Held], Held] => {
        const next: Held = {
          held: true,
          phase: "drain",
          generation: state.generation,
          heldSince: state.held ? state.heldSince : now,
          expiresAt,
        };
        return [[!state.held, next], next];
      },
    );
    if (!started) return status;
    yield* watchExpiry(status.generation);
    yield* Effect.logInfo("Queue promotion held until the server restarts", {
      generation: status.generation,
    });
    yield* awaitEarlierDecisions("drain");
    return status;
  });

  const cutover = Effect.fn("QueuePromotionHold.cutover")(function* (generation: number) {
    const now = yield* DateTime.now;
    const expiresAt = DateTime.addDuration(now, QUEUE_PROMOTION_HOLD_EXPIRY);
    const [matched, status] = yield* SynchronizedRef.modify(
      stateRef,
      (state): [[boolean, QueuePromotionHoldStatus], QueuePromotionHoldStatus] => {
        if (!state.held || state.generation !== generation) return [[false, state], state];
        const next: Held = { ...state, phase: "cutover", expiresAt };
        return [[true, next], next];
      },
    );
    if (!matched) return yield* new QueuePromotionHoldStaleError({ generation, status });
    yield* awaitEarlierDecisions("cutover");
    return status;
  });

  return QueuePromotionHold.of({
    status: SynchronizedRef.get(stateRef),
    phase: Effect.map(SynchronizedRef.get(stateRef), (state) => (state.held ? state.phase : null)),
    hold,
    cutover,
    release: clear(),
    releases: Stream.fromPubSub(releasesPubSub),
  });
});

export const layer = Layer.effect(QueuePromotionHold, make);
