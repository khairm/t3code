// fork:model-allowlist: the idle-gated restart's hold on run starts (gated in the Orchestrator).
// Drain: T3's own starts wait as queued runs marked heldForRestart; the owner's sends and clicks,
// and starts caused by a still-active run, go through. Cutover: every start waits. Release, expiry
// or the restarted server starts what waited. A generation ties a cutover to its hold.
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

const DRAIN_EXPIRY = Duration.minutes(30);
const CUTOVER_EXPIRY = Duration.minutes(2);
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

/** The cutover named a hold that is not the current one: not held, another generation, back in drain, or expired. */
export class QueuePromotionHoldStaleError extends Data.TaggedError("QueuePromotionHoldStaleError")<{
  readonly generation: number;
  readonly status: QueuePromotionHoldStatus;
}> {
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
  /** Null when not held. Read under the thread's dispatch lock, so the hold can wait out a decision. */
  readonly phase: Effect.Effect<QueuePromotionHoldPhase | null>;
  /** Sets a drain hold, or refreshes the current hold's expiry in its phase; replies once every earlier start decision has committed. */
  readonly hold: Effect.Effect<QueuePromotionHoldStatus, QueuePromotionHoldLockWaitError>;
  /** Like `hold`, and brings a cutover back to drain, starting what drain allows. */
  readonly drain: Effect.Effect<QueuePromotionHoldStatus, QueuePromotionHoldLockWaitError>;
  /** Moves the hold of `generation` to cutover, checked again once earlier decisions have committed. */
  readonly cutover: (
    generation: number,
  ) => Effect.Effect<
    QueuePromotionHoldStatus,
    QueuePromotionHoldStaleError | QueuePromotionHoldLockWaitError
  >;
  readonly release: Effect.Effect<void>;
  /** Emits once runs the hold kept may start: after a release, an expiry, or a cutover brought back to drain. */
  readonly releases: Stream.Stream<void>;
}

export class QueuePromotionHold extends Context.Service<
  QueuePromotionHold,
  QueuePromotionHoldShape
>()("t3/orchestration-v2/QueuePromotionHold") {}

type Held = Extract<QueuePromotionHoldStatus, { held: true }>;
/** The revision rises on every transition, so an expiry watcher clears only the state it read. */
interface State {
  readonly revision: number;
  readonly status: QueuePromotionHoldStatus;
}

const expired = (status: Held, now: DateTime.Utc) =>
  DateTime.isGreaterThanOrEqualTo(now, status.expiresAt);

const make = Effect.gen(function* () {
  const threadLocks = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const scope = yield* Effect.scope;
  // Seeded per process, so a cutover named before a restart is refused after it.
  const stateRef = yield* SynchronizedRef.make<State>({
    revision: 0,
    status: { held: false, generation: yield* Clock.currentTimeMillis },
  });
  const watcherRef = yield* Ref.make<Fiber.Fiber<void> | null>(null);
  const releasesPubSub = yield* PubSub.unbounded<void>();
  const status = Effect.map(SynchronizedRef.get(stateRef), (state) => state.status);

  /** One atomic transition; `decide` returns the status to move to, or null to leave the state alone. */
  const transition = (
    decide: (state: State, now: DateTime.Utc) => QueuePromotionHoldStatus | null,
  ) =>
    Effect.flatMap(DateTime.now, (now) =>
      SynchronizedRef.modify(stateRef, (before) => {
        const next = decide(before, now);
        const after = next === null ? before : { revision: before.revision + 1, status: next };
        return [{ before, after }, after];
      }),
    );

  // Decisions run under their thread's lock, so one taken before a transition has committed once
  // that lock is free.
  const awaitEarlierDecisions = <E>(orElse: () => Effect.Effect<void, E>) =>
    Effect.flatMap(threadLocks.activeKeys, (lockedThreads) =>
      Effect.forEach(lockedThreads, (threadId) => threadLocks.withLock(threadId, Effect.void), {
        concurrency: "unbounded",
        discard: true,
      }),
    ).pipe(Effect.timeoutOrElse({ duration: QUEUE_PROMOTION_HOLD_LOCK_WAIT, orElse }));
  /** Published once decisions taken while held have committed, so the sweep finds their runs. */
  const publishReleases = awaitEarlierDecisions(() =>
    Effect.logWarning("A run start decided under the queue promotion hold is still committing"),
  ).pipe(Effect.andThen(PubSub.publish(releasesPubSub, undefined)), Effect.forkIn(scope));

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

  /** One watcher sleeps at a time; a transition replaces the previous one. */
  const watchExpiry = Effect.fn("QueuePromotionHold.watchExpiry")(function* (
    state: State,
    expiresAt: DateTime.Utc,
  ) {
    const watcher = yield* Effect.flatMap(DateTime.now, (now) =>
      Effect.sleep(DateTime.distance(now, expiresAt)),
    ).pipe(Effect.andThen(clear(state.revision)), Effect.forkIn(scope));
    const previous = yield* Ref.getAndSet(watcherRef, watcher);
    if (previous !== null) yield* Fiber.interrupt(previous);
  });

  /** `current` keeps the phase of a held hold; `drain` also brings a cutover back to drain. */
  const holdIn = Effect.fn("QueuePromotionHold.hold")(function* (requested: "current" | "drain") {
    const { before, after } = yield* transition(({ status }, now) =>
      held(status, !status.held || requested === "drain" ? "drain" : status.phase, now),
    );
    const next = after.status as Held;
    yield* watchExpiry(after, next.expiresAt);
    if (!before.status.held) {
      yield* Effect.logInfo("Queue promotion held until the server restarts", {
        generation: next.generation,
      });
    } else if (before.status.phase !== next.phase) {
      yield* publishReleases;
    }
    yield* awaitEarlierDecisions(() => Effect.fail(new QueuePromotionHoldLockWaitError(next)));
    return next;
  });

  const cutover = Effect.fn("QueuePromotionHold.cutover")(function* (generation: number) {
    const accepts = (status: QueuePromotionHoldStatus, now: DateTime.Utc): status is Held =>
      status.held && status.generation === generation && !expired(status, now);
    const { before, after } = yield* transition(({ status }, now) =>
      accepts(status, now) ? held(status, "cutover", now) : null,
    );
    if (after === before) {
      return yield* new QueuePromotionHoldStaleError({ generation, status: after.status });
    }
    yield* watchExpiry(after, (after.status as Held).expiresAt);
    yield* awaitEarlierDecisions(() =>
      Effect.fail(new QueuePromotionHoldLockWaitError({ phase: "cutover" })),
    );
    // Checked again: a release, an expiry or a drain while the barrier waited refuses the cutover.
    const now = yield* DateTime.now;
    const current = yield* status;
    if (!accepts(current, now) || current.phase !== "cutover") {
      return yield* new QueuePromotionHoldStaleError({ generation, status: current });
    }
    return current;
  });

  return QueuePromotionHold.of({
    status,
    phase: Effect.map(status, (current) => (current.held ? current.phase : null)),
    hold: holdIn("current"),
    drain: holdIn("drain"),
    cutover,
    release: clear(),
    releases: Stream.fromPubSub(releasesPubSub),
  });
});

export const layer = Layer.effect(QueuePromotionHold, make);
