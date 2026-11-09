// fork:model-allowlist: the idle-gated restart holds every new run start (the Orchestrator's gates)
// so no queued or server-created message starts between its busy check and the restart. The
// restart, an explicit release or a 30-minute expiry clears it.
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

const QUEUE_PROMOTION_HOLD_EXPIRY = Duration.minutes(30);
export const QUEUE_PROMOTION_HELD_MESSAGE =
  "T3 is holding new runs for a server restart. Try again once it has restarted.";

export interface QueuePromotionHoldShape {
  /** Null when not held. Read under the thread's dispatch lock, so `hold` can wait out a decision. */
  readonly heldSince: Effect.Effect<DateTime.Utc | null>;
  /** Returns once every start decision taken before the hold has committed. */
  readonly hold: Effect.Effect<void>;
  readonly release: Effect.Effect<void>;
  /** Emits on each release, so runs queued during the hold can start. */
  readonly releases: Stream.Stream<void>;
}

export class QueuePromotionHold extends Context.Service<
  QueuePromotionHold,
  QueuePromotionHoldShape
>()("t3/orchestration-v2/QueuePromotionHold") {}

const make = Effect.gen(function* () {
  const threadLocks = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const scope = yield* Effect.scope;
  const heldSinceRef = yield* SynchronizedRef.make<DateTime.Utc | null>(null);
  const releasesPubSub = yield* PubSub.unbounded<void>();

  /** With `expiring`, clears only the hold that expiry timer was started for. */
  const clear = Effect.fnUntraced(function* (expiring?: DateTime.Utc) {
    const cleared = yield* SynchronizedRef.modify(heldSinceRef, (heldSince) =>
      heldSince === null || (expiring !== undefined && heldSince !== expiring)
        ? [null, heldSince]
        : [heldSince, null],
    );
    if (cleared === null) return;
    const fields = { heldSince: DateTime.formatIso(cleared) };
    yield* expiring === undefined
      ? Effect.logInfo("Queue promotion released", fields)
      : Effect.logWarning(
          "Queue promotion hold expired without a server restart; releasing it",
          fields,
        );
    // After the state change, so runs resumed on this event see the hold cleared.
    yield* PubSub.publish(releasesPubSub, undefined);
  });

  const hold = Effect.gen(function* () {
    yield* SynchronizedRef.updateEffect(heldSinceRef, (current) =>
      current !== null
        ? Effect.succeed(current)
        : Effect.gen(function* () {
            const heldSince = yield* DateTime.now;
            yield* Effect.sleep(QUEUE_PROMOTION_HOLD_EXPIRY).pipe(
              Effect.andThen(clear(heldSince)),
              Effect.forkIn(scope),
            );
            yield* Effect.logInfo("Queue promotion held until the server restarts");
            return heldSince;
          }),
    );
    // Decisions run under their thread's lock, so one taken before the flag was set has
    // committed once that lock is free.
    const lockedThreads = yield* threadLocks.activeKeys;
    yield* Effect.forEach(
      lockedThreads,
      (threadId) => threadLocks.withLock(threadId, Effect.void),
      { concurrency: "unbounded", discard: true },
    );
  });

  return QueuePromotionHold.of({
    heldSince: SynchronizedRef.get(heldSinceRef),
    hold,
    release: clear(),
    releases: Stream.fromPubSub(releasesPubSub),
  });
});

export const layer = Layer.effect(QueuePromotionHold, make);
