// fork:model-allowlist: set by POST /api/server/hold-queue-promotion before an idle-gated
// restart; only the restart clears it. While set, a finished run starts no queued run.
let held = false;

export function holdQueuePromotion(): void {
  held = true;
}

export function queuePromotionHeld(): boolean {
  return held;
}
