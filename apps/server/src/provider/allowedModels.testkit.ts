// fork:model-allowlist: run a test under the list every machine sets.
import * as Effect from "effect/Effect";
import { vi } from "vite-plus/test";

import { ALLOWED_MODELS_ENV } from "./allowedModels.ts";
import { overrideAllowedModelsForTests } from "./allowedModelsTestOverride.ts";

const PRODUCTION_ALLOWED_MODELS =
  "claude-fable-5-1,claude-opus-5-5,claude-mythos-5-1,gpt-6-astra,gpt-6.1-sol";

export const withProductionAllowlist = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      overrideAllowedModelsForTests(undefined);
      vi.stubEnv(ALLOWED_MODELS_ENV, PRODUCTION_ALLOWED_MODELS);
    }),
    () => effect,
    () =>
      Effect.sync(() => {
        vi.unstubAllEnvs();
        overrideAllowedModelsForTests("any");
      }),
  );
