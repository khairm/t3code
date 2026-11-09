// fork:model-allowlist: apart from allowedModels.ts, so the test setup that sets it loads no
// server modules before a test file's vi.mock applies.
let override: "any" | undefined;

/** Test setup only: upstream suites run on many model ids, so they run unrestricted. */
export function overrideAllowedModelsForTests(value: "any" | undefined): void {
  override = value;
}

export const allowedModelsTestOverride = (): "any" | undefined => override;
