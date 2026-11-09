// fork:model-allowlist: the machine-wide model allowlist. Every model list, turn start,
// delegate_task and text generation checks it here.
import type { OrchestrationV2ProviderFailure, ProviderDriverKind } from "@t3tools/contracts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { codexModelFamily, normalizeModelSlug } from "@t3tools/shared/model";

export const ALLOWED_MODELS_ENV = "T3_ALLOWED_MODELS";
export const MODEL_NOT_ALLOWED_CODE = "model_not_allowed";

let testOverride: "any" | undefined;

/** Test setup only: upstream suites run on many model ids, so they run unrestricted. */
export function overrideAllowedModelsForTests(value: "any" | undefined): void {
  testOverride = value;
}

/** Throws when T3_ALLOWED_MODELS is missing, empty or has an empty entry. */
export function allowedModels(): ReadonlySet<string> | "any" {
  if (testOverride !== undefined) return testOverride;
  const raw = process.env[ALLOWED_MODELS_ENV]?.trim() ?? "";
  const slugs = raw.split(",").map((slug) => slug.trim());
  if (raw === "" || slugs.includes("")) {
    throw new Error(
      `${ALLOWED_MODELS_ENV} must list the model slugs this machine may run, comma-separated; got ${JSON.stringify(process.env[ALLOWED_MODELS_ENV] ?? null)}.`,
    );
  }
  return new Set(slugs);
}

/** After T3's alias resolution; a 1M-context suffix keeps the slug. */
function allowedIn(
  allowed: ReadonlySet<string> | "any",
  model: string,
  driver: ProviderDriverKind,
): boolean {
  if (allowed === "any") return true;
  const slug = normalizeModelSlug(model, driver)?.replace(/\[1m\]$/i, "");
  if (slug === undefined) return false;
  return allowed.has(driver === "codex" ? codexModelFamily(slug) : slug);
}

export function isModelAllowed(model: string, driver: ProviderDriverKind): boolean {
  return allowedIn(allowedModels(), model, driver);
}

export function filterAllowedModels<M extends { readonly slug: string }>(
  models: ReadonlyArray<M>,
  driver: ProviderDriverKind,
): ReadonlyArray<M> {
  const allowed = allowedModels();
  return models.filter((model) => allowedIn(allowed, model.slug, driver));
}

export const modelNotAllowedMessage = (model: string) => `Model not allowed: ${model}`;

export function modelNotAllowedFailure(model: string): OrchestrationV2ProviderFailure {
  return makeProviderFailure({
    class: "validation_error",
    code: MODEL_NOT_ALLOWED_CODE,
    message: modelNotAllowedMessage(model),
    retryable: false,
  });
}
