// fork:model-allowlist: the machine-wide model allowlist. Every model list, turn start,
// delegate_task and text generation checks it here.
import type {
  OrchestrationV2ProviderFailure,
  ProviderDriverKind,
  ServerProvider,
} from "@t3tools/contracts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { codexModelFamily, normalizeModelSlug } from "@t3tools/shared/model";

import { allowedModelsTestOverride } from "./allowedModelsTestOverride.ts";
import { BUNDLED_CLAUDE_MODEL_CATALOG, resolveClaudeModelSlug } from "./ClaudeModelCatalog.ts";

export const ALLOWED_MODELS_ENV = "T3_ALLOWED_MODELS";
export const MODEL_NOT_ALLOWED_CODE = "model_not_allowed";

/** Native Codex helpers can't be model-checked, so they stay off; helpers use delegate_task. */
export const CODEX_HELPERS_OFF_CONFIG = {
  "agents.enabled": false,
  "features.multi_agent_v2": false,
} as const;

/** A canonical slug: what `allowlistSlug` produces, so an entry can match. */
const CANONICAL_MODEL_SLUG = /^(claude|gpt)-[a-z0-9][a-z0-9.-]*$/;

let parsed: { readonly raw: string; readonly slugs: ReadonlySet<string> } | undefined;

/** Throws when T3_ALLOWED_MODELS is missing, empty, or has an entry that is not a canonical slug. */
export function allowedModels(): ReadonlySet<string> | "any" {
  const testOverride = allowedModelsTestOverride();
  if (testOverride !== undefined) return testOverride;
  const raw = process.env[ALLOWED_MODELS_ENV]?.trim() ?? "";
  if (parsed?.raw === raw) return parsed.slugs;
  const slugs = raw.split(",").map((slug) => slug.trim());
  if (raw === "" || slugs.includes("")) {
    throw new Error(
      `${ALLOWED_MODELS_ENV} must list the model slugs this machine may run, comma-separated; got ${JSON.stringify(process.env[ALLOWED_MODELS_ENV] ?? null)}.`,
    );
  }
  const invalid = slugs.find((slug) => !CANONICAL_MODEL_SLUG.test(slug));
  if (invalid !== undefined) {
    throw new Error(
      `${ALLOWED_MODELS_ENV} entry ${JSON.stringify(invalid)} is not a canonical model slug: lowercase, a claude- or gpt- prefix, no [1m] or other suffix.`,
    );
  }
  parsed = { raw, slugs: new Set(slugs) };
  return parsed.slugs;
}

/** The slug the allowlist names for a model on a driver, or null when no entry can match. */
function allowlistSlug(model: string, driver: ProviderDriverKind): string | null {
  const slug = normalizeModelSlug(model, driver);
  if (slug === null) return null;
  if (driver === "claudeAgent") {
    const resolved = resolveClaudeModelSlug(
      BUNDLED_CLAUDE_MODEL_CATALOG,
      slug.replace(/\[1m\]$/i, ""),
    );
    return resolved.startsWith("claude-") ? resolved : null;
  }
  if (driver === "codex") {
    const family = codexModelFamily(slug);
    return family.startsWith("gpt-") ? family : null;
  }
  return null;
}

function allowedIn(
  allowed: ReadonlySet<string> | "any",
  model: string,
  driver: ProviderDriverKind,
): boolean {
  if (allowed === "any") return true;
  const slug = allowlistSlug(model, driver);
  return slug !== null && allowed.has(slug);
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

/** Cached and merged snapshots too, so a stale cached model never reappears. */
export function filterAllowedProviderModels(provider: ServerProvider): ServerProvider {
  return {
    ...provider,
    models: filterAllowedModels(provider.models, provider.driver),
    ...(provider.updateRequiredModels && {
      updateRequiredModels: filterAllowedModels(provider.updateRequiredModels, provider.driver),
    }),
  };
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
