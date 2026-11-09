// @effect-diagnostics nodeBuiltinImport:off - the fork guard reads server sources.
// fork:model-allowlist
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";
import * as NetService from "@t3tools/shared/Net";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/cli";
import { vi } from "vite-plus/test";

import { cli } from "../binCli.ts";
import {
  ALLOWED_MODELS_ENV,
  allowedModels,
  filterAllowedModels,
  isModelAllowed,
} from "./allowedModels.ts";
import { withProductionAllowlist } from "./allowedModels.testkit.ts";

const claude = ProviderDriverKind.make("claudeAgent");
const codex = ProviderDriverKind.make("codex");

describe("T3_ALLOWED_MODELS", () => {
  it.effect.each(["", " ", "gpt-6-astra,,gpt-6.1-sol", "gpt-6-astra,"])("refuses %j", (value) =>
    Effect.sync(() => {
      vi.stubEnv(ALLOWED_MODELS_ENV, value);
      assert.throws(() => allowedModels(), ALLOWED_MODELS_ENV);
    }).pipe(withProductionAllowlist),
  );

  it.effect("the server refuses to start without it", () =>
    Effect.gen(function* () {
      delete process.env[ALLOWED_MODELS_ENV];
      const baseDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-allowlist-"));
      const exit = yield* Effect.exit(
        Command.runWith(cli, { version: "0.0.0" })(["serve", "--base-dir", baseDir]).pipe(
          Effect.provide(Layer.mergeAll(NodeServices.layer, NetService.layer, TestConsole.layer)),
        ),
      );
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.include(Cause.pretty(exit.cause), ALLOWED_MODELS_ENV);
      assert.deepEqual(NodeFS.readdirSync(baseDir), []);
    }).pipe(withProductionAllowlist),
  );

  it.effect("allows the listed slugs, with or without a 1M context, and nothing else", () =>
    Effect.sync(() => {
      for (const model of ["claude-opus-5-5", "claude-fable-5-1[1m]", "claude-mythos-5-1[1m]"]) {
        assert.isTrue(isModelAllowed(model, claude), model);
      }
      for (const model of ["claude-sonnet-5-5", "claude-haiku-5-5", "claude-opus-5", "opus"]) {
        assert.isFalse(isModelAllowed(model, claude), model);
      }
      assert.isTrue(isModelAllowed("openai.gpt-6-astra", codex));
      assert.isFalse(isModelAllowed("gpt-6-luna", codex));
      assert.deepEqual(
        filterAllowedModels(
          [{ slug: "claude-opus-5-5" }, { slug: "codex-gpt-6-luna" }, { slug: "claude-fable-5" }],
          claude,
        ),
        [{ slug: "claude-opus-5-5" }],
      );
    }).pipe(withProductionAllowlist),
  );
});

// An upstream merge that adds a turn-start path outside ProviderTurnStartService fails here.
describe("every turn start passes the allowlist check", () => {
  const root = NodePath.resolve(import.meta.dirname, "..");
  const sources = (NodeFS.readdirSync(root, { recursive: true }) as string[])
    .filter((file) => file.endsWith(".ts") && !/\.test\.ts$|testkit|testUtils/.test(file))
    .map((file) => ({
      file: file.split(NodePath.sep).join("/"),
      text: NodeFS.readFileSync(NodePath.join(root, file), "utf8"),
    }));
  const callers = (pattern: RegExp) =>
    sources
      .filter(({ text }) => pattern.test(text))
      .map(({ file }) => file)
      .toSorted();

  it("starts provider turns only from the known places", () => {
    assert.deepEqual(callers(/providerTurnStart\.start\(/), ["orchestration-v2/EffectWorker.ts"]);
    assert.deepEqual(callers(/\.startRootRun\(/), ["orchestration-v2/ProviderTurnStartService.ts"]);
    assert.deepEqual(callers(/\.(startTurn|compactThread)\??\.?\(/), [
      "orchestration-v2/ProviderSessionManager.ts",
      "orchestration-v2/ProviderTurnStartService.ts",
      "orchestration-v2/RunExecutionService.ts",
    ]);
  });

  it("checks the model before ProviderTurnStartService opens a session", () => {
    const text = sources.find(
      ({ file }) => file === "orchestration-v2/ProviderTurnStartService.ts",
    )!.text;
    const check = text.indexOf("isModelAllowed(run.modelSelection.model");
    assert.isAbove(check, 0);
    assert.isBelow(check, text.indexOf("providerSessions.open("));
    assert.isBelow(check, text.indexOf("runExecution.startRootRun("));
  });
});
