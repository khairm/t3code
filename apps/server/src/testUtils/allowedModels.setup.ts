// fork:model-allowlist: upstream suites use many model ids; allowlist tests opt back in.
import { overrideAllowedModelsForTests } from "../provider/allowedModels.ts";

overrideAllowedModelsForTests("any");
