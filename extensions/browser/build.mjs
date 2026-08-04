import { build } from "esbuild";
import { mkdir, cp } from "node:fs/promises";

await mkdir("dist", { recursive: true });
await build({ entryPoints: ["src/service-worker.ts", "src/popup.ts", "src/integration.ts", "src/native-bridge.ts"], outdir: "dist", bundle: true, format: "esm", platform: "browser", target: "es2022", sourcemap: false });
await cp("manifest.json", "dist/manifest.json");
await cp("popup.html", "dist/popup.html");
