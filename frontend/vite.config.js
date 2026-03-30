import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";

export default defineConfig({
  plugins: [svelte()],
  build: {
    outDir: "build",
    lib: {
      entry: "src/index.js",
      formats: ["es"],
      fileName: "index",
    },
    rollupOptions: {
      external: [
        "$lib/plugin-invoke",
        "$lib/i18n",
        "$lib/stores/download-store.svelte",
        "$lib/stores/settings-store.svelte",
        "$lib/stores/toast-store.svelte",
        "$lib/stores/convert-store.svelte",
        "$lib/stores/download-listener",
        "$app/navigation",
        "$components/hints/ContextHint.svelte",
        "@tauri-apps/api/core",
        "@tauri-apps/api/event",
        "@tauri-apps/plugin-dialog",
        "svelte",
      ],
    },
  },
});
