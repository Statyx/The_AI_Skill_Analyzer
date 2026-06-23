import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Fabric static apps are served from the item root; keep base relative.
export default defineConfig({
  base: "./",
  plugins: [react()],
  build: {
    outDir: "dist",
    sourcemap: false,
  },
  server: {
    port: 5173,
  },
});
