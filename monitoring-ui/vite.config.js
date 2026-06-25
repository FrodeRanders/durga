import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

const apiTarget = process.env.VITE_API_TARGET || 'http://localhost:8081'
const __dirname = path.dirname(fileURLToPath(import.meta.url))

export default defineConfig({
  plugins: [svelte()],
  server: {
    proxy: {
      '/api': apiTarget
    },
    hmr: process.env.VITE_NO_HMR === '1' ? false : undefined
  },
  cacheDir: process.env.VITE_CACHE_DIR
    || path.join(__dirname, 'node_modules', '.vite'),
  build: {
    outDir: 'dist',
    emptyOutDir: true
  }
})
