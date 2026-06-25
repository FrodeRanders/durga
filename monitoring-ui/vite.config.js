import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

const apiTarget = process.env.VITE_API_TARGET || 'http://localhost:8081'

export default defineConfig({
  plugins: [svelte()],
  server: {
    proxy: {
      '/api': apiTarget
    }
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true
  }
})
