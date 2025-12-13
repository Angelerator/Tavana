import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 3000,
    proxy: {
      '/api': {
        // Use Kind's mapped port (30080 -> 28080) for direct access
        // Or use 8080 if running kubectl port-forward
        target: process.env.GATEWAY_URL || 'http://localhost:28080',
        changeOrigin: true,
      },
    },
  },
})

