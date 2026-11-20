// El proxy sólo existe para `npm run dev` en la laptop; en Docker, nginx es
// quien reenvía /collect y /api al backend. Mismo contrato en los dos lados.
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/collect': 'http://localhost:8084',
      '/api': 'http://localhost:8084',
    },
  },
})
