import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import fs from 'fs'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    https: {
      key: fs.readFileSync('./cert/localhost+3-key.pem'),
      cert: fs.readFileSync('./cert/localhost+3.pem'),
    }
  }
})
