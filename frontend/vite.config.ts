import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',  // Esto permite acceder desde la IP de tu red
    port: 5173,        // Puerto por defecto, puedes cambiarlo si quieres
    // https: {        // <- COMENTADO O ELIMINADO
    //   key: fs.readFileSync('./cert/localhost+3-key.pem'),
    //   cert: fs.readFileSync('./cert/localhost+3.pem'),
    // }
  }
})
