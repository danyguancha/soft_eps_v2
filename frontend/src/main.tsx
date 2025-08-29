// src/main.tsx
import '@ant-design/v5-patch-for-react-19';
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { AlertProvider } from './components/alerts/AlertProvider'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <AlertProvider>  
      <App />
    </AlertProvider>
  </StrictMode>,
)
