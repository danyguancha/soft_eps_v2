// src/services/HealthMonitor.ts
import api from '../Api';

interface HealthCheckResponse {
  status: string;
  port: number;
  timestamp: number;
  version: string;
}

class HealthMonitor {
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private currentPort: number = 8001;
  private isChecking: boolean = false;
  private onPortChangeCallback?: (newPort: number, oldPort: number) => void;
  private onServerDownCallback?: () => void;
  private onServerUpCallback?: (port: number) => void;
  private consecutiveFailures: number = 0;
  private maxConsecutiveFailures: number = 3;
  private wasServerDown: boolean = false;
  
  /**
   * Inicia el monitoreo de salud del servidor
   * @param callbacks - Callbacks opcionales para eventos
   * @param interval - Intervalo de chequeo en milisegundos (default: 10s)
   */
  start(
    callbacks?: {
      onPortChange?: (newPort: number, oldPort: number) => void;
      onServerDown?: () => void;
      onServerUp?: (port: number) => void;
    },
    interval: number = 10000
  ) {
    this.onPortChangeCallback = callbacks?.onPortChange;
    this.onServerDownCallback = callbacks?.onServerDown;
    this.onServerUpCallback = callbacks?.onServerUp;
    
    console.log(`🔍 Iniciando monitoreo de servidor cada ${interval / 1000}s`);
    
    // Check inmediato al iniciar
    this.checkHealth();
    
    // Iniciar intervalo
    this.intervalId = setInterval(() => {
      this.checkHealth();
    }, interval);
  }
  
  /**
   * Verifica la salud del servidor
   */
  private async checkHealth() {
    if (this.isChecking) return;
    
    this.isChecking = true;
    
    try {
      // Intentar en el puerto actual primero
      const response = await fetch(`http://localhost:${this.currentPort}/health`, {
        signal: AbortSignal.timeout(5000),
        method: 'GET',
        headers: { 'Content-Type': 'application/json' }
      });
      
      if (response.ok) {
        const data: HealthCheckResponse = await response.json();
        
        // Reset de failures si el servidor responde
        if (this.wasServerDown) {
          console.log('Servidor recuperado');
          this.wasServerDown = false;
          this.onServerUpCallback?.(data.port);
        }
        this.consecutiveFailures = 0;
        
        // Si el puerto reportado es diferente, actualizar
        if (data.port && data.port !== this.currentPort) {
          console.log(`🔄 Puerto cambió: ${this.currentPort} → ${data.port}`);
          this.updatePort(data.port);
        }
        
        this.isChecking = false;
        return;
      }
    } catch (error) {
      // Si falla, incrementar contador e intentar otros puertos
      this.consecutiveFailures++;
      
      if (this.consecutiveFailures >= this.maxConsecutiveFailures && !this.wasServerDown) {
        console.log('⚠️ Servidor no responde, buscando en otros puertos...');
        this.wasServerDown = true;
        this.onServerDownCallback?.();
      }
      
      await this.tryFindNewPort();
    }
    
    this.isChecking = false;
  }
  
  /**
   * Intenta encontrar el servidor en otros puertos conocidos
   */
  private async tryFindNewPort() {
    const ports = [8001, 8002, 8003, 8080, 8081, 3000, 5000];
    
    for (const port of ports) {
      if (port === this.currentPort) continue;
      
      try {
        const response = await fetch(`http://localhost:${port}/health`, {
          signal: AbortSignal.timeout(3000),
          method: 'GET',
          headers: { 'Content-Type': 'application/json' }
        });
        
        if (response.ok) {
          const data: HealthCheckResponse = await response.json();
          console.log(`Servidor encontrado en puerto ${data.port}`);
          
          if (this.wasServerDown) {
            this.wasServerDown = false;
            this.onServerUpCallback?.(data.port);
          }
          
          this.consecutiveFailures = 0;
          this.updatePort(data.port);
          return;
        }
      } catch {
        // Continuar con el siguiente puerto
        continue;
      }
    }
    
    console.log('❌ No se encontró servidor en ningún puerto conocido');
  }
  
  /**
   * Actualiza el puerto actual y la baseURL de axios
   */
  private updatePort(newPort: number) {
    const oldPort = this.currentPort;
    this.currentPort = newPort;
    
    // Actualizar baseURL de axios
    api.defaults.baseURL = `http://localhost:${newPort}/api/v1`;
    
    console.log(`📡 API actualizada: http://localhost:${newPort}/api/v1`);
    
    // Notificar cambio
    if (this.onPortChangeCallback && oldPort !== newPort) {
      this.onPortChangeCallback(newPort, oldPort);
    }
  }
  
  /**
   * Detiene el monitoreo
   */
  stop() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
      console.log('🛑 Monitoreo detenido');
    }
  }
  
  /**
   * Obtiene el puerto actual
   */
  getCurrentPort(): number {
    return this.currentPort;
  }
  
  /**
   * Verifica manualmente la salud del servidor (force check)
   */
  async forceCheck(): Promise<boolean> {
    try {
      const response = await fetch(`http://localhost:${this.currentPort}/health`, {
        signal: AbortSignal.timeout(5000)
      });
      return response.ok;
    } catch {
      return false;
    }
  }
}

// Exportar instancia singleton
export const healthMonitor = new HealthMonitor();
