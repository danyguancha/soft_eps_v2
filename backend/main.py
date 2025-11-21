# main.py
import os
import time
import threading
import warnings
import socket
import asyncio
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
warnings.filterwarnings("ignore", category=UserWarning)
from api.routes import router
from api.technical_note_routes import router as technical_note_router
from middleware.content_size_limit import ContentSizeLimitMiddleware

# ========== CONFIGURACIÓN ==========
class Config:
    MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024
    UPLOAD_DIR = "uploads"
    REQUEST_TIMEOUT = 300
    PREFERRED_PORT = int(os.getenv("PORT", 8000))
    FALLBACK_PORTS = [8000, 8001, 8002, 8003, 8080, 8081, 3000, 5000]
    MONITORING_INTERVAL = 10
    MAX_RESTARTS = 50
    AUTO_RESTART = os.getenv("AUTO_RESTART", "true").lower() == "true"


name_project = "Sistema de Evaluación de Nota Técnica"
# ========== PORT MANAGER ==========
class PortManager:
    def __init__(self):
        self.current_port = None
        self.host = "0.0.0.0"
        self.should_monitor = True
        self.restart_count = 0
        self.monitoring_thread = None
        self.last_restart_time = 0
    
    def find_available_port(self, preferred: int = None) -> int:
        """Encuentra puerto disponible"""
        ports = ([preferred] if preferred else []) + Config.FALLBACK_PORTS + list(range(8000, 8100))
        
        for port in ports:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    s.bind((self.host, port))
                    return port
            except OSError:
                continue
        
        # Puerto aleatorio como último recurso
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, 0))
            return s.getsockname()[1]
    
    def is_server_healthy(self) -> bool:
        """Verifica salud del servidor"""
        if not self.current_port:
            return False
        
        try:
            import requests
            response = requests.get(
                f"http://127.0.0.1:{self.current_port}/health",
                timeout=5
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def start_monitoring(self):
        """Inicia monitoreo en background"""
        if not Config.AUTO_RESTART:
            print("ℹ️ Auto-restart deshabilitado")
            return
        
        if not self.monitoring_thread:
            self.monitoring_thread = threading.Thread(
                target=self._monitor_loop,
                daemon=True,
                name="HealthMonitor"
            )
            self.monitoring_thread.start()
            print("✓ Monitoreo de salud iniciado")
    
    def _monitor_loop(self):
        """Loop de monitoreo de salud"""
        consecutive_failures = 0
        
        while self.should_monitor and self.restart_count < Config.MAX_RESTARTS:
            try:
                time.sleep(Config.MONITORING_INTERVAL)
                
                if self.is_server_healthy():
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    print(f"Servidor no responde (fallas: {consecutive_failures}/5)")
                
                if consecutive_failures >= 5:
                    print("Iniciando reinicio automático...")
                    self._trigger_restart()
                    consecutive_failures = 0
                    
            except Exception as e:
                print(f"Error en monitoreo: {e}")
    
    def _trigger_restart(self):
        """Activa el reinicio en nuevo puerto"""
        current_time = time.time()
        
        # Throttling: esperar al menos 60s entre reinicios
        if current_time - self.last_restart_time < 60:
            print("⏳ Esperando antes del siguiente reinicio...")
            time.sleep(60)
        
        self.last_restart_time = current_time
        self.restart_count += 1
        
        if self.restart_count >= Config.MAX_RESTARTS:
            print(f"🛑 Máximo de reinicios alcanzado ({Config.MAX_RESTARTS})")
            self.should_monitor = False
            return
        
        old_port = self.current_port
        new_port = self.find_available_port()
        
        # Evitar mismo puerto
        if new_port == old_port:
            Config.FALLBACK_PORTS = [p for p in Config.FALLBACK_PORTS if p != old_port]
            new_port = self.find_available_port()
        
        print(f"Reinicio #{self.restart_count}: puerto {old_port} → {new_port}")
        
        # Iniciar nueva instancia
        self._start_new_instance(new_port)
        self.current_port = new_port
        
        print(f"✓ Servidor reiniciado en puerto {new_port}")
    
    def _start_new_instance(self, port: int):
        """Inicia nueva instancia del servidor"""
        def run_server():
            try:
                config = uvicorn.Config(
                    app="main:app",
                    host=self.host,
                    port=port,
                    reload=False,
                    log_level="info",
                    timeout_keep_alive=Config.REQUEST_TIMEOUT,
                    limit_concurrency=50,
                )
                server = uvicorn.Server(config)
                asyncio.run(server.serve())
            except Exception as e:
                print(f"Error en instancia (puerto {port}): {e}")
        
        thread = threading.Thread(target=run_server, daemon=True, name=f"Server-{port}")
        thread.start()
    
    def stop_monitoring(self):
        """Detiene el monitoreo"""
        self.should_monitor = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)



port_manager = PortManager()

# ========== MIDDLEWARE ==========
class UnifiedMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Log uploads grandes
        if "/upload" in str(request.url):
            if cl := request.headers.get("content-length"):
                size_mb = int(cl) / (1024 * 1024)
                if size_mb > 100:
                    print(f"📤 Upload: {size_mb:.1f}MB")
        
        try:
            response = await call_next(request)
            
            # Headers de seguridad
            response.headers.update({
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "X-XSS-Protection": "1; mode=block",
                "Referrer-Policy": "strict-origin-when-cross-origin"
            })
            
            if "/upload" in str(request.url):
                elapsed = time.time() - start_time
                print(f"✓ Upload procesado: {elapsed:.2f}s")
            
            return response
            
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"Error en request ({elapsed:.2f}s): {e}")
            raise

# ========== CREAR APP ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ===== STARTUP =====
    print("🚀 Iniciando servicios...")
    
    # Inicializar servicios
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        print("✓ DuckDB Service disponible")
    except Exception as e:
        print(f"DuckDB: {e}")
    
    # Crear directorios
    os.makedirs(Config.UPLOAD_DIR, exist_ok=True)
    print(f"✓ Directorios creados: {Config.UPLOAD_DIR}")
    
    # Configurar puerto
    port_manager.current_port = Config.PREFERRED_PORT
    print(f"✓ Puerto configurado: {port_manager.current_port}")
    
    # Iniciar monitoreo
    port_manager.start_monitoring()
    
    print("Sistema listo")
    print("=" * 60)
    
    yield
    
    # ===== SHUTDOWN =====
    print("\n" + "=" * 60)
    print("🛑 Deteniendo sistema...")
    print("=" * 60)
    
    # Detener monitoreo
    port_manager.stop_monitoring()
    print("✓ Monitoreo detenido")
    
    # Limpiar servicios
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        if hasattr(safe_duckdb_service, '_service'):
            safe_duckdb_service._service.close()
            print("✓ Conexión DuckDB cerrada")
    except Exception as e:
        print(f"Error cerrando DuckDB: {e}")
    
    print("Sistema detenido correctamente")
    print("=" * 60)

# Crear app
app = FastAPI(
    title="Sistema de Evaluación de Nota Técnica",
    description="API para el procesamiento y evaluación de notas técnicas. Basadas en la estructura del software SIGIRES.",
    version="2.4.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# ========== MIDDLEWARE ==========
app.add_middleware(ContentSizeLimitMiddleware, max_content_size=Config.MAX_FILE_SIZE)
app.add_middleware(UnifiedMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=3600,
)

# ========== HTTP ENDPOINTS ==========
@app.get("/health")
def health_check():
    """Health check endpoint - usado para monitoreo y detección de puerto"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "version": "2.4.0",
        "port": port_manager.current_port,
        "monitoring": port_manager.should_monitor,
        "restart_count": port_manager.restart_count
    }

# ========== ROUTERS ==========
app.include_router(router, prefix="/api/v1", tags=["API"])
app.include_router(technical_note_router, prefix="/api/v1/technical-note", tags=["Technical Note"])

# ========== EJECUCIÓN ==========
if __name__ == "__main__":    
    try:
        port = port_manager.find_available_port(Config.PREFERRED_PORT)
        port_manager.current_port = port
        uvicorn.run(
            "main:app",
            host=port_manager.host,
            port=port,
            reload=False,
            log_level="info",
            timeout_keep_alive=Config.REQUEST_TIMEOUT,
        )
    except KeyboardInterrupt:
        print("\n✋ Aplicación detenida por el usuario")
    except Exception as e:
        print(f"\nError crítico: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("👋 Fin de ejecución")
