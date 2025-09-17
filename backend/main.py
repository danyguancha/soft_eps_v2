# main.py - VERSIÓN CORREGIDA PARA REINICIO PROGRAMÁTICO
import os
import signal
import sys
import time
import threading
import warnings
import socket
import asyncio
from typing import Optional, List
from fastapi.exceptions import RequestValidationError
import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager
import json

# Suprimir warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
warnings.filterwarnings("ignore", category=UserWarning, message=".*schema_extra.*")

from api.routes import router
from api.technical_note_routes import router as technical_note_router
from middleware.content_size_limit import ContentSizeLimitMiddleware

# ========== CONFIGURACIÓN ==========

class Config:
    MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB
    MAX_UPLOAD_SIZE = 5 * 1024 * 1024 * 1024
    DEFAULT_CHUNK_SIZE = 8192
    PROCESSING_CHUNK_SIZE = 10000
    UPLOAD_DIR = "uploads"
    EXPORTS_DIR = "exports"
    PANDAS_MAX_COLUMNS = 2000
    SAMPLE_ROWS_FOR_PREVIEW = 1000
    REQUEST_TIMEOUT = 300
    UPLOAD_TIMEOUT = 600
    PREFERRED_PORT = int(os.getenv("PREFERRED_PORT", 8000))
    FALLBACK_PORTS = [8001, 8002, 8003, 8080, 8081, 3000, 5000, 9000, 9001, 9002]
    MAX_PORT_RETRIES = int(os.getenv("MAX_PORT_RETRIES", 10))
    AUTO_RESTART = os.getenv("AUTO_RESTART", "true").lower() == "true"
    MONITORING_INTERVAL = 30  # AUMENTADO: Verificar cada 30 segundos en lugar de 10

# ========== GESTOR WEBSOCKET ==========

class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"📡 Cliente WebSocket conectado. Total: {len(self.active_connections)}")
        
        if hasattr(port_manager, 'current_port') and port_manager.current_port:
            await self.send_port_info(websocket, port_manager.current_port)
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        print(f"📡 Cliente WebSocket desconectado. Total: {len(self.active_connections)}")
    
    async def send_port_info(self, websocket: WebSocket, port: int):
        try:
            message = {
                "type": "port_update",
                "port": port,
                "host": port_manager.host if hasattr(port_manager, 'host') else "localhost",
                "timestamp": time.time(),
                "restart_count": port_manager.restart_count if hasattr(port_manager, 'restart_count') else 0
            }
            await websocket.send_text(json.dumps(message))
            print(f"📡 Información de puerto enviada: {port}")
        except Exception as e:
            print(f"❌ Error enviando info de puerto: {e}")
    
    async def broadcast_port_change(self, new_port: int, old_port: int):
        if not self.active_connections:
            print("📡 Sin clientes WebSocket para notificar")
            return
        
        message = {
            "type": "port_changed",
            "new_port": new_port,
            "old_port": old_port,
            "host": port_manager.host if hasattr(port_manager, 'host') else "localhost",
            "timestamp": time.time(),
            "restart_count": port_manager.restart_count if hasattr(port_manager, 'restart_count') else 0,
            "message": f"El servidor se movió del puerto {old_port} al {new_port}"
        }
        
        disconnected_clients = []
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
                print(f"📡 Notificación de cambio de puerto enviada")
            except Exception as e:
                print(f"❌ Error notificando cliente: {e}")
                disconnected_clients.append(connection)
        
        for client in disconnected_clients:
            self.disconnect(client)

# ========== FUNCIÓN PARA CREAR APP (NUEVA) ==========

def create_app() -> FastAPI:
    """Crea una nueva instancia de FastAPI con todos los componentes"""
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        print("🔧 Inicializando servicios...")
        
        try:
            from services.duckdb_service_wrapper import safe_duckdb_service
            print("✅ DuckDB Service disponible")
        except Exception as e:
            print(f"⚠️ DuckDB Service no disponible: {e}")
        
        os.makedirs(Config.UPLOAD_DIR, exist_ok=True)
        os.makedirs(Config.EXPORTS_DIR, exist_ok=True)
        
        print("✅ Servicios inicializados correctamente")
        
        yield    
        
        print("🧹 Limpiando recursos...")
        try:
            from services.duckdb_service_wrapper import safe_duckdb_service
            if hasattr(safe_duckdb_service, '_service'):
                safe_duckdb_service._service.close()
        except:
            pass
        
        print("✅ Recursos liberados")

    # CREAR NUEVA INSTANCIA DE FASTAPI
    new_app = FastAPI(
        title="Sistema de Procesamiento de Archivos - Puerto Dinámico",
        description="API con puerto dinámico y reinicio automático",
        version="2.2.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan
    )

    # MIDDLEWARE
    class LargeFileLoggingMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            start_time = time.time()
            
            if "/upload" in str(request.url):
                content_length = request.headers.get("content-length")
                if content_length:
                    size_mb = int(content_length) / 1024 / 1024
                    if size_mb > 100: 
                        print(f"📤 Upload de archivo grande detectado: {size_mb:.1f}MB")
            
            try:
                response = await call_next(request)
                
                if "/upload" in str(request.url):
                    process_time = time.time() - start_time
                    print(f"✅ Upload procesado en {process_time:.2f}s")
                
                return response
                
            except Exception as e:
                process_time = time.time() - start_time
                print(f"❌ Error en request {request.url}: {e} (tiempo: {process_time:.2f}s)")
                raise

    class SecurityHeadersMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            response = await call_next(request)
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            return response

    # AGREGAR MIDDLEWARE
    new_app.add_middleware(ContentSizeLimitMiddleware, max_content_size=Config.MAX_UPLOAD_SIZE)
    new_app.add_middleware(SecurityHeadersMiddleware)
    new_app.add_middleware(LargeFileLoggingMiddleware)

    new_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=[
            "Authorization", "Content-Type", "Content-Length", "Accept", 
            "Accept-Encoding", "X-Requested-With"
        ],
        max_age=3600,
    )

    # ARCHIVOS ESTÁTICOS
    if os.path.exists(Config.EXPORTS_DIR):
        new_app.mount("/static", StaticFiles(directory=Config.EXPORTS_DIR), name="static")

    # ========== ENDPOINTS WEBSOCKET - ANTES DE ROUTERS ==========

    @new_app.websocket("/ws/port-monitor")
    async def websocket_port_monitor(websocket: WebSocket):
        print(f"📡 Nueva conexión WebSocket solicitada")
        
        await websocket_manager.connect(websocket)
        try:
            while True:
                data = await websocket.receive_text()
                print(f"📡 Mensaje recibido del WebSocket: {data}")
                
                if data == "get_port_info":
                    if port_manager.current_port:
                        await websocket_manager.send_port_info(websocket, port_manager.current_port)
                elif data == "ping":
                    await websocket.send_text("pong")
                    
        except WebSocketDisconnect:
            print("📡 Cliente WebSocket desconectado normalmente")
            websocket_manager.disconnect(websocket)
        except Exception as e:
            print(f"❌ Error en WebSocket port-monitor: {e}")
            websocket_manager.disconnect(websocket)

    # ========== ENDPOINTS BÁSICOS PRIMERO ==========
    
    @new_app.get("/")
    def read_root():
        return {
            "message": "Sistema de Procesamiento de Archivos API - Puerto Dinámico",
            "version": "2.2.0",
            "status": "operational",
            "current_port": port_manager.current_port,
            "restart_count": port_manager.restart_count,
            "monitoring_active": port_manager.should_monitor,
            "websocket_endpoint": f"ws://localhost:{port_manager.current_port}/ws/port-monitor",
            "docs": "/docs",
            "redoc": "/redoc"
        }

    @new_app.get("/health")  # ENDPOINT DIRECTO, NO EN ROUTER
    def health_check():
        return {
            "status": "healthy",
            "timestamp": time.time(),
            "version": "2.2.0",
            "current_port": port_manager.current_port,
            "restart_count": port_manager.restart_count,
            "monitoring": port_manager.should_monitor,
            "server_running": port_manager.is_server_running()
        }

    @new_app.get("/api/v1/health")  # TAMBIÉN AGREGAR EN RUTA API
    def health_check_api():
        return {
            "status": "healthy",
            "timestamp": time.time(),
            "version": "2.2.0",
            "current_port": port_manager.current_port,
            "restart_count": port_manager.restart_count,
            "monitoring": port_manager.should_monitor,
            "server_running": port_manager.is_server_running()
        }

    @new_app.get("/api/v1/discover")
    def discover_service():
        return {
            "service": "Sistema de Procesamiento de Archivos",
            "version": "2.2.0",
            "current_port": port_manager.current_port,
            "host": port_manager.host,
            "base_url": f"http://{port_manager.host}:{port_manager.current_port}",
            "api_base_url": f"http://{port_manager.host}:{port_manager.current_port}/api/v1",
            "websocket_url": f"ws://{port_manager.host}:{port_manager.current_port}/ws/port-monitor",
            "status": "active",
            "timestamp": time.time(),
            "restart_count": port_manager.restart_count
        }

    # ========== INCLUIR ROUTERS AL FINAL ==========
    
    new_app.include_router(router, prefix="/api/v1", tags=["API"])
    new_app.include_router(technical_note_router, prefix="/api/v1/technical-note", tags=["Technical Note"])
    
    print("✅ App creada con todos los endpoints configurados")
    return new_app

# ========== GESTOR DE PUERTOS DINÁMICO (CORREGIDO) ==========

class DynamicPortManager:
    def __init__(self, host="0.0.0.0"):
        self.host = host
        self.current_port = None
        self.current_server = None
        self.monitoring_thread = None
        self.should_monitor = True
        self.restart_count = 0
        self.max_restarts = 50
        self.last_restart_time = 0
        
    def is_port_available(self, port: int) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                sock.bind((self.host, port))
                return True
        except (OSError, socket.timeout):
            return False
    
    def find_available_port(self, preferred_port: Optional[int] = None) -> int:
        ports_to_try = []
        
        if preferred_port:
            ports_to_try.append(preferred_port)
        
        ports_to_try.extend(Config.FALLBACK_PORTS)
        
        for port in range(8000, 8100):
            if port not in ports_to_try:
                ports_to_try.append(port)
        
        for port in ports_to_try:
            if self.is_port_available(port):
                return port
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.host, 0))
            return sock.getsockname()[1]
    
    def is_server_running(self) -> bool:
        if not self.current_port:
            return False
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)  # AUMENTADO: timeout más largo
                result = sock.connect_ex((self.host, self.current_port))
                return result == 0
        except:
            return False
    
    def monitor_server(self):
        """MONITOREO MEJORADO: menos agresivo"""
        consecutive_failures = 0
        max_consecutive_failures = 5  # AUMENTADO: más tolerante
        
        while self.should_monitor and self.restart_count < self.max_restarts:
            try:
                time.sleep(Config.MONITORING_INTERVAL)
                
                # VERIFICACIÓN MÁS ESPECÍFICA: probar endpoint health
                try:
                    import requests
                    response = requests.get(
                        f"http://127.0.0.1:{self.current_port}/health", #f"http://{self.host}:{self.current_port}/health", 
                        timeout=10
                    )
                    if response.status_code == 200:
                        consecutive_failures = 0  # Reset si responde correctamente
                        continue
                    else:
                        consecutive_failures += 1
                        print(f"⚠️ Endpoint health falló: {response.status_code}")
                except Exception as e:
                    consecutive_failures += 1
                    print(f"⚠️ Servidor no responde (fallas consecutivas: {consecutive_failures}): {e}")
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"🔄 Reiniciando servidor automáticamente...")
                    self.restart_server()
                    consecutive_failures = 0
                    
            except Exception as e:
                print(f"❌ Error en monitoreo: {e}")
                consecutive_failures += 1
    
    def restart_server(self):
        """REINICIO MEJORADO: crea nueva app"""
        current_time = time.time()
        
        if current_time - self.last_restart_time < 60:  # AUMENTADO: 60 segundos
            print("⏳ Esperando antes del siguiente reinicio...")
            time.sleep(60)
        
        self.last_restart_time = current_time
        self.restart_count += 1
        
        if self.restart_count >= self.max_restarts:
            print(f"🚨 Máximo número de reinicios alcanzado ({self.max_restarts})")
            self.should_monitor = False
            return
        
        print(f"🔄 Reinicio #{self.restart_count}: Buscando nuevo puerto...")
        
        old_port = self.current_port
        new_port = self.find_available_port()
        
        if new_port == old_port:
            Config.FALLBACK_PORTS = [p for p in Config.FALLBACK_PORTS if p != old_port]
            new_port = self.find_available_port()
        
        print(f"🚀 Reiniciando servidor: {old_port} → {new_port}")
        
        # Notificar cambio de puerto
        self.notify_port_change(new_port, old_port)
        
        # IMPORTANTE: Crear nueva app para el reinicio
        new_app = create_app()
        
        server_thread = threading.Thread(
            target=self.start_server_instance, 
            args=(new_app, new_port),  # Usar nueva app
            daemon=True
        )
        server_thread.start()
        
        time.sleep(10)  # AUMENTADO: más tiempo para inicializar
        
        if self.is_server_running():
            print(f"✅ Servidor reiniciado exitosamente en puerto {new_port}")
        else:
            print(f"❌ Fallo al reiniciar servidor en puerto {new_port}")
    
    def notify_port_change(self, new_port: int, old_port: int):
        try:
            def run_notification():
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(websocket_manager.broadcast_port_change(new_port, old_port))
                    loop.close()
                except Exception as e:
                    print(f"❌ Error en loop de notificación: {e}")
            
            notification_thread = threading.Thread(target=run_notification, daemon=True)
            notification_thread.start()
            
        except Exception as e:
            print(f"❌ Error notificando cambio de puerto: {e}")
    
    def start_server_instance(self, app, port: int):
        try:
            self.current_port = port
            
            config = uvicorn.Config(
                app=app,  # Usar la app pasada como parámetro
                host=self.host,
                port=port,
                reload=False,
                log_level="info",
                access_log=True,
                loop="asyncio",
                http="httptools",
                timeout_keep_alive=Config.REQUEST_TIMEOUT,
                limit_concurrency=50,
                limit_max_requests=10000,
            )
            
            server = uvicorn.Server(config)
            self.current_server = server
            
            print(f"🌐 Servidor disponible en: http://{self.host}:{port}")
            print(f"📚 Documentación en: http://{self.host}:{port}/docs")
            print(f"🔌 WebSocket en: ws://{self.host}:{port}/ws/port-monitor")
            print(f"❤️ Health check en: http://{self.host}:{port}/health")
            
            server.run()
            
        except Exception as e:
            print(f"❌ Error en instancia de servidor (puerto {port}): {e}")
            if Config.AUTO_RESTART and self.should_monitor:
                time.sleep(10)
                self.restart_server()
    
    def start_with_monitoring(self):
        print("🚀 Iniciando servidor con puerto dinámico y monitoreo automático...")
        
        # Crear app inicial
        initial_app = create_app()
        
        initial_port = self.find_available_port(Config.PREFERRED_PORT)
        
        if Config.AUTO_RESTART:
            self.monitoring_thread = threading.Thread(
                target=self.monitor_server,
                daemon=True
            )
            self.monitoring_thread.start()
            print("👁️ Monitoreo automático activado")
        
        self.start_server_instance(initial_app, initial_port)
    
    def stop_monitoring(self):
        self.should_monitor = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=10)

# ========== INSTANCIAS GLOBALES ==========

port_manager = DynamicPortManager()
websocket_manager = WebSocketManager()

# CREAR APP INICIAL (GLOBAL)
app = create_app()

# ========== MANEJO DE SEÑALES ==========

def signal_handler(sig, frame):
    print(f"\n🛑 Recibida señal {sig}, cerrando aplicación...")
    port_manager.stop_monitoring()
    
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        if hasattr(safe_duckdb_service, '_service') and safe_duckdb_service._service:
            safe_duckdb_service._service.close()
    except Exception as e:
        print(f"Error cerrando DuckDB: {e}")
    
    print("✅ Aplicación cerrada correctamente")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)   
signal.signal(signal.SIGTERM, signal_handler) 

if hasattr(signal, 'SIGBREAK'):
    signal.signal(signal.SIGBREAK, signal_handler)

# ========== INICIO DEL SERVIDOR ==========

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 SISTEMA DE PROCESAMIENTO DE ARCHIVOS v2.2.0")
    print("🔄 Puerto Dinámico y Reinicio Automático Activado")
    print("📡 Soporte WebSocket Habilitado")
    print("=" * 60)
    
    try:
        port_manager.start_with_monitoring()
    except KeyboardInterrupt:
        print("\n🛑 Aplicación detenida por el usuario")
    except Exception as e:
        print(f"\n💥 Error crítico: {e}")
        import traceback
        traceback.print_exc()
    finally:
        port_manager.stop_monitoring()
        print("🏁 Aplicación terminada")
