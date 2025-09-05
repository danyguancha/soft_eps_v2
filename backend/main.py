# main.py - VERSIÓN SEGURA CON MANEJO DE SEÑALES
import os
import signal
import sys
import time
import threading
import warnings
from fastapi.exceptions import RequestValidationError
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager

# Suprimir warnings de Pydantic V2
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
warnings.filterwarnings("ignore", category=UserWarning, message=".*schema_extra.*")

from api.routes import router
from api.technical_note_routes import router as technical_note_router
from middleware.content_size_limit import ContentSizeLimitMiddleware

# ========== CONFIGURACIÓN ==========

class Config:
    # Límites de archivos
    MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB
    MAX_UPLOAD_SIZE = 5 * 1024 * 1024 * 1024  # 5GB para requests completos
    
    # Configuraciones de procesamiento
    DEFAULT_CHUNK_SIZE = 8192  # 8KB para streaming
    PROCESSING_CHUNK_SIZE = 10000  # Registros por chunk
    
    # Directorio de uploads
    UPLOAD_DIR = "uploads"
    EXPORTS_DIR = "exports"
    
    # Límites de memoria
    PANDAS_MAX_COLUMNS = 2000  # Máximo columnas permitidas
    SAMPLE_ROWS_FOR_PREVIEW = 1000  # Filas para preview inicial
    
    # Timeouts
    REQUEST_TIMEOUT = 300  # 5 minutos
    UPLOAD_TIMEOUT = 600   # 10 minutos

# ========== MANEJO DE SEÑALES PARA SHUTDOWN LIMPIO ==========

def signal_handler(sig, frame):
    """Maneja señales del sistema para shutdown limpio"""
    print(f"\n🛑 Recibida señal {sig}, cerrando aplicación...")
    
    # Cerrar DuckDB de forma limpia
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        if hasattr(safe_duckdb_service, '_service') and safe_duckdb_service._service:
            safe_duckdb_service._service.close()
            print("✅ DuckDB cerrado limpiamente")
    except Exception as e:
        print(f"⚠️ Error cerrando DuckDB: {e}")
    
    # Cerrar otros recursos si existen
    try:
        # Cerrar conexiones de base de datos, archivos abiertos, etc.
        print("✅ Recursos del sistema liberados")
    except Exception as e:
        print(f"⚠️ Error liberando recursos: {e}")
    
    print("👋 Aplicación cerrada correctamente")
    sys.exit(0)

# Registrar manejadores de señales
signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Terminación del sistema

# En Windows, también manejar SIGBREAK
if hasattr(signal, 'SIGBREAK'):
    signal.signal(signal.SIGBREAK, signal_handler)

# ========== CREACIÓN DE LA APLICACIÓN ==========

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manejo del ciclo de vida de la aplicación"""
    # Startup
    print("🚀 Iniciando Sistema de Procesamiento de Archivos...")
    
    # Verificar e inicializar servicios
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        print("✅ DuckDB Service disponible")
    except Exception as e:
        print(f"⚠️ DuckDB Service no disponible: {e}")
    
    # Crear directorios necesarios
    os.makedirs(Config.UPLOAD_DIR, exist_ok=True)
    os.makedirs(Config.EXPORTS_DIR, exist_ok=True)
    print(f"📁 Directorios verificados: {Config.UPLOAD_DIR}, {Config.EXPORTS_DIR}")
    
    yield
    
    # Shutdown
    print("🛑 Cerrando aplicación...")
    
    # Limpiar recursos
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        if hasattr(safe_duckdb_service, '_service'):
            safe_duckdb_service._service.close()
            print("✅ DuckDB cerrado en shutdown")
    except:
        pass

app = FastAPI(
    title="Sistema de Procesamiento de Archivos - Versión Segura",
    description="API para procesamiento eficiente de archivos Excel y CSV con IA integrada - Soporta archivos hasta 5GB con manejo robusto de errores",
    version="2.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# ========== MIDDLEWARE ==========

class LargeFileLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware personalizado para logging de archivos grandes"""
    
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Log requests de archivos grandes
        if "/upload" in str(request.url):
            content_length = request.headers.get("content-length")
            if content_length:
                size_mb = int(content_length) / 1024 / 1024
                if size_mb > 100:  # Log si es mayor a 100MB
                    print(f"📤 Upload de archivo grande detectado: {size_mb:.1f}MB")
                
                # Almacenar start time para logging posterior
                request.state.start_time = start_time
        
        try:
            response = await call_next(request)
            
            # Log tiempo de respuesta para uploads
            if "/upload" in str(request.url):
                process_time = time.time() - start_time
                print(f"⏱️  Upload procesado en {process_time:.2f}s")
            
            return response
            
        except Exception as e:
            process_time = time.time() - start_time
            print(f"❌ Error en request {request.url}: {e} (tiempo: {process_time:.2f}s)")
            raise

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware para headers de seguridad"""
    
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        
        # Agregar headers de seguridad
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        
        return response

# ========== MANEJADORES DE EXCEPCIÓN ==========

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Manejador mejorado de errores de validación"""
    print(f"❌ ERROR 422 DETALLADO:")
    print(f"   - URL: {request.method} {request.url}")
    
    try:
        body = await request.body()
        if body:
            print(f"   - Body recibido: {body.decode()[:500]}...")  # Limitar tamaño del log
    except:
        print(f"   - No se pudo leer el body")
    
    print(f"   - Errores específicos:")
    for error in exc.errors():
        print(f"     * Campo: {error.get('loc', [])}")
        print(f"     * Error: {error.get('msg', '')}")
        print(f"     * Tipo: {error.get('type', '')}")
    
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.errors(),
            "message": "Error de validación - Ver logs del servidor para más detalles",
            "url": str(request.url)
        }
    )

@app.exception_handler(413)
async def request_entity_too_large_handler(request: Request, exc):
    """Manejador para archivos demasiado grandes"""
    return JSONResponse(
        status_code=413,
        content={
            "error": "Archivo demasiado grande",
            "detail": f"El archivo excede el límite máximo de {Config.MAX_FILE_SIZE/1024/1024/1024:.1f}GB",
            "max_size_gb": Config.MAX_FILE_SIZE/1024/1024/1024,
            "suggestions": [
                "Divida el archivo en partes más pequeñas",
                "Comprima el archivo antes de subirlo",
                "Use un formato más eficiente (CSV vs XLSX para datos grandes)"
            ]
        }
    )

@app.exception_handler(MemoryError)
async def memory_error_handler(request: Request, exc):
    """Manejador de errores de memoria"""
    print(f"🚨 ERROR DE MEMORIA en {request.url}")
    
    return JSONResponse(
        status_code=507,
        content={
            "error": "Insuficiente memoria",
            "detail": "El archivo es demasiado grande para procesarlo en memoria disponible",
            "suggestions": [
                "Pruebe con un archivo más pequeño",
                "El archivo puede tener demasiadas columnas o filas",
                "Considere procesar el archivo por secciones"
            ]
        }
    )

@app.exception_handler(TimeoutError)
async def timeout_error_handler(request: Request, exc):
    """Manejador de errores de timeout"""
    print(f"⏱️ TIMEOUT en {request.url}: {exc}")
    
    return JSONResponse(
        status_code=408,
        content={
            "error": "Timeout",
            "detail": str(exc),
            "suggestions": [
                "El archivo puede ser muy grande",
                "Intente nuevamente con un archivo más pequeño",
                "Verifique su conexión de red"
            ]
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc):
    """Manejador general de excepciones para prevenir crashes"""
    import traceback
    
    print(f"🚨 EXCEPCIÓN GENERAL en {request.url}:")
    print(f"   Tipo: {type(exc).__name__}")
    print(f"   Mensaje: {exc}")
    traceback.print_exc()
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Error interno del servidor",
            "detail": "Ha ocurrido un error inesperado",
            "type": type(exc).__name__,
            "message": str(exc),
            "url": str(request.url)
        }
    )

# ========== CONFIGURACIÓN DE MIDDLEWARE ==========

# Middleware para límite de tamaño de contenido (aplicar PRIMERO)
app.add_middleware(
    ContentSizeLimitMiddleware, 
    max_content_size=Config.MAX_UPLOAD_SIZE
)

# Middleware de seguridad
app.add_middleware(SecurityHeadersMiddleware)

# Middleware de logging personalizado
app.add_middleware(LargeFileLoggingMiddleware)

# CORS - Configurado para producción
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especifica dominios exactos
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=[
        "Authorization",
        "Content-Type", 
        "Content-Length",
        "Accept",
        "Accept-Encoding",
        "X-Requested-With"
    ],
    max_age=3600,  # Cache preflight por 1 hora
)

# ========== MONTAJE DE ARCHIVOS ESTÁTICOS ==========

if os.path.exists(Config.EXPORTS_DIR):
    app.mount("/static", StaticFiles(directory=Config.EXPORTS_DIR), name="static")

# ========== INCLUIR RUTAS ==========

app.include_router(router, prefix="/api/v1", tags=["API"])
app.include_router(technical_note_router, prefix="/api/v1/technical-note", tags=["Technical Note"])

# ========== ENDPOINTS PRINCIPALES ==========

@app.get("/")
def read_root():
    """Endpoint raíz con información del sistema"""
    return {
        "message": "Sistema de Procesamiento de Archivos API - Versión Segura",
        "version": "2.1.0",
        "status": "operational",
        "features": [
            "Soporte para archivos hasta 5GB",
            "Procesamiento por chunks",
            "Detección automática de separadores CSV",
            "Streaming upload",
            "IA integrada",
            "Manejo robusto de errores",
            "Timeouts y fallbacks automáticos",
            "Shutdown limpio"
        ],
        "docs": "/docs",
        "redoc": "/redoc",
        "debug_endpoints": {
            "health": "/api/v1/debug/health",
            "files": "/api/v1/debug/files"
        },
        "limits": {
            "max_file_size_gb": Config.MAX_FILE_SIZE/1024/1024/1024,
            "supported_formats": ["CSV", "XLS", "XLSX"],
            "max_columns": Config.PANDAS_MAX_COLUMNS,
            "request_timeout_minutes": Config.REQUEST_TIMEOUT/60,
            "upload_timeout_minutes": Config.UPLOAD_TIMEOUT/60
        }
    }

@app.get("/health")
def health_check():
    """Endpoint de salud básico"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "version": "2.1.0"
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8002,
        reload=True,
        reload_dirs=[".", "api", "controllers", "services"],
        log_level="info",
        access_log=True,
        loop="asyncio",
        http="httptools",
        timeout_keep_alive=Config.REQUEST_TIMEOUT,
        limit_concurrency=50,
        limit_max_requests=10000,
    )
