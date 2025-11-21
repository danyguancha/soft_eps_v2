# main.py
import os
import time
import warnings
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
    # Configuración para producción
    MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB
    UPLOAD_DIR = "uploads"
    REQUEST_TIMEOUT = 300  # 5 minutos
    
    # Puerto FIJO para producción
    PORT = int(os.getenv("PORT", 8000))
    HOST = os.getenv("HOST", "0.0.0.0")
    
    # Ambiente
    ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
    
    # Workers (para Gunicorn/Uvicorn workers)
    WORKERS = int(os.getenv("WORKERS", 2))
    
    # CORS - Ajustar según tu red
    ALLOWED_ORIGINS = os.getenv(
        "ALLOWED_ORIGINS",
        "*"  # En producción, especifica IPs: "http://192.168.1.100,http://nt.local"
    ).split(",")


# ========== MIDDLEWARE ==========
class ProductionMiddleware(BaseHTTPMiddleware):
    """Middleware optimizado para producción"""
    
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Log solo para operaciones importantes
        log_paths = ["/upload", "/process", "/nt-rpms"]
        should_log = any(path in str(request.url) for path in log_paths)
        
        if should_log:
            print(f"📥 {request.method} {request.url.path}")
            
            # Log tamaño si es upload
            if content_length := request.headers.get("content-length"):
                size_mb = int(content_length) / (1024 * 1024)
                if size_mb > 10:
                    print(f"   Tamaño: {size_mb:.1f} MB")
        
        try:
            response = await call_next(request)
            
            # Headers de seguridad
            response.headers.update({
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "SAMEORIGIN",  # Permitir mismo origen
                "X-XSS-Protection": "1; mode=block",
                "Referrer-Policy": "strict-origin-when-cross-origin"
            })
            
            if should_log:
                elapsed = time.time() - start_time
                print(f"✓ Completado en {elapsed:.2f}s (Status: {response.status_code})")
            
            return response
            
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ Error en {request.url.path} ({elapsed:.2f}s): {e}")
            raise


# ========== LIFESPAN ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación"""
    
    # ===== STARTUP =====
    print("=" * 60)
    print("🚀 INICIANDO SISTEMA DE EVALUACIÓN NT")
    print("=" * 60)
    print(f"Ambiente: {Config.ENVIRONMENT}")
    print(f"Puerto: {Config.PORT}")
    print(f"Host: {Config.HOST}")
    print(f"Workers: {Config.WORKERS}")
    print("-" * 60)
    
    # Inicializar servicios
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        print("✓ DuckDB Service inicializado")
    except Exception as e:
        print(f"⚠️  DuckDB no disponible: {e}")
    
    # Crear directorios necesarios
    directories = [
        Config.UPLOAD_DIR,
        "parquet_cache",
        "extract_info_nt",
        "temp_uploads"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✓ Directorio: {directory}")
    
    print("-" * 60)
    print("✅ Sistema listo para recibir peticiones")
    print("=" * 60)
    
    yield
    
    # ===== SHUTDOWN =====
    print("\n" + "=" * 60)
    print("🛑 DETENIENDO SISTEMA")
    print("=" * 60)
    
    # Cerrar conexiones
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        if hasattr(safe_duckdb_service, '_service') and safe_duckdb_service._service:
            safe_duckdb_service._service.close()
            print("✓ Conexión DuckDB cerrada")
    except Exception as e:
        print(f"⚠️  Error cerrando DuckDB: {e}")
    
    print("✓ Sistema detenido correctamente")
    print("=" * 60)


# ========== CREAR APP ==========
app = FastAPI(
    title="Sistema de Evaluación de Nota Técnica",
    description="API para el procesamiento y evaluación de notas técnicas. Basadas en la estructura del software SIGIRES.",
    version="2.4.0",
    docs_url="/docs" if Config.ENVIRONMENT == "development" else None,  # Docs solo en dev
    redoc_url="/redoc" if Config.ENVIRONMENT == "development" else None,
    lifespan=lifespan
)


# ========== MIDDLEWARE ==========
# 1. Límite de tamaño
app.add_middleware(
    ContentSizeLimitMiddleware, 
    max_content_size=Config.MAX_FILE_SIZE
)

# 2. Middleware de producción
app.add_middleware(ProductionMiddleware)

# 3. CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,
)


# ========== HEALTH CHECK ==========
@app.get("/health")
def health_check():
    """Health check endpoint para monitoreo del servidor"""
    return {
        "status": "healthy",
        "service": "nt-rpms-backend",
        "version": "2.4.0",
        "environment": Config.ENVIRONMENT,
        "timestamp": time.time()
    }


@app.get("/")
def root():
    """Endpoint raíz"""
    return {
        "message": "Sistema de Evaluación de Nota Técnica",
        "version": "2.4.0",
        "docs": "/docs" if Config.ENVIRONMENT == "development" else "Deshabilitado en producción",
        "health": "/health"
    }


# ========== ROUTERS ==========
app.include_router(
    router, 
    prefix="/api/v1", 
    tags=["API"]
)

app.include_router(
    technical_note_router, 
    prefix="/api/v1/technical-note", 
    tags=["Technical Note"]
)


# ========== EJECUCIÓN DIRECTA ==========
if __name__ == "__main__":
    """
    Ejecución directa para desarrollo.
    En producción, usar Gunicorn/Uvicorn desde servicio.
    """
    print("\n⚠️  MODO DESARROLLO - No usar en producción")
    print("   Use systemd/NSSM con Gunicorn/Uvicorn\n")
    
    try:
        uvicorn.run(
            "main:app",
            host=Config.HOST,
            port=Config.PORT,
            reload=Config.ENVIRONMENT == "development",
            log_level="info",
            timeout_keep_alive=Config.REQUEST_TIMEOUT,
            access_log=True,
        )
    except KeyboardInterrupt:
        print("\n✋ Aplicación detenida por el usuario")
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()
