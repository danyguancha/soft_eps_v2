# main.py
import os
from fastapi.exceptions import RequestValidationError
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager

from api.routes import router
from api.technical_note_routes import router as technical_note_router
from middleware.content_size_limit import ContentSizeLimitMiddleware


# Configuraciones para archivos grandes
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


# Evento de startup para crear directorios
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    os.makedirs(Config.UPLOAD_DIR, exist_ok=True)
    os.makedirs(Config.EXPORTS_DIR, exist_ok=True)
    
    print(f"📁 Directorios creados:")
    print(f"   - Uploads: {Config.UPLOAD_DIR}")
    print(f"   - Exports: {Config.EXPORTS_DIR}")
    print(f"🚀 Límites configurados:")
    print(f"   - Tamaño máximo de archivo: {Config.MAX_FILE_SIZE/1024/1024/1024:.1f}GB")
    print(f"   - Chunk size para streaming: {Config.DEFAULT_CHUNK_SIZE}B")
    
    yield
    
    # Shutdown
    print("📊 Sistema de procesamiento de archivos cerrado")


# Configuración de la aplicación con lifespan events
app = FastAPI(
    title="Sistema de Procesamiento de Archivos",
    description="API para procesamiento eficiente de archivos Excel y CSV con IA integrada - Soporta archivos hasta 5GB",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)


# Middleware personalizado para logging de archivos grandes
class LargeFileLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Log requests de archivos grandes
        if "/upload" in str(request.url):
            content_length = request.headers.get("content-length")
            if content_length:
                size_mb = int(content_length) / 1024 / 1024
                if size_mb > 100:  # Log si es mayor a 100MB
                    print(f"📤 Upload de archivo grande detectado: {size_mb:.1f}MB")
        
        response = await call_next(request)
        
        # Log tiempo de respuesta para uploads
        if "/upload" in str(request.url) and hasattr(request.state, "start_time"):
            process_time = time.time() - request.state.start_time
            print(f"⏱️  Upload procesado en {process_time:.2f}s")
        
        return response
    

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print(f"❌ ERROR 422 DETALLADO:")
    print(f"   - URL: {request.method} {request.url}")
    
    try:
        body = await request.body()
        print(f"   - Body recibido: {body.decode()}")
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
            "message": "Error de validación - Ver logs del servidor"
        }
    )


# Middleware para límite de tamaño de contenido (aplicar PRIMERO)
app.add_middleware(
    ContentSizeLimitMiddleware, 
    max_content_size=Config.MAX_UPLOAD_SIZE
)

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


# Montar archivos estáticos
if os.path.exists(Config.EXPORTS_DIR):
    app.mount("/static", StaticFiles(directory=Config.EXPORTS_DIR), name="static")


# Incluir rutas de la API
app.include_router(router, prefix="/api/v1", tags=["API"])
app.include_router(technical_note_router, prefix="/api/v1/technical-note", tags=["Technical Note"])


# Manejador global de errores para archivos grandes
@app.exception_handler(413)
async def request_entity_too_large_handler(request: Request, exc):
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


# Manejador de errores de memoria
@app.exception_handler(MemoryError)
async def memory_error_handler(request: Request, exc):
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


# Endpoints principales
@app.get("/")
def read_root():
    return {
        "message": "Sistema de Procesamiento de Archivos API",
        "version": "2.0.0",
        "features": [
            "Soporte para archivos hasta 5GB",
            "Procesamiento por chunks",
            "Detección automática de separadores CSV",
            "Streaming upload",
            "IA integrada"
        ],
        "docs": "/docs",
        "redoc": "/redoc",
        "limits": {
            "max_file_size_gb": Config.MAX_FILE_SIZE/1024/1024/1024,
            "supported_formats": ["CSV", "XLS", "XLSX"],
            "max_columns": Config.PANDAS_MAX_COLUMNS
        }
    }


# Configuración para desarrollo
if __name__ == "__main__":
    import time    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[".", "api", "controllers", "services"],
        log_level="info",
        access_log=True,
        # Configuraciones específicas para archivos grandes
        loop="asyncio",
        http="httptools",
        timeout_keep_alive=300,  # 5 minutos para uploads largos
        limit_concurrency=50,
        limit_max_requests=10000,
    )
