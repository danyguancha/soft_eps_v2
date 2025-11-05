# main.py
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager

from api.routes import router
from api.technical_note_routes import router as technical_note_router
from middleware.content_size_limit import ContentSizeLimitMiddleware


# Configuración mínima
UPLOAD_DIR = "uploads"
EXPORTS_DIR = "exports"
MAX_UPLOAD_SIZE = 5 * 1024 * 1024 * 1024  # 5GB


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicialización y limpieza de recursos"""
    print("🔧 Inicializando servicios...")
    
    # Crear directorios
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    
    # Inicializar DuckDB si es necesario
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        print("✅ DuckDB Service disponible")
    except Exception as e:
        print(f"⚠️ DuckDB Service no disponible: {e}")
    
    yield
    
    # Limpieza
    print("🧹 Limpiando recursos...")
    try:
        from services.duckdb_service_wrapper import safe_duckdb_service
        if hasattr(safe_duckdb_service, '_service'):
            safe_duckdb_service._service.close()
    except:
        pass


# Crear app
app = FastAPI(
    title="Sistema de Procesamiento de Archivos",
    version="2.0.0",
    lifespan=lifespan
)


# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Middleware de tamaño
app.add_middleware(ContentSizeLimitMiddleware, max_content_size=MAX_UPLOAD_SIZE)


# Archivos estáticos
if os.path.exists(EXPORTS_DIR):
    app.mount("/static", StaticFiles(directory=EXPORTS_DIR), name="static")


# Endpoints básicos
@app.get("/")
def root():
    return {
        "message": "Sistema de Procesamiento de Archivos API",
        "version": "2.0.0",
        "status": "operational"
    }


@app.get("/health")
def health():
    return {"status": "healthy"}


# Incluir routers
app.include_router(router, prefix="/api/v1", tags=["API"])
app.include_router(technical_note_router, prefix="/api/v1/technical-note", tags=["Technical Note"])
