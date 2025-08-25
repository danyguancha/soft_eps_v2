
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from api.routes import router
import os

app = FastAPI(
    title="Sistema de Procesamiento de Archivos",
    description="API para procesamiento eficiente de archivos Excel y CSV con IA integrada",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


if not os.path.exists("exports"):
    os.makedirs("exports")

app.mount("/static", StaticFiles(directory="exports"), name="static")
app.include_router(router, prefix="/api/v1")

@app.get("/")
def read_root():
    return {
        "message": "Sistema de Procesamiento de Archivos API",
        "version": "1.0.0",
        "docs": "/docs"
    }
