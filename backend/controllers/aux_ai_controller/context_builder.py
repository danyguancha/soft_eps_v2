# controllers/aux_ai_controller/context_builder.py
import os
import json
import glob
from typing import Dict, Any, List, Optional


class ContextBuilder:
    """Construye contexto para el AI desde metadatos"""
    
    def __init__(self, metadata_cache_path: str = "metadata_cache"):
        self.metadata_cache_path = metadata_cache_path
        self.max_context_length = 8000
    
    async def build_context(self, file_id: Optional[str] = None) -> str:
        """Construye contexto completo"""
        try:
            context_parts = []
            
            # Obtener archivos disponibles
            available_files = await self.get_available_files()
            
            if not available_files:
                return self._get_no_files_message()
            
            if file_id:
                # Contexto específico
                context_parts.append("=== ARCHIVO SELECCIONADO ===")
                file_context = await self.build_file_context(file_id, available_files)
                context_parts.append(file_context)
            else:
                # Contexto general
                context_parts.append("=== ARCHIVOS DISPONIBLES ===")
                general_context = self.build_general_context(available_files)
                context_parts.append(general_context)
            
            full_context = "\n".join(context_parts)
            
            # Limitar longitud
            if len(full_context) > self.max_context_length:
                full_context = full_context[:self.max_context_length] + "...\n[Contexto truncado]"
            
            return full_context
            
        except Exception as e:
            print(f"❌ Error construyendo contexto: {e}")
            return "Contexto no disponible."
    
    async def get_available_files(self) -> List[Dict[str, Any]]:
        """Lee todos los archivos de metadatos disponibles"""
        try:
            available_files = []
            
            if not os.path.exists(self.metadata_cache_path):
                print(f"⚠️ Carpeta {self.metadata_cache_path} no encontrada")
                return []
            
            metadata_files = glob.glob(os.path.join(self.metadata_cache_path, "*.json"))
            
            for metadata_file in metadata_files:
                try:
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    file_info = {
                        'file_id': metadata.get('file_id', os.path.basename(metadata_file).replace('.json', '')),
                        'original_name': metadata.get('original_name', 'Desconocido'),
                        'extension': metadata.get('extension', 'csv'),
                        'columns': metadata.get('columns', []),
                        'total_rows': metadata.get('total_rows', 0),
                        'file_size_mb': metadata.get('original_size_mb', 0),
                        'cached_at': metadata.get('cached_at', ''),
                        'parquet_path': metadata.get('parquet_path', ''),  # ✅ Incluir parquet_path
                        'sample_data': metadata.get('sample_data', [])
                    }
                    
                    available_files.append(file_info)
                    
                except Exception as file_error:
                    print(f"⚠️ Error leyendo {metadata_file}: {file_error}")
                    continue
            
            print(f"📁 Encontrados {len(available_files)} archivos en metadata_cache")
            return available_files
            
        except Exception as e:
            print(f"❌ Error obteniendo archivos: {e}")
            return []
    
    async def build_file_context(self, file_id: str, available_files: List[Dict]) -> str:
        """Construye contexto detallado de un archivo"""
        try:
            # Buscar archivo
            file_info = None
            for f in available_files:
                if f['file_id'] == file_id or f['original_name'] == file_id or file_id in f['original_name']:
                    file_info = f
                    break
            
            if not file_info:
                return f"❌ Archivo '{file_id}' no encontrado."
            
            context_parts = []
            
            # Información básica
            context_parts.append(f"📄 **Archivo:** {file_info['original_name']}")
            context_parts.append(f"📊 **Formato:** {file_info['extension'].upper()}")
            context_parts.append(f"📈 **Total de filas:** {file_info['total_rows']:,}")
            context_parts.append(f"📋 **Total de columnas:** {len(file_info['columns'])}")
            
            # Columnas
            if file_info['columns']:
                context_parts.append(f"\n**COLUMNAS DISPONIBLES ({len(file_info['columns'])}):**")
                for i, col in enumerate(file_info['columns'], 1):
                    context_parts.append(f"  {i}. {col}")
            
            # ✅ AGREGAR MUESTRA DE DATOS SI ESTÁ DISPONIBLE
            if file_info.get('sample_data'):
                context_parts.append(f"\n**MUESTRA DE DATOS (primeras 3 filas):**")
                sample_data = file_info['sample_data'][:3]
                
                # Formatear como tabla simple
                if sample_data and isinstance(sample_data[0], dict):
                    for idx, row in enumerate(sample_data, 1):
                        context_parts.append(f"\nFila {idx}:")
                        for key, value in row.items():
                            context_parts.append(f"  - {key}: {value}")
            
            context_parts.append(f"\n✅ **Estado:** Archivo cargado y listo para análisis")
            context_parts.append(f"\n⚠️ **Nota:** Para estadísticas calculadas (promedios, medianas, etc.), usa la sección Análisis de la aplicación")
            
            return "\n".join(context_parts)
            
        except Exception as e:
            print(f"❌ Error en contexto específico: {e}")
            return f"Error obteniendo información del archivo"
    
    def build_general_context(self, available_files: List[Dict]) -> str:
        """Construye contexto general de todos los archivos"""
        try:
            context_parts = []
            context_parts.append(f"📁 **Total de archivos cargados:** {len(available_files)}\n")
            
            # Lista de archivos
            context_parts.append("**ARCHIVOS DISPONIBLES:**")
            for i, file_info in enumerate(available_files, 1):
                context_parts.append(
                    f"{i}. **{file_info['original_name']}** - "
                    f"{len(file_info['columns'])} columnas, {file_info['total_rows']:,} filas"
                )
            
            return "\n".join(context_parts)
            
        except Exception as e:
            return "Error construyendo contexto general"
    
    def _get_no_files_message(self) -> str:
        """Mensaje cuando no hay archivos"""
        return """
❌ **No hay archivos cargados actualmente.**

📁 **Para comenzar:**
1. Ve a la sección "Transformar"
2. Carga un archivo CSV o Excel
3. Una vez cargado, podrás hacer consultas sobre tus datos
"""


# Instancia global
context_builder = ContextBuilder()
