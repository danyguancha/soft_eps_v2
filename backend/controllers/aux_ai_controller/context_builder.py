# controllers/aux_ai_controller/context_builder.py
import asyncio
import os
import json
import glob
from typing import Dict, Any, List, Optional

import aiofiles


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
            print(f"Error construyendo contexto: {e}")
            return "Contexto no disponible."
    
    async def _read_metadata_file(self, metadata_file: str) -> Dict[str, Any]:
        """Lee un archivo de metadatos individual de forma asíncrona"""
        try:
            async with aiofiles.open(metadata_file, 'r', encoding='utf-8') as f:
                content = await f.read()
                metadata = json.loads(content)
            
            return {
                'file_id': metadata.get('file_id', os.path.basename(metadata_file).replace('.json', '')),
                'original_name': metadata.get('original_name', 'Desconocido'),
                'extension': metadata.get('extension', 'csv'),
                'columns': metadata.get('columns', []),
                'total_rows': metadata.get('total_rows', 0),
                'file_size_mb': metadata.get('original_size_mb', 0),
                'cached_at': metadata.get('cached_at', ''),
                'parquet_path': metadata.get('parquet_path', ''),
                'sample_data': metadata.get('sample_data', [])
            }
        except Exception as file_error:
            print(f"Error leyendo {metadata_file}: {file_error}")
            return None


    async def get_available_files(self) -> List[Dict[str, Any]]:
        """Lee todos los archivos de metadatos disponibles de forma asíncrona y concurrente"""
        try:
            if not os.path.exists(self.metadata_cache_path):
                print(f"Carpeta {self.metadata_cache_path} no encontrada")
                return []
            
            metadata_files = glob.glob(os.path.join(self.metadata_cache_path, "*.json"))
    
            tasks = [self._read_metadata_file(metadata_file) for metadata_file in metadata_files]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            available_files = [file_info for file_info in results if file_info is not None]
            
            print(f"Encontrados {len(available_files)} archivos en metadata_cache")
            return available_files
            
        except Exception as e:
            print(f"Error obteniendo archivos: {e}")
            return []
    
    async def _find_file_info_async(self, file_id: str, available_files: List[Dict]) -> Dict:
        """Busca información del archivo por ID o nombre (async)"""
        # Permitir que el event loop procese otras tareas
        await asyncio.sleep(0)
        
        for f in available_files:
            if f['file_id'] == file_id or f['original_name'] == file_id or file_id in f['original_name']:
                return f
        return {}


    def _format_basic_info(self, file_info: Dict) -> List[str]:
        """Formatea información básica del archivo"""
        return [
            f"**Archivo:** {file_info['original_name']}",
            f"**Formato:** {file_info['extension'].upper()}",
            f"**Total de filas:** {file_info['total_rows']:,}",
            f"**Total de columnas:** {len(file_info['columns'])}"
        ]


    def _format_columns(self, columns: List[str]) -> List[str]:
        """Formatea la lista de columnas"""
        if not columns:
            return []
        
        parts = [f"\n**COLUMNAS DISPONIBLES ({len(columns)}):**"]
        for i, col in enumerate(columns, 1):
            parts.append(f"  {i}. {col}")
        return parts


    def _format_sample_data(self, sample_data: List[Dict]) -> List[str]:
        """Formatea muestra de datos como texto"""
        if not sample_data:
            return []
        
        parts = ["\n**MUESTRA DE DATOS (primeras 3 filas):**"]
        sample_subset = sample_data[:3]
        
        if sample_subset and isinstance(sample_subset[0], dict):
            for idx, row in enumerate(sample_subset, 1):
                parts.append(f"\nFila {idx}:")
                for key, value in row.items():
                    parts.append(f"  - {key}: {value}")
        
        return parts


    def _format_footer(self) -> List[str]:
        """Formatea el pie del contexto"""
        return [
            "\n**Estado:** Archivo cargado y listo para análisis",
            "\n**Nota:** Para estadísticas calculadas (promedios, medianas, etc.), usa la sección Análisis de la aplicación"
        ]


    async def build_file_context(self, file_id: str, available_files: List[Dict]) -> str:
        """Construye contexto detallado de un archivo de forma asíncrona"""
        try:
            # Buscar archivo (async para no bloquear)
            file_info = await self._find_file_info_async(file_id, available_files)
            
            if not file_info:
                return f"Archivo '{file_id}' no encontrado."
            
            # Construir contexto por partes
            context_parts = []
            context_parts.extend(self._format_basic_info(file_info))
            context_parts.extend(self._format_columns(file_info.get('columns', [])))
            context_parts.extend(self._format_sample_data(file_info.get('sample_data', [])))
            context_parts.extend(self._format_footer())
            
            return "\n".join(context_parts)
            
        except Exception as e:
            print(f"Error en contexto específico: {e}")
            return "Error obteniendo información del archivo"
    
    def build_general_context(self, available_files: List[Dict]) -> str:
        """Construye contexto general de todos los archivos"""
        try:
            context_parts = []
            context_parts.append(f"**Total de archivos cargados:** {len(available_files)}\n")
            
            # Lista de archivos
            context_parts.append("**ARCHIVOS DISPONIBLES:**")
            for i, file_info in enumerate(available_files, 1):
                context_parts.append(
                    f"{i}. **{file_info['original_name']}** - "
                    f"{len(file_info['columns'])} columnas, {file_info['total_rows']:,} filas"
                )
            
            return "\n".join(context_parts)
            
        except Exception as e:
            return f"Error construyendo contexto general , {e}"
    
    def _get_no_files_message(self) -> str:
        """Mensaje cuando no hay archivos"""
        return """
**No hay archivos cargados actualmente.**

**Para comenzar:**
1. Ve a la sección "Transformar"
2. Carga un archivo CSV o Excel
3. Una vez cargado, podrás hacer consultas sobre tus datos
"""


# Instancia global
context_builder = ContextBuilder()
