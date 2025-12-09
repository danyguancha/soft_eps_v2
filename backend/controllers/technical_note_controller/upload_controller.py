# upload_controller.py
"""Controlador para manejar rutas de red y validación de acceso"""
import os
import platform
from typing import Dict, Any, Optional


class NetworkPathController:
    """Controlador para validar y normalizar rutas de red compartidas"""
    
    def __init__(self):
        self.system = platform.system()
    
    def validate_and_normalize(self, network_path: str) -> Dict[str, Any]:
        """
        Valida acceso a ruta de red y la normaliza
        
        Args:
            network_path: Ruta UNC (\\ip\carpeta) o path mapeado (Z:\carpeta)
            
        Returns:
            Dict con success, normalized_path y detalles
        """
        try:
            # Normalizar la ruta
            normalized_path = self._normalize_path(network_path)
            
            print(f"\n{'='*60}")
            print(f"VALIDANDO ACCESO A CARPETA COMPARTIDA")
            print(f"{'='*60}")
            print(f"Ruta original: {network_path}")
            print(f"Ruta normalizada: {normalized_path}")
            
            # Validar que la ruta existe
            if not os.path.exists(normalized_path):
                return {
                    "success": False,
                    "error": f"La ruta no existe o no es accesible: {normalized_path}",
                    "suggestion": "Verifica que:\n" +
                                "1. La carpeta esté compartida en el cliente\n" +
                                "2. El servidor tenga permisos de lectura\n" +
                                "3. El firewall permita compartir archivos\n" +
                                "4. Ambos equipos estén en la misma red"
                }
            
            # Validar que sea un directorio
            if not os.path.isdir(normalized_path):
                return {
                    "success": False,
                    "error": f"La ruta no es un directorio: {normalized_path}"
                }
            
            # Intentar listar contenido
            try:
                files = os.listdir(normalized_path)
                excel_files = [f for f in files if f.lower().endswith(('.xlsx', '.xls'))]
                
                print(f"✓ Acceso exitoso")
                print(f"  Total archivos: {len(files)}")
                print(f"  Archivos Excel: {len(excel_files)}")
                print(f"{'='*60}\n")
                
                return {
                    "success": True,
                    "normalized_path": normalized_path,
                    "total_files": len(files),
                    "excel_files": len(excel_files),
                    "access_type": "network_share" if self._is_network_path(network_path) else "local"
                }
                
            except PermissionError:
                return {
                    "success": False,
                    "error": f"Sin permisos para leer la carpeta: {normalized_path}",
                    "suggestion": "Verifica los permisos de la carpeta compartida"
                }
                
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": f"Error validando ruta: {str(e)}"
            }
    
    def _normalize_path(self, path: str) -> str:
        """Normaliza la ruta según el sistema operativo"""
        if self.system == "Windows":
            # Convertir barras a backslash en Windows
            normalized = path.replace('/', '\\')
            # Eliminar espacios al inicio/final
            normalized = normalized.strip()
            return normalized
        else:
            # En Linux/Mac, convertir backslash a forward slash
            normalized = path.replace('\\', '/')
            return normalized
    
    def _is_network_path(self, path: str) -> bool:
        """Detecta si es una ruta de red"""
        return path.startswith('\\\\') or path.startswith('//')


# Instancia global
network_path_controller = NetworkPathController()
