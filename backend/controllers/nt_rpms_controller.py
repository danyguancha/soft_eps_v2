"""Controlador para procesar archivos NT RPMS y NT RMPN"""
import os
import time
import duckdb
from typing import Dict, Any

from controllers.duckdb_controller.cache_controller import CacheController
from controllers.duckdb_controller.file_conversion_controller import FileConversionController
from controllers.extract_nt_rpms_controller import (
    extract_nt_rpms_to_csv,
    extract_nt_rmpn_to_csv,
    NT_RPMS_CONFIG,
    NT_RMPN_CONFIG,
    SheetConfig
)
from controllers.technical_note_controller.upload_controller import network_path_controller


class NTRPMSController:
    """
    Controlador para procesamiento NT RPMS y NT RMPN.

    NT_RPMS → CSV + Parquet  (usado activamente en reportes)
    NT_RMPN → Solo CSV       (generado para consulta, sin uso en reportes)
    """

    def __init__(self):
        self.base_output_dir    = "extract_info_nt"
        self.parquet_dir        = "parquet_cache"
        self.metadata_dir       = "metadata_cache"
        self.departamentos_file = "extract_info_nt/departamentos.xlsx"

        for directory in [self.base_output_dir, self.parquet_dir, self.metadata_dir]:
            os.makedirs(directory, exist_ok=True)

        if not os.path.exists(self.departamentos_file):
            print(f"⚠️  ADVERTENCIA: No se encontró {self.departamentos_file}")
            print(f"   Los códigos geográficos no serán enriquecidos")

        self.cache_controller = CacheController(
            parquet_dir=self.parquet_dir,
            metadata_dir=self.metadata_dir
        )
        self.conn = duckdb.connect(database=':memory:')
        self.file_converter = FileConversionController(
            conn=self.conn,
            parquet_dir=self.parquet_dir,
            cache_controller=self.cache_controller
        )

    # ==================== RED ====================

    def process_network_path(self, network_path: str) -> Dict[str, Any]:
        """Procesa carpeta NT RPMS + NT RMPN desde ruta de red compartida."""
        start_time = time.time()
        try:
            print("\n" + "="*60)
            print("PROCESAMIENTO NT RPMS + NT RMPN - CARPETA EN RED")
            print("="*60)

            validation_result = network_path_controller.validate_and_normalize(network_path)
            if not validation_result.get("success"):
                return {
                    "success":    False,
                    "error":      validation_result.get("error"),
                    "suggestion": validation_result.get("suggestion"),
                    "total_time": time.time() - start_time
                }

            normalized_path = validation_result["normalized_path"]
            print(f"Procesando archivos desde: {normalized_path}")

            result = self.process_nt_rpms_folder(normalized_path)

            if result.get("success"):
                result["network_info"] = {
                    "original_path":         network_path,
                    "resolved_path":         normalized_path,
                    "access_type":           validation_result.get("access_type"),
                    "total_files_in_folder": validation_result.get("total_files"),
                    "excel_files_found":     validation_result.get("excel_files")
                }
                print(f"\n{'='*60}")
                print("✓✓✓ PROCESAMIENTO DESDE RED COMPLETADO ✓✓✓")
                print("="*60 + "\n")

            return result

        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                "success":    False,
                "error":      f"Error procesando carpeta de red: {str(e)}",
                "total_time": time.time() - start_time
            }

    # ==================== PROCESAMIENTO PRINCIPAL ====================

    def process_nt_rpms_folder(
        self,
        folder_path: str,
        rpms_config: SheetConfig = NT_RPMS_CONFIG,
        rmpn_config: SheetConfig = NT_RMPN_CONFIG
    ) -> Dict[str, Any]:
        """
        Genera ambos archivos consolidados:
          - NT_RPMS_consolidado.csv + NT_RPMS_consolidado.parquet  ← usado en reportes
          - NT_RMPN_consolidado.csv                                 ← solo CSV
        """
        start_time = time.time()
        try:
            if not os.path.isdir(folder_path):
                return {"success": False, "error": f"La carpeta no existe: {folder_path}"}

            timestamp = int(time.time())

            print("\n" + "="*60)
            print("INICIANDO PROCESAMIENTO NT RPMS + NT RMPN")
            print("="*60)
            print(f"Carpeta origen: {folder_path}")

            # ── [1/2] NT RPMS → CSV + Parquet (usado en reportes) ────
            rpms_result = self._process_rpms(
                folder_path=folder_path,
                config=rpms_config,
                file_id=f"nt_rpms_{timestamp}",
                start_time=start_time
            )

            # ── [2/2] NT RMPN → Solo CSV (generado, no usado en reportes) ──
            rmpn_result = self._process_rmpn_csv_only(
                folder_path=folder_path,
                config=rmpn_config,
                start_time=start_time
            )

            # El éxito global depende solo de RPMS (RMPN es opcional)
            overall_success = rpms_result.get("success", False)

            combined = {
                "success":    overall_success,
                "rpms":       rpms_result,
                "rmpn":       rmpn_result,
                "total_time": time.time() - start_time
            }

            self._print_final_summary(combined)
            return combined

        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                "success":    False,
                "error":      f"Error inesperado: {str(e)}",
                "total_time": time.time() - start_time
            }

    # ── Alias de compatibilidad ───────────────────────────────────────
    def process_nt_folder(self, folder_path: str, **kwargs) -> Dict[str, Any]:
        return self.process_nt_rpms_folder(folder_path)

    # ==================== LÓGICA INTERNA ====================

    def _process_rpms(
        self,
        folder_path: str,
        config: SheetConfig,
        file_id: str,
        start_time: float
    ) -> Dict[str, Any]:
        """NT_RPMS → CSV + Parquet. Mismo flujo que el controlador original que funcionaba."""
        label    = config.output_label
        csv_path = os.path.join(self.base_output_dir, "NT_RPMS_consolidado.csv")

        print(f"\n{'─'*60}")
        print(f"[{label}] Extrayendo datos → CSV + Parquet")
        print(f"  CSV destino : {csv_path}")
        print(f"  Patrón hoja : {config.sheet_pattern}")

        # ── Extracción ────────────────────────────────────────────────
        extraction_start = time.time()
        extraction_result = extract_nt_rpms_to_csv(
            folder_path=folder_path,
            output_csv_path=csv_path,
            separator=';',
            departamentos_file=self.departamentos_file,
            config=config
        )

        if not extraction_result.get("success"):
            return {
                "success": False,
                "label":   label,
                "error":   f"[{label}] Error en extracción de datos",
                "details": extraction_result
            }

        extraction_time = time.time() - extraction_start
        print(f"✓ [{label}] Extracción completada en {extraction_time:.2f}s")

        # ── Conversión a Parquet ──────────────────────────────────────
        print(f"[{label}] Convirtiendo CSV a Parquet...")
        conversion_start = time.time()

        conversion_result = self.file_converter.convert_file_to_parquet(
            file_path=csv_path,
            file_id=file_id,
            original_name="NT_RPMS_consolidado.csv",
            ext='csv'
        )

        if not conversion_result.get("success"):
            return {
                "success":            False,
                "label":              label,
                "error":              f"[{label}] Error en conversión a Parquet",
                "extraction_success": True,
                "csv_path":           csv_path,
                "conversion_details": conversion_result
            }

        conversion_time = time.time() - conversion_start

        return {
            "success":            True,
            "label":              label,
            "csv_path":           csv_path,
            "parquet_path":       conversion_result["parquet_path"],
            "total_rows":         conversion_result["total_rows"],
            "total_columns":      len(conversion_result.get("columns", [])),
            "columns":            conversion_result.get("columns", []),
            "extraction_summary": extraction_result.get("summary", {}),
            "timing": {
                "extraction_time": extraction_time,
                "conversion_time": conversion_time,
            },
            "compression_info": {
                "original_size_mb":  conversion_result.get("original_size_mb", 0),
                "parquet_size_mb":   conversion_result.get("parquet_size_mb", 0),
                "compression_ratio": conversion_result.get("compression_ratio", 0)
            },
            "from_cache": conversion_result.get("from_cache", False),
            "file_hash":  conversion_result.get("file_hash", ""),
            "file_id":    file_id
        }

    def _process_rmpn_csv_only(
        self,
        folder_path: str,
        config: SheetConfig,
        start_time: float
    ) -> Dict[str, Any]:
        """NT_RMPN → Solo CSV. No se convierte a Parquet ni se usa en reportes."""
        label    = config.output_label
        csv_path = os.path.join(self.base_output_dir, "NT_RMPN_consolidado.csv")

        print(f"\n{'─'*60}")
        print(f"[{label}] Extrayendo datos → Solo CSV (no usado en reportes)")
        print(f"  CSV destino : {csv_path}")
        print(f"  Patrón hoja : {config.sheet_pattern}")

        extraction_start = time.time()

        try:
            extraction_result = extract_nt_rmpn_to_csv(
                folder_path=folder_path,
                output_csv_path=csv_path,
                separator=';',
                departamentos_file=self.departamentos_file,
                config=config
            )

            if not extraction_result.get("success"):
                return {
                    "success": False,
                    "label":   label,
                    "error":   f"[{label}] Error en extracción de datos",
                    "details": extraction_result
                }

            extraction_time = time.time() - extraction_start
            print(f"✓ [{label}] CSV generado en {extraction_time:.2f}s (sin conversión a Parquet)")

            return {
                "success":            True,
                "label":              label,
                "csv_path":           csv_path,
                "parquet_path":       None,   # no aplica
                "total_rows":         extraction_result.get("total_rows", 0),
                "total_columns":      extraction_result.get("total_columns", 0),
                "columns":            extraction_result.get("column_order", []),
                "extraction_summary": extraction_result.get("summary", {}),
                "timing": {
                    "extraction_time": extraction_time,
                    "conversion_time": 0.0,
                },
                "compression_info": {},
                "from_cache": False,
                "file_hash":  "",
                "file_id":    ""
            }

        except Exception as e:
            print(f"⚠️  [{label}] Error generando CSV (no crítico): {e}")
            return {
                "success": False,
                "label":   label,
                "error":   str(e)
            }

    # ==================== SUMMARY ====================

    def _print_final_summary(self, result: Dict[str, Any]):
        print("\n" + "="*60)
        print("✓✓✓ PROCESAMIENTO COMPLETADO ✓✓✓")
        print("="*60)

        for key in ("rpms", "rmpn"):
            sheet  = result.get(key, {})
            label  = sheet.get("label", key.upper())
            status = "✓ OK" if sheet.get("success") else "✗ ERROR"
            print(f"\n  [{label}] {status}")

            if sheet.get("success"):
                print(f"    CSV:     {sheet.get('csv_path')}")
                pq = sheet.get("parquet_path")
                print(f"    Parquet: {pq if pq else 'No generado (no requerido para ' + label + ')'}")
                print(f"    Filas:   {sheet.get('total_rows', 0):,}")
                t = sheet.get("timing", {})
                conv = f" | conversión {t['conversion_time']:.2f}s" if t.get("conversion_time") else ""
                print(f"    Tiempo:  extracción {t.get('extraction_time', 0):.2f}s{conv}")
                c = sheet.get("compression_info", {})
                if c.get("original_size_mb"):
                    print(f"    Tamaño:  CSV {c['original_size_mb']:.2f} MB → "
                          f"Parquet {c['parquet_size_mb']:.2f} MB "
                          f"({c['compression_ratio']:.1f}%)")
            else:
                print(f"    Error: {sheet.get('error', 'desconocido')}")

        print(f"\n  Tiempo total: {result.get('total_time', 0):.2f}s")
        print("="*60 + "\n")

    # ==================== UTILIDADES ====================

    def get_processing_status(self, file_hash: str) -> Dict[str, Any]:
        try:
            cached_parquet = self.cache_controller.get_cached_parquet_path(file_hash)
            if not os.path.exists(cached_parquet):
                return {"success": False, "error": f"No se encontró hash: {file_hash}"}
            metadata = self.cache_controller.file_cache.get(file_hash, {})
            return {
                "success":      True,
                "file_hash":    file_hash,
                "parquet_path": cached_parquet,
                "metadata":     metadata
            }
        except Exception as e:
            return {"success": False, "error": f"Error obteniendo estado: {str(e)}"}

    def list_processed_files(self) -> Dict[str, Any]:
        try:
            if not os.path.exists(self.base_output_dir):
                return {
                    "success": True, "files": [], "count": 0,
                    "message": "No se han procesado archivos NT"
                }

            from datetime import datetime
            processed_files = []

            for csv_file in [f for f in os.listdir(self.base_output_dir) if f.endswith('.csv')]:
                csv_path  = os.path.join(self.base_output_dir, csv_file)
                file_stat = os.stat(csv_path)
                parquet_path  = None
                parquet_found = False

                if os.path.exists(self.parquet_dir):
                    for pq_file in os.listdir(self.parquet_dir):
                        if (pq_file.endswith('.parquet') and
                                csv_file.replace('.csv', '') in pq_file):
                            parquet_path  = os.path.join(self.parquet_dir, pq_file)
                            parquet_found = True
                            break

                processed_files.append({
                    "filename":     csv_file,
                    "csv_path":     csv_path,
                    "parquet_path": parquet_path,
                    "has_parquet":  parquet_found,
                    "size_mb":      round(file_stat.st_size / (1024 * 1024), 2),
                    "created":      datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
                    "modified":     datetime.fromtimestamp(file_stat.st_mtime).isoformat()
                })

            return {"success": True, "files": processed_files, "count": len(processed_files)}

        except Exception as e:
            return {"success": False, "error": f"Error listando archivos: {str(e)}"}


# Instancia global
nt_rpms_controller = NTRPMSController()