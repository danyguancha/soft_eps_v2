

from datetime import datetime
import os
import shutil
import duckdb

class InitializeConnection:
    
    def initialize_duckdb_connection(self, duckdb_dir, parquet_dir, metadata_dir, db_path):
        """Inicializa conexión DuckDB con manejo robusto de errores"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                print(f"🔄 Intento {attempt + 1}/{max_attempts} inicializando DuckDB...")
                
                # Intentar conectar
                conn = duckdb.connect(db_path)
                
                # Configurar DuckDB para máximo rendimiento
                conn.execute("PRAGMA threads=4")
                conn.execute("PRAGMA memory_limit='8GB'")
                conn.execute("SET enable_progress_bar=true")
                
                # Test de conexión
                conn.execute("SELECT 1").fetchone()
                
                print(f"✅ DuckDB conectado exitosamente en intento {attempt + 1}")
                return conn
                
            except UnicodeDecodeError as e:
                print(f"❌ Error de encoding en DuckDB (intento {attempt + 1}): {e}")
                self._handle_corrupted_database(attempt, duckdb_dir, parquet_dir, metadata_dir, db_path)
                
            except Exception as e:
                print(f"❌ Error general en DuckDB (intento {attempt + 1}): {e}")
                self._handle_corrupted_database(attempt, duckdb_dir, parquet_dir, metadata_dir, db_path)

        # Si todos los intentos fallan, crear conexión en memoria
        print("⚠️ Usando DuckDB en memoria como fallback")
        try:
            conn = duckdb.connect(":memory:")
            conn.execute("PRAGMA threads=2")
            conn.execute("PRAGMA memory_limit='4GB'")
            return conn
        except Exception as e:
            print(f"❌ Error crítico: No se puede inicializar DuckDB: {e}")
            return None
        
    def _handle_corrupted_database(self, attempt: int, duckdb_dir, parquet_dir, metadata_dir, db_path):
        """Maneja base de datos corrupta"""
        try:
            if attempt == 0:
                # Primer intento: mover base de datos corrupta
                backup_name = f"main_corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
                backup_path = os.path.join(duckdb_dir, backup_name)
                
                if os.path.exists(db_path):
                    shutil.move(db_path, backup_path)
                    print(f"📦 Base de datos corrupta respaldada como: {backup_name}")
                
            elif attempt == 1:
                # Segundo intento: limpiar completamente el directorio
                if os.path.exists(duckdb_dir):
                    shutil.rmtree(duckdb_dir)
                    os.makedirs(duckdb_dir, exist_ok=True)
                    print("🧹 Directorio DuckDB limpiado completamente")
                
            else:
                # Último intento: limpiar todo el cache
                for dir_path in [duckdb_dir, parquet_dir, metadata_dir]:
                    if os.path.exists(dir_path):
                        shutil.rmtree(dir_path)
                        os.makedirs(dir_path, exist_ok=True)
                print("🧹 Todo el cache DuckDB limpiado")
                
        except Exception as cleanup_error:
            print(f"❌ Error limpiando base de datos corrupta: {cleanup_error}")