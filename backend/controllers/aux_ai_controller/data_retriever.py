# controllers/aux_ai_controller/data_retriever.py
import asyncio
from services.duckdb_service.duckdb_service import duckdb_service


class DataRetriever:
    """Recupera datos usando DuckDB"""
    
    def __init__(self, max_sample_rows: int = 5):
        self.max_sample_rows = max_sample_rows
    
    async def get_sample_data(self, file_id: str) -> str:
        """Obtiene muestra de datos"""
        try:
            loop = asyncio.get_event_loop()
            
            def get_sample():
                query = f"SELECT * FROM '{file_id}' LIMIT {self.max_sample_rows}"
                return duckdb_service.conn.execute(query).fetchdf()
            
            result = await loop.run_in_executor(None, get_sample)
            
            if not result.empty:
                return result.to_markdown(index=False, tablefmt="grid")
            
            return "No hay datos disponibles"
            
        except Exception as e:
            print(f"Error obteniendo muestra: {e}")
            return "Muestra no disponible"


# Instancia global
data_retriever = DataRetriever()
