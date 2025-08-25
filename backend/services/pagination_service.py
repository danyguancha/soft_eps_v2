import pandas as pd
import math
from models.schemas import PaginatedResponse

class PaginationService:
    @staticmethod
    def paginate(df: pd.DataFrame, page: int, page_size: int) -> PaginatedResponse:
        """Pagina un DataFrame y devuelve respuesta estructurada"""
        total = len(df)
        total_pages = math.ceil(total / page_size) if total > 0 else 1
        
        # Validar página
        page = max(1, min(page, total_pages))
        
        # Calcular índices
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        # Obtener datos paginados
        paginated_df = df.iloc[start_idx:end_idx]
        
        # Convertir a diccionarios
        data = paginated_df.fillna("").to_dict(orient="records")
        
        return PaginatedResponse(
            data=data,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        )
