from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Generic, TypeVar
from enum import Enum

T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    data: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_previous: bool

class FileUploadResponse(BaseModel):
    message: str
    file_id: str
    columns: List[str]
    sheets: Optional[List[str]] = None
    total_rows: int

class FileCrossRequest(BaseModel):
    file1_key: str
    file2_key: str
    file1_sheet: Optional[str]
    file2_sheet: Optional[str]
    key_column_file1: str
    key_column_file2: str
    join_type: str = "inner"  # inner, left, right, outer

class FilterOperator(str, Enum):
    EQUALS = "equals"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    GREATER_EQUAL = "gte"
    LESS_EQUAL = "lte"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"

class FilterCondition(BaseModel):
    column: str
    operator: FilterOperator
    value: Optional[Any] = None
    values: Optional[List[Any]] = None

class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"

class SortCondition(BaseModel):
    column: str
    direction: SortDirection = SortDirection.ASC

class DataRequest(BaseModel):
    file_id: str
    sheet_name: Optional[str] = None
    filters: Optional[List[FilterCondition]] = []
    sort: Optional[List[SortCondition]] = []
    page: int = 1
    page_size: int = 100
    search: Optional[str] = None 

class TransformOperation(str, Enum):
    CONCATENATE = "concatenate"
    SPLIT_COLUMN = "split_column"
    REPLACE_VALUES = "replace_values"
    CREATE_CALCULATED = "create_calculated"
    RENAME_COLUMN = "rename_column"
    DELETE_COLUMN = "delete_column"
    FILL_NULL = "fill_null"
    TO_UPPERCASE = "to_uppercase"
    TO_LOWERCASE = "to_lowercase"
    EXTRACT_SUBSTRING = "extract_substring"

class CrossPreviewRequest(BaseModel):
    file1_key: str
    file2_key: str
    file1_sheet: Optional[str] = None
    file2_sheet: Optional[str] = None
    key_column_file1: str
    key_column_file2: str
    cross_type: str = "inner"
    columns_to_include: Optional[Dict[str, List[str]]] = None
    limit: Optional[int] = 50

class TransformRequest(BaseModel):
    file_id: str
    operation: TransformOperation
    params: dict

class AIRequest(BaseModel):
    question: str
    file_context: Optional[str] = None

class ExportFormat(str, Enum):
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"

class ExportRequest(BaseModel):
    file_id: str
    sheet_name: Optional[str] = None
    filters: Optional[List[FilterCondition]] = []
    sort: Optional[List[SortCondition]] = []
    search: Optional[str] = None
    format: ExportFormat = ExportFormat.CSV
    filename: Optional[str] = None
    include_headers: bool = True
    selected_columns: Optional[List[str]] = None  # Exportar solo columnas específicas

class DeleteRowsRequest(BaseModel):
    file_id: str
    sheet_name: Optional[str] = None
    row_indices: List[int]  # Índices de las filas a eliminar
    
class DeleteRowsByFilterRequest(BaseModel):
    file_id: str
    sheet_name: Optional[str] = None
    filters: List[FilterCondition]  # Eliminar filas que cumplan estos filtros

class BulkDeleteRequest(BaseModel):
    file_id: str
    sheet_name: Optional[str] = None
    conditions: List[FilterCondition]
    confirm_delete: bool = False  # Confirmación explícita para operaciones masivas

class ExportResponse(BaseModel):
    message: str
    filename: str
    file_path: str
    rows_exported: int
    format: str

class DeleteResponse(BaseModel):
    message: str
    rows_deleted: int
    remaining_rows: int
