from services.csv_service import CSVService
from services.excel_service import ExcelService
from services.cross_service import CrossService
from controllers.file_controller import storage, file_services

def perform_cross(request):
    file1 = storage.get(request.file1_key)
    file2 = storage.get(request.file2_key)
    if not file1 or not file2:
        raise Exception("Archivos no encontrados")

    service1 = file_services[file1["ext"]]
    obj1 = service1.load(file1["path"])
    df1 = service1.get_data(obj1, sheet_name=request.file1_sheet) if file1["ext"] != "csv" else service1.get_data(obj1)

    service2 = file_services[file2["ext"]]
    obj2 = service2.load(file2["path"])
    df2 = service2.get_data(obj2, sheet_name=request.file2_sheet) if file2["ext"] != "csv" else service2.get_data(obj2)

    result = CrossService.cross_files(df1, df2, request.key_column_file1, request.key_column_file2)
    return result
