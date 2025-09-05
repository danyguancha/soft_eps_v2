import { useState, useCallback, useEffect, useMemo } from 'react';
import type { ExcelFilter } from '../types/DataTable.types';

export const useLocalFiltering = (
  filename: string | null, 
  data: Record<string, any>[], 
  excelFilters: Record<string, ExcelFilter>
) => {
  const [localFilteredData, setLocalFilteredData] = useState<Record<string, any>[]>([]);
  const [isLocalFiltering, setIsLocalFiltering] = useState(false);
  const [localCurrentPage, setLocalCurrentPage] = useState(1);
  const [localPageSize, setLocalPageSize] = useState(20);

  const applyLocalFilters = useCallback((dataToFilter: Record<string, any>[], filters: Record<string, ExcelFilter>) => {
    let filteredData = [...dataToFilter];

    for (const [columnName, filter] of Object.entries(filters)) {
      if (filter && filter.selectedValues.length < filter.allValues.length && filter.selectedValues.length > 0) {
        console.log(`🔧 Aplicando filtro local: ${columnName} con ${filter.selectedValues.length} valores`);
        
        filteredData = filteredData.filter(row => {
          const cellValue = String(row[columnName] || '').trim();
          return filter.selectedValues.includes(cellValue);
        });
      }
    }

    console.log(`Filtrado local: ${filteredData.length} registros de ${dataToFilter.length} originales`);
    return filteredData;
  }, []);

  useEffect(() => {
    if (!filename) {
      const hasActiveFilters = Object.values(excelFilters).some(filter => 
        filter && filter.selectedValues.length < filter.allValues.length && filter.selectedValues.length > 0
      );

      if (hasActiveFilters) {
        console.log('Aplicando filtros locales a datos de cruce...');
        const filtered = applyLocalFilters(data, excelFilters);
        setLocalFilteredData(filtered);
        setIsLocalFiltering(true);
        setLocalCurrentPage(1);
      } else {
        console.log('Sin filtros activos, mostrando todos los datos');
        setLocalFilteredData([]);
        setIsLocalFiltering(false);
        setLocalCurrentPage(1);
      }
    }
  }, [filename, data, excelFilters, applyLocalFilters]);

  const displayData = useMemo(() => {
    if (!filename && isLocalFiltering) {
      return localFilteredData;
    }
    return data;
  }, [filename, isLocalFiltering, localFilteredData, data]);

  const paginatedDisplayData = useMemo(() => {
    if (!filename) {
      const startIndex = (localCurrentPage - 1) * localPageSize;
      const endIndex = startIndex + localPageSize;
      return displayData.slice(startIndex, endIndex);
    }
    return displayData;
  }, [filename, displayData, localCurrentPage, localPageSize]);

  const handleLocalPaginationChange = useCallback((page: number, size: number, onPaginationChange: (page: number, pageSize: number) => void) => {
    if (!filename) {
      setLocalCurrentPage(page);
      setLocalPageSize(size);
      console.log(`📄 Paginación local: página ${page}, tamaño ${size}`);
    } else {
      onPaginationChange(page, size);
    }
  }, [filename]);

  const resetLocalFiltering = useCallback(() => {
    setLocalFilteredData([]);
    setIsLocalFiltering(false);
    setLocalCurrentPage(1);
  }, []);

  return {
    localFilteredData,
    isLocalFiltering,
    localCurrentPage,
    localPageSize,
    displayData,
    paginatedDisplayData,
    handleLocalPaginationChange,
    resetLocalFiltering,
    setLocalCurrentPage,
    setLocalPageSize
  };
};
