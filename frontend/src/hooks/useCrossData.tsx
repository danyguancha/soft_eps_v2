// src/hooks/useCrossData.ts
import { useState, useCallback, useEffect } from 'react';
import { useAlert } from '../components/alerts/AlertProvider';
import type { FilterCondition, SortCondition, CrossTableState, UseCrossDataReturn } from '../types/api.types';

export const useCrossData = (): UseCrossDataReturn => {
  const { showAlert } = useAlert();
  const [crossResult, setCrossResult] = useState<any>(null);
  const [crossTableState, setCrossTableState] = useState<CrossTableState>({
    currentPage: 1,
    pageSize: 20,
    filters: [],
    sorting: [],
    searchTerm: '',
  });
  const [processedCrossData, setProcessedCrossData] = useState<any[]>([]);
  const [crossDataTotal, setCrossDataTotal] = useState(0);

  // ✅ Función para procesar datos del cruce
  const processCrossData = useCallback(() => {
    if (!crossResult?.data) {
      setProcessedCrossData([]);
      setCrossDataTotal(0);
      return;
    }

    let filteredData = [...crossResult.data];

    // Aplicar búsqueda
    if (crossTableState.searchTerm) {
      const searchTerm = crossTableState.searchTerm.toLowerCase();
      filteredData = filteredData.filter(row =>
        Object.values(row).some(value => 
          String(value || '').toLowerCase().includes(searchTerm)
        )
      );
    }

    // Aplicar filtros
    crossTableState.filters.forEach(filter => {
      if (filter.value && filter.value.length > 0) {
        filteredData = filteredData.filter(row =>
          filter.value.includes(String(row[filter.column] || ''))
        );
      }
    });

    // Aplicar ordenamiento
    if (crossTableState.sorting.length > 0) {
      const sort = crossTableState.sorting[0];
      filteredData.sort((a, b) => {
        const aVal = a[sort.column] || '';
        const bVal = b[sort.column] || '';
        
        const aNum = Number(aVal);
        const bNum = Number(bVal);
        
        if (!isNaN(aNum) && !isNaN(bNum)) {
          return sort.direction === 'asc' ? aNum - bNum : bNum - aNum;
        }
        
        const aStr = String(aVal).toLowerCase();
        const bStr = String(bVal).toLowerCase();
        
        if (sort.direction === 'asc') {
          return aStr.localeCompare(bStr);
        } else {
          return bStr.localeCompare(aStr);
        }
      });
    }

    setCrossDataTotal(filteredData.length);

    // Aplicar paginación
    const start = (crossTableState.currentPage - 1) * crossTableState.pageSize;
    const end = start + crossTableState.pageSize;
    const paginatedData = filteredData.slice(start, end);

    setProcessedCrossData(paginatedData);
  }, [crossResult, crossTableState]);

  useEffect(() => {
    processCrossData();
  }, [processCrossData]);

  // ✅ Handlers para la tabla del cruce
  const handleCrossPaginationChange = useCallback((page: number, size: number) => {
    setCrossTableState(prev => ({ ...prev, currentPage: page, pageSize: size }));
  }, []);

  const handleCrossFiltersChange = useCallback((filters: FilterCondition[]) => {
    setCrossTableState(prev => ({ ...prev, filters, currentPage: 1 }));
  }, []);

  const handleCrossSortChange = useCallback((sorting: SortCondition[]) => {
    setCrossTableState(prev => ({ ...prev, sorting, currentPage: 1 }));
  }, []);

  const handleCrossSearch = useCallback((searchTerm: string) => {
    setCrossTableState(prev => ({ ...prev, searchTerm, currentPage: 1 }));
  }, []);

  const handleCrossComplete = useCallback(async (result: any) => {
    setCrossResult(result);
    setCrossTableState({
      currentPage: 1,
      pageSize: 20,
      filters: [],
      sorting: [],
      searchTerm: '',
    });
    
    await showAlert({
      title: '🎉 ¡Cruce Completado!',
      message: `Se procesaron ${result.total_rows?.toLocaleString()} registros exitosamente`,
      variant: 'success'
    });
  }, [showAlert]);

  const handleExportCrossResult = useCallback(async (format: 'csv' | 'xlsx' = 'csv') => {
    if (!crossResult) {
      await showAlert({
        title: 'Sin datos',
        message: 'No hay resultado de cruce para exportar',
        variant: 'warning'
      });
      return;
    }

    try {
      const exportData = crossResult.data || [];

      if (format === 'csv') {
        const headers = crossResult.columns.join(';');
        const rows = exportData.map((row: any) =>
          crossResult.columns.map((col: string) => {
            const value = row[col];
            if (typeof value === 'string' && (value.includes(';') || value.includes('"'))) {
              return `"${value.replace(/"/g, '""')}"`;
            }
            return value || '';
          }).join(';')
        );

        const csvContent = [headers, ...rows].join('\n');
        const BOM = '\uFEFF';
        const blob = new Blob([BOM + csvContent], {
          type: 'text/csv;charset=utf-8-sig;' 
        });

        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `cruce_resultado_${new Date().toISOString().slice(0, 10)}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);

        await showAlert({
          title: '✅ Exportación exitosa',
          message: 'Archivo CSV exportado exitosamente',
          variant: 'success'
        });
      }
    } catch (error) {
      await showAlert({
        title: 'Error de exportación',
        message: 'Error al exportar archivo',
        variant: 'error'
      });
    }
  }, [crossResult, showAlert]);

  const handleClearCrossResult = useCallback(async () => {
    await showAlert({
      title: '🗑️ ¿Limpiar resultado?',
      message: 'Esta acción eliminará el resultado del cruce actual.',
      variant: 'warning',
      actions: [
        {
          label: 'Cancelar',
          type: 'secondary',
          onClick: () => {}
        },
        {
          label: 'Limpiar',
          type: 'primary',
          onClick: () => {
            setCrossResult(null);
            setProcessedCrossData([]);
            setCrossDataTotal(0);
            setCrossTableState({
              currentPage: 1,
              pageSize: 20,
              filters: [],
              sorting: [],
              searchTerm: '',
            });
            showAlert({
              title: 'Limpieza completa',
              message: 'Resultado del cruce eliminado',
              variant: 'info'
            });
          }
        }
      ]
    });
  }, [showAlert]);

  return {
    crossResult,
    crossTableState,
    processedCrossData,
    crossDataTotal,
    handleCrossPaginationChange,
    handleCrossFiltersChange,
    handleCrossSortChange,
    handleCrossSearch,
    handleCrossComplete,
    handleExportCrossResult,
    handleClearCrossResult,
  };
};
