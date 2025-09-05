// src/hooks/useCrossData.ts - VERSIÓN CORREGIDA SIN PAGINACIÓN INTERNA
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

  // ✅ Función para procesar datos del cruce SIN PAGINACIÓN
  const processCrossData = useCallback(() => {
    if (!crossResult?.data) {
      setProcessedCrossData([]);
      setCrossDataTotal(0);
      return;
    }

    console.log('📊 Procesando datos de cruce:', {
      datosOriginales: crossResult.data.length,
      filtrosActivos: crossTableState.filters.length,
      busquedaActiva: !!crossTableState.searchTerm,
      ordenamientoActivo: crossTableState.sorting.length > 0
    });

    let filteredData = [...crossResult.data];
    const originalLength = filteredData.length;

    // ✅ Aplicar búsqueda global
    if (crossTableState.searchTerm) {
      const searchTerm = crossTableState.searchTerm.toLowerCase();
      filteredData = filteredData.filter(row =>
        Object.values(row).some(value => 
          String(value || '').toLowerCase().includes(searchTerm)
        )
      );
      console.log(`🔍 Después de búsqueda: ${filteredData.length} de ${originalLength}`);
    }

    // ✅ Aplicar filtros por columna
    crossTableState.filters.forEach(filter => {
      if (filter.values && filter.values.length > 0) { // ✅ Usar filter.values para filtros estilo Excel
        const beforeFilter = filteredData.length;
        filteredData = filteredData.filter(row =>
          filter.values!.includes(String(row[filter.column] || ''))
        );
        console.log(`🔧 Filtro ${filter.column}: ${filteredData.length} de ${beforeFilter}`);
      } else if (filter.value && filter.value.length > 0) { // ✅ Mantener compatibilidad con filtros tradicionales
        const beforeFilter = filteredData.length;
        filteredData = filteredData.filter(row =>
          filter.value.includes(String(row[filter.column] || ''))
        );
        console.log(`🔧 Filtro ${filter.column}: ${filteredData.length} de ${beforeFilter}`);
      }
    });

    // ✅ Aplicar ordenamiento
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
      console.log(`📊 Datos ordenados por ${sort.column} (${sort.direction})`);
    }

    // ✅ CAMBIO PRINCIPAL: NO paginar aquí, pasar TODOS los datos procesados
    setCrossDataTotal(filteredData.length);
    setProcessedCrossData(filteredData); // ✅ TODOS los datos filtrados/ordenados

    console.log('✅ Procesamiento completado:', {
      datosFinales: filteredData.length,
      datosOriginales: originalLength,
      porcentajeFiltrado: `${((filteredData.length / originalLength) * 100).toFixed(1)}%`
    });

  }, [crossResult, crossTableState]);

  useEffect(() => {
    processCrossData();
  }, [processCrossData]);

  // ✅ Handlers actualizados (ya no manejan paginación interna)
  const handleCrossPaginationChange = useCallback((page: number, size: number) => {
    // ✅ CAMBIO: Ya no actualiza estado interno, el DataTable maneja su propia paginación
    console.log(`📄 Cambio de paginación solicitado: página ${page}, tamaño ${size} (manejado por DataTable)`);
    setCrossTableState(prev => ({ ...prev, currentPage: page, pageSize: size }));
  }, []);

  const handleCrossFiltersChange = useCallback((filters: FilterCondition[]) => {
    console.log('🔧 Aplicando filtros en hook:', filters);
    setCrossTableState(prev => ({ ...prev, filters, currentPage: 1 }));
  }, []);

  const handleCrossSortChange = useCallback((sorting: SortCondition[]) => {
    console.log('📊 Aplicando ordenamiento en hook:', sorting);
    setCrossTableState(prev => ({ ...prev, sorting, currentPage: 1 }));
  }, []);

  const handleCrossSearch = useCallback((searchTerm: string) => {
    console.log('🔍 Aplicando búsqueda en hook:', searchTerm);
    setCrossTableState(prev => ({ ...prev, searchTerm, currentPage: 1 }));
  }, []);

  const handleCrossComplete = useCallback(async (result: any) => {
    console.log('🎉 Cruce completado:', {
      totalRegistros: result.total_rows,
      columnas: result.columns?.length,
      datosRecibidos: result.data?.length
    });

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
      message: `Se procesaron ${result.total_rows?.toLocaleString()} registros exitosamente con ${result.columns?.length} columnas`,
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
      // ✅ CAMBIO: Exportar datos procesados (filtrados/ordenados), no solo originales
      const exportData = processedCrossData.length > 0 ? processedCrossData : crossResult.data || [];
      
      console.log(`📤 Exportando ${exportData.length} registros en formato ${format}`);

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
          message: `Archivo CSV exportado con ${exportData.length.toLocaleString()} registros`,
          variant: 'success'
        });
      }
      // ✅ TODO: Implementar exportación Excel si es necesaria
    } catch (error) {
      console.error('Error exportando:', error);
      await showAlert({
        title: 'Error de exportación',
        message: 'Error al exportar archivo',
        variant: 'error'
      });
    }
  }, [crossResult, processedCrossData, showAlert]);

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
            console.log('🗑️ Limpiando resultado del cruce');
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
