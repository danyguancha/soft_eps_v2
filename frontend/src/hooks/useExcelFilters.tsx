import { useState, useCallback } from 'react';
import { message } from 'antd';
import type { ExcelFilter } from '../types/DataTable.types';
import type { FilterCondition } from '../types/api.types';
import { TechnicalNoteService } from '../services/TechnicalNoteService';

export const useExcelFilters = (filename: string | null | undefined, data: Record<string, any>[]) => {
  const [excelFilters, setExcelFilters] = useState<Record<string, ExcelFilter>>({});

  const loadUniqueValues = useCallback(async (columnName: string) => {
    // CASO 1: Datos de cruce (sin filename)
    if (!filename) {
      console.log(`Obteniendo valores únicos locales para: ${columnName}`);
      
      try {
        setExcelFilters(prev => ({
          ...prev,
          [columnName]: {
            column: columnName,
            selectedValues: [],
            allValues: [],
            searchTerm: '',
            loading: true
          }
        }));

        const uniqueValues = Array.from(new Set(
          data
            .map(row => String(row[columnName] || '').trim())
            .filter(val => val !== '' && val !== 'undefined' && val !== 'null')
        )).sort();

        console.log(`Valores únicos locales encontrados: ${uniqueValues.length} para ${columnName}`);
        
        setExcelFilters(prev => ({
          ...prev,
          [columnName]: {
            column: columnName,
            selectedValues: [...uniqueValues],
            allValues: uniqueValues,
            searchTerm: '',
            loading: false
          }
        }));

      } catch (error) {
        console.error(`❌ Error obteniendo valores únicos locales de ${columnName}:`, error);
        
        setExcelFilters(prev => ({
          ...prev,
          [columnName]: {
            column: columnName,
            selectedValues: [],
            allValues: [],
            searchTerm: '',
            loading: false
          }
        }));
      }
      return;
    }

    // CASO 2: Datos del servidor (con filename)
    if (excelFilters[columnName]?.allValues.length > 0) {
      return;
    }

    try {
      setExcelFilters(prev => ({
        ...prev,
        [columnName]: {
          column: columnName,
          selectedValues: [],
          allValues: [],
          searchTerm: '',
          loading: true
        }
      }));

      console.log(`🔍 Cargando valores únicos del servidor para: ${columnName}`);
      
      const uniqueData = await TechnicalNoteService.getColumnUniqueValues(filename, columnName);
      
      console.log(`Valores únicos del servidor cargados para ${columnName}:`, uniqueData.total_unique);
      
      setExcelFilters(prev => ({
        ...prev,
        [columnName]: {
          column: columnName,
          selectedValues: [...uniqueData.unique_values],
          allValues: uniqueData.unique_values,
          searchTerm: '',
          loading: false
        }
      }));
      
    } catch (error) {
      console.error(`❌ Error cargando valores únicos del servidor de ${columnName}:`, error);
      message.error(`Error cargando valores únicos de ${columnName}`);
      
      setExcelFilters(prev => ({
        ...prev,
        [columnName]: {
          column: columnName,
          selectedValues: [],
          allValues: [],
          searchTerm: '',
          loading: false
        }
      }));
    }
  }, [filename, data, excelFilters]);

  const applyExcelFilter = useCallback((columnName: string, onFiltersChange: (filters: FilterCondition[]) => void) => {
    const filter = excelFilters[columnName];
    if (!filter) return;

    console.log(`🔧 Aplicando filtro Excel para ${columnName}: ${filter.selectedValues.length} de ${filter.allValues.length}`);

    if (!filename) {
      console.log('Filtrado local será aplicado por useEffect');
      message.success(`Filtro local aplicado: ${columnName} (${filter.selectedValues.length} valores)`);
      return;
    }

    const activeFilters: FilterCondition[] = [];
    
    if (filter.selectedValues.length < filter.allValues.length && filter.selectedValues.length > 0) {
      activeFilters.push({
        column: columnName,
        operator: 'in',
        values: filter.selectedValues
      });
    }

    const otherFilters = Object.keys(excelFilters)
      .filter(col => col !== columnName)
      .map(col => {
        const otherFilter = excelFilters[col];
        if (otherFilter && otherFilter.selectedValues.length < otherFilter.allValues.length && otherFilter.selectedValues.length > 0) {
          return {
            column: col,
            operator: 'in' as const,
            values: otherFilter.selectedValues
          };
        }
        return null;
      })
      .filter(Boolean) as FilterCondition[];

    const allFilters = [...activeFilters, ...otherFilters];
    
    onFiltersChange(allFilters);
    message.success(`Filtro del servidor aplicado: ${columnName} (${filter.selectedValues.length} valores)`);
  }, [excelFilters, filename]);

  const clearExcelFilter = useCallback((columnName: string, onFiltersChange: (filters: FilterCondition[]) => void) => {
    const filter = excelFilters[columnName];
    if (!filter) return;

    setExcelFilters(prev => ({
      ...prev,
      [columnName]: {
        ...prev[columnName],
        selectedValues: [...prev[columnName].allValues]
      }
    }));

    setTimeout(() => {
      applyExcelFilter(columnName, onFiltersChange);
    }, 0);
    
    message.success(`Filtro removido de ${columnName}`);
  }, [excelFilters, applyExcelFilter]);

  const clearAllFilters = useCallback((onFiltersChange: (filters: FilterCondition[]) => void) => {
    setExcelFilters({});
    
    if (filename) {
      onFiltersChange([]);
    }
    
    message.success('Todos los filtros removidos');
  }, [filename]);

  return {
    excelFilters,
    setExcelFilters,
    loadUniqueValues,
    applyExcelFilter,
    clearExcelFilter,
    clearAllFilters
  };
};
