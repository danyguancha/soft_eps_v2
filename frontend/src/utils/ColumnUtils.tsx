
import type { ExcelFilter } from '../types/DataTable.types';

export const calculateColumnWidth = (columnName: string): number => {
  let columnWidth = 120;
  const textLength = columnName.length;
  
  if (textLength <= 5) columnWidth = 90;
  else if (textLength <= 10) columnWidth = 130;
  else if (textLength <= 15) columnWidth = 160;
  else if (textLength <= 20) columnWidth = 190;
  else columnWidth = 220;

  if (columnName.toLowerCase().includes('id')) columnWidth = 110;
  if (columnName.toLowerCase().includes('nombre')) columnWidth = 170;
  if (columnName.toLowerCase().includes('apellido')) columnWidth = 170;
  if (columnName.toLowerCase().includes('documento')) columnWidth = 150;
  if (columnName.toLowerCase().includes('municipio')) columnWidth = 150;
  if (columnName.toLowerCase().includes('gestor')) columnWidth = 210;
  if (columnName.toLowerCase().includes('medicamento')) columnWidth = 190;
  if (columnName.toLowerCase().includes('diagnóstico')) columnWidth = 190;
  
  return columnWidth;
};

export const isColumnFiltered = (columnName: string, excelFilters: Record<string, ExcelFilter>): boolean => {
  const currentFilter = excelFilters[columnName];
  return currentFilter && currentFilter.selectedValues.length < currentFilter.allValues.length;
};
