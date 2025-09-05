import type { FilterCondition, SortCondition } from './api.types';

export interface DataTableProps {
  data: Record<string, any>[];
  columns: string[];
  loading?: boolean;
  filename?: string | null;
  pagination: {
    current: number;
    pageSize: number;
    total: number;
    showSizeChanger: boolean;
    showQuickJumper: boolean;
    size?: 'default' | 'small';
  };
  onPaginationChange: (page: number, pageSize: number) => void;
  onFiltersChange: (filters: FilterCondition[]) => void;
  onSortChange: (sort: SortCondition[]) => void;
  onDeleteRows: (indices: number[]) => void;
  onSearch: (searchTerm: string) => void;
}

export interface ExcelFilter {
  column: string;
  selectedValues: string[];
  allValues: string[];
  searchTerm: string;
  loading: boolean;
}

export interface SortState {
  column: string;
  direction: 'asc' | 'desc';
}
