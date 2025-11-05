import { useState, useCallback, useMemo } from 'react';
import { message } from 'antd';
import type { TableProps } from 'antd/es/table';
import type { SorterResult } from 'antd/es/table/interface';
import type { SortCondition } from '../types/api.types';
import type { SortState } from '../types/DataTable.types';
import { useExcelFilters } from './useExcelFilters';
import { useLocalFiltering } from './useLocalFiltering';

export const useDataTable = (
    filename: string | null,
    data: Record<string, any>[],
    pagination: any,
    onPaginationChange: (page: number, pageSize: number) => void,
    onFiltersChange: (filters: any[]) => void,
    onSortChange: (sort: SortCondition[]) => void,
    onDeleteRows: (indices: number[]) => void,
    onSearch: (searchTerm: string) => void
) => {
    const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
    const [searchTerm, setSearchTerm] = useState('');
    const [sortState, setSortState] = useState<SortState | null>(null);

    // Hooks de filtrado
    const excelFiltersHook = useExcelFilters(filename, data);
    const localFilteringHook = useLocalFiltering(filename, data, excelFiltersHook.excelFilters);

    const handleTableChange: TableProps<any>['onChange'] = useCallback(
        (_pagination: any, _filters: any, sorter: any[] | SorterResult<any>, _extra: any) => {
            if (sorter && !Array.isArray(sorter)) {
                const singleSorter = sorter as SorterResult<any>;
                if (singleSorter.field && singleSorter.order) {
                    const direction = singleSorter.order === 'ascend' ? 'asc' : 'desc';
                    setSortState({
                        column: singleSorter.field as string,
                        direction
                    });

                    onSortChange([{
                        column: singleSorter.field as string,
                        direction
                    }]);
                }
            } else {
                setSortState(null);
                onSortChange([]);
            }
        },
        [onSortChange]
    );

    const handleDeleteSelected = useCallback(() => {
        const indices = selectedRowKeys.map(key => Number(key));
        onDeleteRows(indices);
        setSelectedRowKeys([]);
    }, [selectedRowKeys, onDeleteRows]);

    const handleGlobalSearch = useCallback(() => {
        onSearch(searchTerm);
        if (searchTerm) {
            message.success(`🔍 Buscando: "${searchTerm}"`);
        }
    }, [searchTerm, onSearch]);


    const finalPagination = useMemo(() => {
        if (!filename) {
            return {
                current: localFilteringHook.localCurrentPage,
                pageSize: localFilteringHook.localPageSize,
                total: localFilteringHook.displayData.length,
                showSizeChanger: true,
                showQuickJumper: true,
                size: 'small' as const
            };
        }
        return pagination;
    }, [filename, localFilteringHook.localCurrentPage, localFilteringHook.localPageSize, localFilteringHook.displayData.length, pagination]);

    const hasActiveFilters = Object.values(excelFiltersHook.excelFilters).some(filter =>
        filter && filter.selectedValues.length < filter.allValues.length
    );

    const hasSelectedRows = selectedRowKeys.length > 0;

    return {
        // Estado
        selectedRowKeys,
        setSelectedRowKeys,
        searchTerm,
        setSearchTerm,
        sortState,
        hasActiveFilters,
        hasSelectedRows,

        // Datos procesados
        displayData: localFilteringHook.displayData,
        paginatedDisplayData: localFilteringHook.paginatedDisplayData,
        finalPagination,

        // Handlers
        handleTableChange,
        handleDeleteSelected,
        handleGlobalSearch,
        handleLocalPaginationChange: (page: number, size: number) =>
            localFilteringHook.handleLocalPaginationChange(page, size, onPaginationChange),

        // Excel Filters (sin duplicar clearAllFilters)
        excelFilters: excelFiltersHook.excelFilters,
        setExcelFilters: excelFiltersHook.setExcelFilters,
        loadUniqueValues: excelFiltersHook.loadUniqueValues,
        applyExcelFilter: excelFiltersHook.applyExcelFilter,
        clearExcelFilter: excelFiltersHook.clearExcelFilter,

        // Local Filtering
        isLocalFiltering: localFilteringHook.isLocalFiltering,

        // Clear all filters (función combinada)
        clearAllFilters: useCallback(() => {
            excelFiltersHook.clearAllFilters(onFiltersChange);
            localFilteringHook.resetLocalFiltering();
        }, [excelFiltersHook, localFilteringHook, onFiltersChange])
    };
};
