// hooks/useTechnicalNote.ts - VERSIÓN CORREGIDA CON FILTROS DEL SERVIDOR
import { useState, useEffect, useCallback, useRef } from 'react';
import { message } from 'antd';
import { TechnicalNoteService, type TechnicalFileInfo, type TechnicalFileData, type TechnicalFileMetadata } from '../services/TechnicalNoteService';
import type { FilterCondition, SortCondition } from '../types/api.types';

export const useTechnicalNote = () => {
  // Refs para evitar loops infinitos
  const loadingRef = useRef(false);
  const processingRef = useRef(false);

  // Estados básicos
  const [availableFiles, setAvailableFiles] = useState<TechnicalFileInfo[]>([]);
  const [currentFileData, setCurrentFileData] = useState<TechnicalFileData | null>(null);
  const [currentFileMetadata, setCurrentFileMetadata] = useState<TechnicalFileMetadata | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingFiles, setLoadingFiles] = useState(false);
  const [loadingMetadata, setLoadingMetadata] = useState(false);
  const [selectedFile, setSelectedFile] = useState<string | null>(null);

  // ✅ Estados para DataTable - DATOS DEL SERVIDOR (no filtros locales)
  const [filteredData, setFilteredData] = useState<Record<string, any>[]>([]);
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 20, // ✅ Empezar con 20 como ejemplo
    total: 0,
    showSizeChanger: true,
    showQuickJumper: true,
    size: 'small' as const
  });

  // ✅ Estados para filtros/búsqueda/ordenamiento DEL SERVIDOR
  const [serverFilters, setServerFilters] = useState<FilterCondition[]>([]);
  const [serverSearch, setServerSearch] = useState<string>('');
  const [serverSort, setServerSort] = useState<{column?: string, order?: 'asc' | 'desc'}>({});

  // Estados adicionales para paginación del servidor
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [serverPagination, setServerPagination] = useState<any>(null);

  // ✅ Cargar archivos disponibles
  const loadAvailableFiles = useCallback(async () => {
    if (loadingRef.current) return;

    try {
      loadingRef.current = true;
      setLoadingFiles(true);
      console.log('📁 Cargando lista de archivos técnicos...');
      
      const files = await TechnicalNoteService.getAvailableFiles();
      setAvailableFiles(files);
      
      console.log(`✅ ${files.length} archivos técnicos disponibles`);
      
      if (files.length > 0) {
        message.success(`📁 ${files.length} archivos técnicos encontrados`);
      } else {
        message.warning('⚠️ No se encontraron archivos técnicos');
      }
      
    } catch (error: any) {
      message.error('❌ Error cargando lista de archivos técnicos');
      console.error('Error loading available files:', error);
      setAvailableFiles([]);
    } finally {
      setLoadingFiles(false);
      loadingRef.current = false;
    }
  }, []);

  // ✅ Cargar metadatos
  const loadFileMetadata = useCallback(async (filename: string) => {
    try {
      setLoadingMetadata(true);
      console.log(`📋 Cargando metadatos: ${filename}`);
      
      const metadata = await TechnicalNoteService.getFileMetadata(filename);
      setCurrentFileMetadata(metadata);
      
      console.log(`✅ Metadatos cargados: ${metadata.total_rows.toLocaleString()} filas`);
      return metadata;
    } catch (error: any) {
      message.error(`❌ Error cargando metadatos de ${filename}`);
      console.error('Error loading metadata:', error);
      return null;
    } finally {
      setLoadingMetadata(false);
    }
  }, []);

  // ✅ MÉTODO PRINCIPAL: Cargar página con filtros del servidor
  const loadFileDataWithServerFilters = useCallback(async (
    filename: string, 
    page: number = 1, 
    size: number = 20,
    sheetName?: string,
    filters?: FilterCondition[],
    search?: string,
    sortBy?: string,
    sortOrder?: 'asc' | 'desc'
  ) => {
    if (loading || processingRef.current) {
      console.log('⚠️ Ya se está cargando, saltando...');
      return null;
    }

    try {
      processingRef.current = true;
      setLoading(true);
      
      console.log(`📖 FILTRADO DEL SERVIDOR: ${filename} - Página ${page}, Filtros: ${filters?.length || 0}, Búsqueda: "${search || 'ninguna'}"`);
      
      // ✅ LLAMADA AL SERVIDOR CON FILTROS
      const data = await TechnicalNoteService.getFileData(
        filename, page, size, sheetName, filters, search, sortBy, sortOrder
      );
      
      console.log('🔍 Respuesta del servidor:', {
        totalEncontrados: data.pagination?.total_rows,
        totalOriginal: data.pagination?.original_total,
        filtrado: data.pagination?.filtered,
        registrosEnPagina: data.pagination?.rows_in_page
      });
      
      // ✅ Establecer datos del servidor
      setCurrentFileData(data);
      setFilteredData([...data.data]); // ✅ Datos YA filtrados del servidor
      setServerPagination(data.pagination);
      setCurrentPage(page);
      setPageSize(size);
      
      // ✅ Actualizar estados de filtros actuales
      setServerFilters(filters || []);
      setServerSearch(search || '');
      setServerSort({ column: sortBy, order: sortOrder });
      
      // ✅ Configurar paginación para DataTable con datos filtrados del servidor
      setPagination(prev => ({
        ...prev,
        current: data.pagination.current_page,
        pageSize: data.pagination.page_size,
        total: data.pagination.total_rows // ✅ Total filtrado del servidor
      }));
      
      console.log(`✅ Datos cargados: ${data.pagination.rows_in_page} registros de ${data.pagination.total_rows} (${data.pagination.filtered ? 'filtrados' : 'todos'})`);
      
      return data;
    } catch (error: any) {
      message.error(`❌ Error: ${error.message}`);
      console.error('Error loading with server filters:', error);
      return null;
    } finally {
      setLoading(false);
      processingRef.current = false;
    }
  }, [loading]);

  // ✅ Cargar primera página de un archivo
  const loadFileData = useCallback(async (filename: string, sheetName?: string) => {
    try {
      setSelectedFile(filename);
      
      // Cargar metadatos primero
      const metadata = await loadFileMetadata(filename);
      if (!metadata) {
        throw new Error('No se pudieron cargar los metadatos');
      }
      
      // ✅ Cargar primera página SIN filtros
      const data = await loadFileDataWithServerFilters(filename, 1, pageSize, sheetName);
      
      if (data) {
        const isLarge = TechnicalNoteService.isLargeFile(metadata.total_rows);
        
        message.success(
          `✅ ${data.display_name} cargado: ${data.pagination.rows_in_page} de ${data.pagination.total_rows.toLocaleString()} registros
          ${isLarge ? ' 📊 Archivo grande' : ''}`,
          3
        );
      }
      
    } catch (error: any) {
      message.error(`❌ Error: ${error.message}`, 5);
      console.error('Error loading file data:', error);
      
      setCurrentFileData(null);
      setCurrentFileMetadata(null);
      setFilteredData([]);
      setServerPagination(null);
    }
  }, [pageSize, loadFileMetadata, loadFileDataWithServerFilters]);

  // ✅ Handler para paginación - CON FILTROS ACTUALES
  const handlePaginationChange = useCallback((page: number, newPageSize: number) => {
    if (!selectedFile || processingRef.current) return;
    
    console.log(`📄 Cambio paginación: página ${page}, tamaño ${newPageSize}`);
    
    // ✅ Mantener filtros actuales al cambiar página
    loadFileDataWithServerFilters(
      selectedFile, 
      page, 
      newPageSize, 
      undefined, 
      serverFilters, 
      serverSearch, 
      serverSort.column, 
      serverSort.order
    );
  }, [selectedFile, serverFilters, serverSearch, serverSort, loadFileDataWithServerFilters]);

  // ✅ Handler para filtros - AHORA DEL SERVIDOR
  const handleFiltersChange = useCallback((filters: FilterCondition[]) => {
    if (!selectedFile) return;
    
    console.log(`🔍 APLICANDO FILTROS DEL SERVIDOR:`, filters);
    console.log(`📋 Filtros a aplicar: ${filters.length} filtros sobre TODOS los registros`);
    
    // ✅ NUEVA LLAMADA AL SERVIDOR con filtros (página 1)
    loadFileDataWithServerFilters(
      selectedFile, 
      1, // ✅ Volver a página 1 cuando se aplican filtros
      pageSize, 
      undefined, 
      filters, // ✅ Filtros del servidor
      serverSearch, 
      serverSort.column, 
      serverSort.order
    );
  }, [selectedFile, pageSize, serverSearch, serverSort, loadFileDataWithServerFilters]);

  // ✅ Handler para búsqueda - AHORA DEL SERVIDOR
  const handleSearch = useCallback((searchTerm: string) => {
    if (!selectedFile) return;
    
    console.log(`🔍 BÚSQUEDA DEL SERVIDOR: "${searchTerm}" sobre TODOS los registros`);
    
    // ✅ NUEVA LLAMADA AL SERVIDOR con búsqueda (página 1)
    loadFileDataWithServerFilters(
      selectedFile, 
      1, // ✅ Volver a página 1 cuando se busca
      pageSize, 
      undefined, 
      serverFilters, 
      searchTerm, // ✅ Búsqueda del servidor
      serverSort.column, 
      serverSort.order
    );
  }, [selectedFile, pageSize, serverFilters, serverSort, loadFileDataWithServerFilters]);

  // ✅ Handler para ordenamiento - AHORA DEL SERVIDOR
  const handleSortChange = useCallback((sort: SortCondition[]) => {
    if (!selectedFile) return;
    
    const sortBy = sort.length > 0 ? sort[0].column : undefined;
    const sortOrder = sort.length > 0 ? sort[0].direction : undefined;
    
    console.log(`📊 ORDENAMIENTO DEL SERVIDOR: ${sortBy} ${sortOrder} sobre TODOS los registros`);
    
    // ✅ NUEVA LLAMADA AL SERVIDOR con ordenamiento (mantener página actual)
    loadFileDataWithServerFilters(
      selectedFile, 
      currentPage, // ✅ Mantener página actual para ordenamiento
      pageSize, 
      undefined, 
      serverFilters, 
      serverSearch, 
      sortBy, 
      sortOrder // ✅ Ordenamiento del servidor
    );
  }, [selectedFile, currentPage, pageSize, serverFilters, serverSearch, loadFileDataWithServerFilters]);

  // ✅ Handler para eliminar filas - SOLO LOCAL (no del servidor)
  const handleDeleteRows = useCallback((indices: number[]) => {
    if (!currentFileData) return;
    
    // Solo eliminar de la vista local, no del servidor
    const newData = filteredData.filter((_, index) => !indices.includes(index));
    setFilteredData(newData);
    
    setPagination(prev => ({
      ...prev,
      total: prev.total - indices.length
    }));
    
    message.success(`🗑️ ${indices.length} fila(s) eliminada(s) de la vista (no del archivo)`);
    console.log(`🗑️ Filas eliminadas localmente: ${indices.length}`);
  }, [filteredData, currentFileData]);

  // ✅ Limpiar datos
  const clearCurrentData = useCallback(() => {
    console.log('🧹 Limpiando datos...');
    
    setCurrentFileData(null);
    setCurrentFileMetadata(null);
    setSelectedFile(null);
    setFilteredData([]);
    setServerPagination(null);
    setCurrentPage(1);
    setPageSize(20);
    
    // ✅ Limpiar filtros del servidor
    setServerFilters([]);
    setServerSearch('');
    setServerSort({});
    
    setPagination(prev => ({
      ...prev,
      current: 1,
      total: 0
    }));
    
    loadingRef.current = false;
    processingRef.current = false;
  }, []);

  const getFileByDisplayName = useCallback((displayName: string) => {
    return availableFiles.find(file => file.display_name === displayName);
  }, [availableFiles]);

  // Cargar archivos al montar
  useEffect(() => {
    loadAvailableFiles();
    
    return () => {
      loadingRef.current = false;
      processingRef.current = false;
    };
  }, [loadAvailableFiles]);

  return {
    // Estados básicos
    availableFiles,
    currentFileData,
    currentFileMetadata,
    loading,
    loadingFiles,
    loadingMetadata,
    selectedFile,
    
    // Estados para DataTable
    filteredData, // ✅ Datos ya filtrados del servidor
    pagination,
    activeFilters: serverFilters, // ✅ Filtros activos del servidor
    activeSort: serverSort.column ? [{ column: serverSort.column, direction: serverSort.order || 'asc' }] : [],
    globalSearch: serverSearch,
    
    // Estados de paginación del servidor
    currentPage,
    pageSize,
    serverPagination,
    
    // Acciones básicas
    loadFileData,
    loadFileMetadata,
    loadAvailableFiles,
    clearCurrentData,
    getFileByDisplayName,
    
    // ✅ Handlers para DataTable - TODOS DEL SERVIDOR
    handlePaginationChange,
    handleFiltersChange, // ✅ Filtros del servidor
    handleSortChange,    // ✅ Ordenamiento del servidor
    handleDeleteRows,
    handleSearch,        // ✅ Búsqueda del servidor
    
    // Helpers
    hasData: !!currentFileData && Array.isArray(currentFileData.data) && currentFileData.data.length > 0,
    hasMetadata: !!currentFileMetadata,
    totalRows: serverPagination?.total_rows || currentFileMetadata?.total_rows || 0,
    totalColumns: currentFileData?.columns?.length || currentFileMetadata?.total_columns || 0,
    columns: currentFileData?.columns || currentFileMetadata?.columns || [],
    displayedRows: Array.isArray(filteredData) ? filteredData.length : 0,
    isLargeFile: currentFileMetadata ? TechnicalNoteService.isLargeFile(currentFileMetadata.total_rows) : false,
    currentPageInfo: serverPagination?.showing || '',
    totalPages: serverPagination?.total_pages || 0,
    
    // ✅ Info adicional de filtrado
    isFiltered: serverPagination?.filtered || false,
    originalTotal: serverPagination?.original_total || 0
  };
};
