// hooks/useTechnicalNote.ts - VERSIÓN COMPLETA CORREGIDA CON VALIDACIÓN ROBUSTA
import { useState, useEffect, useCallback, useRef } from 'react';
import {
  TechnicalNoteService,
  type TechnicalFileInfo,
  type TechnicalFileData,
  type TechnicalFileMetadata,
  type KeywordAgeReport,
  type GeographicFilters,
  type GlobalStatistics
} from '../services/TechnicalNoteService';
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

  // ESTADOS PARA REPORTE CON FILTROS GEOGRÁFICOS
  const [keywordReport, setKeywordReport] = useState<KeywordAgeReport | null>(null);
  const [loadingReport, setLoadingReport] = useState(false);
  const [showReport, setShowReport] = useState(false);
  const [reportKeywords, setReportKeywords] = useState<string[]>(['medicina']);
  const [reportMinCount, setReportMinCount] = useState<number>(0);
  const [showTemporalData, setShowTemporalData] = useState<boolean>(true);

  // ESTADOS PARA FILTROS GEOGRÁFICOS
  const [geographicFilters, setGeographicFilters] = useState<GeographicFilters>({});
  const [departamentosOptions, setDepartamentosOptions] = useState<string[]>([]);
  const [municipiosOptions, setMunicipiosOptions] = useState<string[]>([]);
  const [ipsOptions, setIpsOptions] = useState<string[]>([]);
  const [loadingGeoFilters, setLoadingGeoFilters] = useState({
    departamentos: false,
    municipios: false,
    ips: false
  });

  // Estados para DataTable - DATOS DEL SERVIDOR (no filtros locales)
  const [filteredData, setFilteredData] = useState<Record<string, any>[]>([]);
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 20,
    total: 0,
    showSizeChanger: true,
    showQuickJumper: true,
    size: 'small' as const
  });

  // Estados para filtros/búsqueda/ordenamiento DEL SERVIDOR
  const [serverFilters, setServerFilters] = useState<FilterCondition[]>([]);
  const [serverSearch, setServerSearch] = useState<string>('');
  const [serverSort, setServerSort] = useState<{ column?: string, order?: 'asc' | 'desc' }>({});

  // Estados adicionales para paginación del servidor
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [serverPagination, setServerPagination] = useState<any>(null);

  // FUNCIÓN AUXILIAR PARA OBTENER ESTADÍSTICAS GLOBALES TIPADAS
  const getGlobalStatistics = useCallback((): GlobalStatistics | null => {
    return keywordReport?.global_statistics || null;
  }, [keywordReport]);

  // FUNCIÓN AUXILIAR PARA CÁLCULOS SEGUROS
  const calculateReportTotals = useCallback(() => {
    if (!keywordReport) {
      return {
        totalRecords: 0,
        totalDenominador: 0,
        totalNumerador: 0,
        coberturaGlobal: 0,
        actividades100Pct: 0,
        actividadesMenos50Pct: 0
      };
    }

    const globalStats = getGlobalStatistics();
    const totalRecords = Object.values(keywordReport.totals_by_keyword)
      .reduce((sum, item) => sum + (item.count || 0), 0);

    return {
      totalRecords,
      totalDenominador: globalStats?.total_denominador_global || 0,
      totalNumerador: globalStats?.total_numerador_global || 0,
      coberturaGlobal: globalStats?.cobertura_global_porcentaje || 0,
      actividades100Pct: globalStats?.actividades_100_pct_cobertura || 0,
      actividadesMenos50Pct: globalStats?.actividades_menos_50_pct_cobertura || 0
    };
  }, [keywordReport, getGlobalStatistics]);

  // MÉTODOS PARA FILTROS GEOGRÁFICOS
  const loadDepartamentos = useCallback(async (filename: string) => {
    if (!filename) return;

    try {
      setLoadingGeoFilters(prev => ({ ...prev, departamentos: true }));
      
      const departamentos = await TechnicalNoteService.getDepartamentos(filename);
      setDepartamentosOptions(departamentos);
      
      console.log(`Departamentos cargados: ${departamentos.length}`);
    } catch (error) {
      console.error('Error cargando departamentos:', error);
      setDepartamentosOptions([]);
    } finally {
      setLoadingGeoFilters(prev => ({ ...prev, departamentos: false }));
    }
  }, []);

  const loadMunicipios = useCallback(async (filename: string, departamento: string) => {
    if (!filename || !departamento) return;

    try {
      setLoadingGeoFilters(prev => ({ ...prev, municipios: true }));
      
      const municipios = await TechnicalNoteService.getMunicipios(filename, departamento);
      setMunicipiosOptions(municipios);
      
      console.log(`Municipios cargados para ${departamento}: ${municipios.length}`);
    } catch (error) {
      console.error('Error cargando municipios:', error);
      setMunicipiosOptions([]);
    } finally {
      setLoadingGeoFilters(prev => ({ ...prev, municipios: false }));
    }
  }, []);

  const loadIps = useCallback(async (filename: string, departamento: string, municipio: string) => {
    if (!filename || !departamento || !municipio) return;

    try {
      setLoadingGeoFilters(prev => ({ ...prev, ips: true }));
      
      const ips = await TechnicalNoteService.getIps(filename, departamento, municipio);
      setIpsOptions(ips);
      
      console.log(`IPS cargadas para ${municipio}: ${ips.length}`);
    } catch (error) {
      console.error('Error cargando IPS:', error);
      setIpsOptions([]);
    } finally {
      setLoadingGeoFilters(prev => ({ ...prev, ips: false }));
    }
  }, []);

  // HANDLERS PARA FILTROS GEOGRÁFICOS
  const handleDepartamentoChange = useCallback((departamento: string | null) => {
    setGeographicFilters(() => ({
      departamento: departamento,
      municipio: null,
      ips: null
    }));
    
    setMunicipiosOptions([]);
    setIpsOptions([]);
    
    if (departamento && selectedFile) {
      loadMunicipios(selectedFile, departamento);
    }
  }, [selectedFile, loadMunicipios]);

  const handleMunicipioChange = useCallback((municipio: string | null) => {
    setGeographicFilters(prev => ({
      ...prev,
      municipio: municipio,
      ips: null
    }));
    
    setIpsOptions([]);
    
    if (municipio && geographicFilters.departamento && selectedFile) {
      loadIps(selectedFile, geographicFilters.departamento, municipio);
    }
  }, [selectedFile, geographicFilters.departamento, loadIps]);

  const handleIpsChange = useCallback((ips: string | null) => {
    setGeographicFilters(prev => ({
      ...prev,
      ips: ips
    }));
  }, []);

  const resetGeographicFilters = useCallback(() => {
    setGeographicFilters({});
    setMunicipiosOptions([]);
    setIpsOptions([]);
  }, []);

  // Cargar archivos disponibles
  const loadAvailableFiles = useCallback(async () => {
    if (loadingRef.current) return;

    try {
      loadingRef.current = true;
      setLoadingFiles(true);
      console.log('📁 Cargando lista de archivos técnicos...');

      const files = await TechnicalNoteService.getAvailableFiles();
      setAvailableFiles(files);

      console.log(`${files.length} archivos técnicos disponibles`);

    } catch (error: any) {
      console.error('Error loading available files:', error);
      setAvailableFiles([]);
    } finally {
      setLoadingFiles(false);
      loadingRef.current = false;
    }
  }, []);

  // Cargar metadatos
  const loadFileMetadata = useCallback(async (filename: string) => {
    try {
      setLoadingMetadata(true);
      console.log(`📋 Cargando metadatos: ${filename}`);

      const metadata = await TechnicalNoteService.getFileMetadata(filename);
      setCurrentFileMetadata(metadata);

      console.log(`Metadatos cargados: ${metadata.total_rows.toLocaleString()} filas`);
      return metadata;
    } catch (error: any) {
      console.error('Error loading metadata:', error);
      return null;
    } finally {
      setLoadingMetadata(false);
    }
  }, []);

  // CORREGIDO: Cargar reporte con VALIDACIÓN ROBUSTA
  const loadKeywordAgeReport = useCallback(async (
    filename: string,
    cutoffDate: string, // SEGUNDO PARÁMETRO
    keywords?: string[],
    minCount: number = 0,
    includeTemporal: boolean = true,
    geoFilters: GeographicFilters = {}
  ) => {
    console.log('🔍 loadKeywordAgeReport llamado con:', {
      filename,
      cutoffDate,
      cutoffDateType: typeof cutoffDate,
      cutoffDateTruthy: !!cutoffDate,
      cutoffDateLength: cutoffDate?.length,
      keywords,
      minCount,
      includeTemporal,
      geoFilters
    });

    // VALIDACIÓN ROBUSTA: Verificar que cutoffDate no sea undefined, null, ni string vacío
    if (!cutoffDate || typeof cutoffDate !== 'string' || cutoffDate.trim() === '') {
      const errorMsg = `❌ Fecha de corte inválida: ${JSON.stringify(cutoffDate)} (tipo: ${typeof cutoffDate})`;
      console.error(errorMsg);
      throw new Error('Fecha de corte es obligatoria para generar el reporte');
    }

    // VALIDACIÓN ADICIONAL: Verificar formato básico YYYY-MM-DD
    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateRegex.test(cutoffDate.trim())) {
      const errorMsg = `❌ Formato de fecha inválido: ${cutoffDate}. Debe ser YYYY-MM-DD`;
      console.error(errorMsg);
      throw new Error('Formato de fecha de corte inválido. Use YYYY-MM-DD');
    }

    try {
      setLoadingReport(true);

      console.log('📊 Cargando reporte numerador/denominador con:', {
        filename,
        cutoffDate: cutoffDate.trim(), // Usar fecha limpia
        keywords,
        minCount,
        includeTemporal,
        geoFilters
      });

      // CORRECCIÓN: Orden correcto de parámetros según TechnicalNoteService
      // getKeywordAgeReport(filename, cutoffDate, keywords, minCount, includeTemporal, geoFilters)
      const report = await TechnicalNoteService.getKeywordAgeReport(
        filename,
        cutoffDate.trim(),  // Limpiar espacios antes de enviar
        keywords,
        minCount,
        includeTemporal,
        geoFilters
      );
      
      setKeywordReport(report);
      setShowReport(true);

      const totalItems = report.items.length;
      const globalStats: GlobalStatistics | undefined = report.global_statistics;
      const totalDenominador = globalStats?.total_denominador_global || 0;
      const totalNumerador = globalStats?.total_numerador_global || 0;
      const coberturaGlobal = globalStats?.cobertura_global_porcentaje || 0;

      console.log(`Reporte numerador/denominador cargado:`);
      console.log(`   📊 ${totalItems} actividades`);
      console.log(`   📊 DENOMINADOR: ${totalDenominador.toLocaleString()}`);
      console.log(`   NUMERADOR: ${totalNumerador.toLocaleString()}`);
      console.log(`   📈 COBERTURA: ${coberturaGlobal}%`);
      console.log(`   🗓️ Fecha corte: ${cutoffDate}`);

      return report;
    } catch (error: any) {
      console.error('❌ Error loading keyword age report:', error);
      setKeywordReport(null);
      throw error;
    } finally {
      setLoadingReport(false);
    }
  }, []);

  // MÉTODO PRINCIPAL: Cargar página con filtros del servidor
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

      const data = await TechnicalNoteService.getFileData(
        filename, page, size, sheetName, filters, search, sortBy, sortOrder
      );

      console.log('🔍 Respuesta del servidor:', {
        totalEncontrados: data.pagination?.total_rows,
        totalOriginal: data.pagination?.original_total,
        filtrado: data.pagination?.filtered,
        registrosEnPagina: data.pagination?.rows_in_page
      });

      setCurrentFileData(data);
      setFilteredData([...data.data]);
      setServerPagination(data.pagination);
      setCurrentPage(page);
      setPageSize(size);

      setServerFilters(filters || []);
      setServerSearch(search || '');
      setServerSort({ column: sortBy, order: sortOrder });

      setPagination(prev => ({
        ...prev,
        current: data.pagination.current_page,
        pageSize: data.pagination.page_size,
        total: data.pagination.total_rows
      }));

      console.log(`Datos cargados: ${data.pagination.rows_in_page} registros de ${data.pagination.total_rows} (${data.pagination.filtered ? 'filtrados' : 'todos'})`);

      return data;
    } catch (error: any) {
      console.error('Error loading with server filters:', error);
      return null;
    } finally {
      setLoading(false);
      processingRef.current = false;
    }
  }, [loading]);

  // Cargar primera página
  const loadFileData = useCallback(async (
    filename: string, 
    cutoffDate?: string,
    sheetName?: string
  ) => {
    try {
      setSelectedFile(filename);

      const metadata = await loadFileMetadata(filename);
      if (!metadata) {
        throw new Error('No se pudieron cargar los metadatos');
      }

      await loadDepartamentos(filename);

      const data = await loadFileDataWithServerFilters(filename, 1, pageSize, sheetName);

      return data;
    } catch (error: any) {
      console.error('Error loading file data:', error);

      setCurrentFileData(null);
      setCurrentFileMetadata(null);
      setFilteredData([]);
      setServerPagination(null);
      setDepartamentosOptions([]);
      setMunicipiosOptions([]);
      setIpsOptions([]);
      
      throw error;
    }
  }, [pageSize, loadFileMetadata, loadFileDataWithServerFilters, loadDepartamentos]);

  // Handler para paginación
  const handlePaginationChange = useCallback((page: number, newPageSize: number) => {
    if (!selectedFile || processingRef.current) return;

    console.log(`📄 Cambio paginación: página ${page}, tamaño ${newPageSize}`);

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

  // Handler para filtros
  const handleFiltersChange = useCallback((filters: FilterCondition[]) => {
    if (!selectedFile) return;

    console.log(`🔍 APLICANDO FILTROS DEL SERVIDOR:`, filters);

    loadFileDataWithServerFilters(
      selectedFile,
      1,
      pageSize,
      undefined,
      filters,
      serverSearch,
      serverSort.column,
      serverSort.order
    );
  }, [selectedFile, pageSize, serverSearch, serverSort, loadFileDataWithServerFilters]);

  // Handler para búsqueda
  const handleSearch = useCallback((searchTerm: string) => {
    if (!selectedFile) return;

    console.log(`🔍 BÚSQUEDA DEL SERVIDOR: "${searchTerm}"`);

    loadFileDataWithServerFilters(
      selectedFile,
      1,
      pageSize,
      undefined,
      serverFilters,
      searchTerm,
      serverSort.column,
      serverSort.order
    );
  }, [selectedFile, pageSize, serverFilters, serverSort, loadFileDataWithServerFilters]);

  // Handler para ordenamiento
  const handleSortChange = useCallback((sort: SortCondition[]) => {
    if (!selectedFile) return;

    const sortBy = sort.length > 0 ? sort[0].column : undefined;
    const sortOrder = sort.length > 0 ? sort[0].direction : undefined;

    console.log(`📊 ORDENAMIENTO DEL SERVIDOR: ${sortBy} ${sortOrder}`);

    loadFileDataWithServerFilters(
      selectedFile,
      currentPage,
      pageSize,
      undefined,
      serverFilters,
      serverSearch,
      sortBy,
      sortOrder
    );
  }, [selectedFile, currentPage, pageSize, serverFilters, serverSearch, loadFileDataWithServerFilters]);

  // Handler para eliminar filas
  const handleDeleteRows = useCallback((indices: number[]) => {
    if (!currentFileData) return;

    const newData = filteredData.filter((_, index) => !indices.includes(index));
    setFilteredData(newData);

    setPagination(prev => ({
      ...prev,
      total: prev.total - indices.length
    }));
    console.log(`🗑️ Filas eliminadas localmente: ${indices.length}`);
  }, [filteredData, currentFileData]);

  // HANDLERS PARA REPORTE
  const toggleReportVisibility = useCallback(() => {
    setShowReport(!showReport);
  }, [showReport]);

  const regenerateReport = useCallback((cutoffDate: string) => {
    if (!selectedFile) {
      console.error('❌ No hay archivo seleccionado');
      return;
    }
    
    if (!cutoffDate || cutoffDate.trim() === '') {
      console.error('❌ No hay fecha de corte seleccionada');
      return;
    }
    
    console.log('🔄 Regenerando reporte con:', {
      archivo: selectedFile,
      filtrosGeo: geographicFilters,
      fechaCorte: cutoffDate
    });
    
    return loadKeywordAgeReport(
      selectedFile,
      cutoffDate,
      reportKeywords,
      reportMinCount,
      showTemporalData,
      geographicFilters
    );
  }, [selectedFile, reportKeywords, reportMinCount, showTemporalData, geographicFilters, loadKeywordAgeReport]);

  const handleSetReportKeywords = useCallback((keywords: string[]) => {
    setReportKeywords(keywords);
  }, []);

  const handleAddKeyword = useCallback((keyword: string) => {
    if (!reportKeywords.includes(keyword)) {
      setReportKeywords(prev => [...prev, keyword]);
    }
  }, [reportKeywords]);

  const handleRemoveKeyword = useCallback((keyword: string) => {
    setReportKeywords(prev => prev.filter(k => k !== keyword));
  }, []);

  // WRAPPER ACTUALIZADO CON LOGS
  const handleLoadKeywordAgeReport = useCallback((
    filename: string,
    cutoffDate: string,
    keywords?: string[],
    minCount?: number,
    includeTemporal?: boolean,
    geoFiltersOverride?: GeographicFilters
  ) => {
    const filtersToUse = geoFiltersOverride || geographicFilters;
    
    console.log('🎯 handleLoadKeywordAgeReport llamado:', {
      filename,
      cutoffDate,
      cutoffDateType: typeof cutoffDate,
      cutoffDateValue: cutoffDate,
      keywords,
      minCount,
      includeTemporal,
      geographicFilters: filtersToUse
    });
    
    return loadKeywordAgeReport(
      filename,
      cutoffDate,
      keywords,
      minCount,
      includeTemporal,
      filtersToUse
    );
  }, [geographicFilters, loadKeywordAgeReport]);

  // Limpiar datos
  const clearCurrentData = useCallback(() => {
    console.log('🧹 Limpiando datos...');

    setCurrentFileData(null);
    setCurrentFileMetadata(null);
    setSelectedFile(null);
    setFilteredData([]);
    setServerPagination(null);
    setCurrentPage(1);
    setPageSize(20);

    setServerFilters([]);
    setServerSearch('');
    setServerSort({});

    setKeywordReport(null);
    setShowReport(false);
    setGeographicFilters({});
    setDepartamentosOptions([]);
    setMunicipiosOptions([]);
    setIpsOptions([]);

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
    filteredData,
    pagination,
    activeFilters: serverFilters,
    activeSort: serverSort.column ? [{ column: serverSort.column, direction: serverSort.order || 'asc' }] : [],
    globalSearch: serverSearch,

    // Estados de paginación del servidor
    currentPage,
    pageSize,
    serverPagination,

    // Estados del reporte
    keywordReport,
    loadingReport,
    showReport,
    reportKeywords,
    reportMinCount,
    showTemporalData,

    // Estados de filtros geográficos
    geographicFilters,
    departamentosOptions,
    municipiosOptions,
    ipsOptions,
    loadingGeoFilters,

    // Acciones básicas
    loadFileData,
    loadFileMetadata,
    loadAvailableFiles,
    clearCurrentData,
    getFileByDisplayName,

    // Handlers para DataTable
    handlePaginationChange,
    handleFiltersChange,
    handleSortChange,
    handleDeleteRows,
    handleSearch,

    // Acciones del reporte
    loadKeywordAgeReport: handleLoadKeywordAgeReport,
    toggleReportVisibility,
    regenerateReport,
    onSetReportKeywords: handleSetReportKeywords,
    onSetReportMinCount: setReportMinCount,
    onSetShowTemporalData: setShowTemporalData,
    onAddKeyword: handleAddKeyword,
    onRemoveKeyword: handleRemoveKeyword,

    // Handlers para filtros geográficos
    onDepartamentoChange: handleDepartamentoChange,
    onMunicipioChange: handleMunicipioChange,
    onIpsChange: handleIpsChange,
    resetGeographicFilters,

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

    // Info adicional de filtrado
    isFiltered: serverPagination?.filtered || false,
    originalTotal: serverPagination?.original_total || 0,

    // Info del reporte
    hasReport: !!keywordReport && keywordReport.items.length > 0,
    reportItemsCount: keywordReport?.items?.length || 0,
    reportTotalRecords: calculateReportTotals().totalRecords,

    // Campos de estadísticas globales
    reportGlobalStats: getGlobalStatistics(),
    reportMetodo: keywordReport?.metodo,
    reportVersion: keywordReport?.version,
    reportTotalDenominador: calculateReportTotals().totalDenominador,
    reportTotalNumerador: calculateReportTotals().totalNumerador,
    reportCoberturaGlobal: calculateReportTotals().coberturaGlobal,
    reportActividades100Pct: calculateReportTotals().actividades100Pct,
    reportActividadesMenos50Pct: calculateReportTotals().actividadesMenos50Pct,

    // Info de filtros geográficos
    hasGeographicFilters: !!(geographicFilters.departamento || geographicFilters.municipio || geographicFilters.ips),
    geographicSummary: [
      geographicFilters.departamento && `Dept: ${geographicFilters.departamento}`,
      geographicFilters.municipio && `Mun: ${geographicFilters.municipio}`,
      geographicFilters.ips && `IPS: ${geographicFilters.ips}`
    ].filter(Boolean).join(' → ')
  };
};
