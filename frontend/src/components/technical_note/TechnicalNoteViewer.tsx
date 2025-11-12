// components/technical-note/TechnicalNoteViewer.tsx
import React, { useState, useEffect, useMemo } from 'react';
import { Modal, message } from 'antd';
import dayjs, { Dayjs } from 'dayjs';
import 'dayjs/locale/es';
import { useTechnicalNote } from '../../hooks/useTechnicalNote';
import { useFileUpload } from '../../hooks/useFileUpload';
import { getVisibleGroups, isPredefinedFile, getPredefinedGroupKey } from '../../config/ageGroups.config';
import type { AgeGroupIcon, CustomUploadedFile } from '../../types/FileTypes';

// Componentes refactorizados
import { HeaderSection } from './HeaderSection';
import { LoadingProgress } from './LoadingProgress';
import { CutoffDateSelector } from './CutoffDateSelector';
import { FileGridSection } from './FileGridSection';
import { FileUploadModal } from './FileUploadModal';
import { MainContent } from './MainContent';

dayjs.locale('es');

const TechnicalNoteViewer: React.FC = () => {
  const [fileSelectionLoading, setFileSelectionLoading] = useState(false);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [cutoffDate, setCutoffDate] = useState<Dayjs | null>(null);

  // CONVERTIR cutoffDate de Dayjs a string YYYY-MM-DD
  const cutoffDateString = useMemo(() => {
    const result = cutoffDate ? cutoffDate.format('YYYY-MM-DD') : undefined;
    console.log('🔍 TechnicalNoteViewer - cutoffDateString calculado:', result);
    return result;
  }, [cutoffDate]);

  // Hook personalizado para gestión de archivos
  const {
    uploadedFiles,
    uploading,
    fileList,
    handleCustomUpload,
    handleBeforeUpload,
    handleUploadChange,
    handleRemoveUploadedFile
  } = useFileUpload();

  // Hook principal de nota técnica
  const {
    availableFiles,
    currentFileMetadata,
    loading,
    loadingFiles,
    selectedFile,
    filteredData,
    pagination,
    currentPage,
    totalPages,
    keywordReport,
    loadingReport,
    showReport,
    hasReport,
    reportTotalRecords,
    reportKeywords,
    reportMinCount,
    showTemporalData,
    geographicFilters,
    departamentosOptions,
    municipiosOptions,
    ipsOptions,
    loadingGeoFilters,
    loadFileData,
    loadAvailableFiles,
    getFileByDisplayName,
    handlePaginationChange,
    handleFiltersChange,
    handleSortChange,
    handleDeleteRows,
    handleSearch,
    loadKeywordAgeReport,
    toggleReportVisibility,
    regenerateReport,
    onSetReportKeywords,
    onSetReportMinCount,
    onSetShowTemporalData,
    onAddKeyword,
    onRemoveKeyword,
    onDepartamentoChange,
    onMunicipioChange,
    onIpsChange,
    resetGeographicFilters,
    hasData,
    columns,
    currentPageInfo,
    hasGeographicFilters,
    geographicSummary,
  } = useTechnicalNote();

  // ✅ CALCULAR GRUPOS VISIBLES DINÁMICAMENTE
  // Esta función ya filtra correctamente para que NO se dupliquen los archivos predefinidos
  const visibleFileGroups = useMemo(() => {
    const groups = getVisibleGroups(availableFiles, uploadedFiles);
    console.log('📊 Grupos visibles calculados:', groups.length);
    console.log('📋 Nombres de grupos:', groups.map(g => `${g.displayName} (${g.filename})`));
    return groups;
  }, [availableFiles, uploadedFiles]);

  // Handler para cambio de fecha de corte
  const handleCutoffDateChange = (date: Dayjs | null) => {
    console.log('📅 handleCutoffDateChange llamado con:', date?.format('YYYY-MM-DD'));
    setCutoffDate(date);
    if (date) {
      console.log(`Fecha de corte seleccionada: ${date.format('DD/MM/YYYY')}`);
      message.success(`Fecha de corte establecida: ${date.format('DD/MM/YYYY')}`);
    } else {
      console.log('⚠️ Fecha de corte eliminada');
      message.warning('Fecha de corte eliminada. Debe seleccionar una fecha para continuar.');
    }
  };

  // DEBUG: Log cuando cambia cutoffDate
  useEffect(() => {
    console.log('🔍 TechnicalNoteViewer - Estado cutoffDate:', {
      cutoffDate: cutoffDate?.format('YYYY-MM-DD'),
      cutoffDateString,
      hasCutoffDate: !!cutoffDate
    });
  }, [cutoffDate, cutoffDateString]);

  // Cargar archivos disponibles al iniciar
  useEffect(() => {
    const loadFiles = async () => {
      try {
        await loadAvailableFiles();
      } catch (error) {
        console.error('Error cargando archivos disponibles:', error);
      }
    };
    loadFiles();
  }, [loadAvailableFiles]);

  // Handler unificado para selección de archivos
  const handleFileGroupClick = async (group: AgeGroupIcon) => {
    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte antes de cargar archivos');
      return;
    }

    if (!group.filename) return;

    try {
      setFileSelectionLoading(true);

      console.log(`🔍 Cargando archivo: ${group.filename}`);
      console.log(`📅 Con fecha de corte: ${cutoffDate.format('YYYY-MM-DD')}`);
      
      await loadFileData(group.filename, cutoffDate.format('YYYY-MM-DD'));
      
      if (showUploadModal) {
        setShowUploadModal(false);
      }

      console.log(`✅ Archivo cargado exitosamente: ${group.displayName}`);
    } catch (error) {
      console.error(`❌ Error cargando ${group.displayName}:`, error);
      message.error(`Error cargando ${group.displayName}`);
    } finally {
      setFileSelectionLoading(false);
    }
  };

  // Handler para eliminar archivo personalizado
  const handleRemoveUploadedFileWithConfirm = (fileToRemove: CustomUploadedFile) => {
    // No permitir eliminar archivos predefinidos
    if (isPredefinedFile(fileToRemove.filename)) {
      message.warning('No se pueden eliminar archivos del sistema predefinidos');
      return;
    }

    Modal.confirm({
      title: '¿Eliminar archivo?',
      content: `¿Estás seguro de que quieres eliminar "${fileToRemove.name}"?`,
      onOk: async () => {
        try {
          await handleRemoveUploadedFile(fileToRemove);
          await loadAvailableFiles();
          message.success('Archivo eliminado correctamente');
        } catch (error) {
          console.error('Error eliminando archivo:', error);
          message.error('Error eliminando archivo');
        }
      }
    });
  };

  // Handlers de upload con actualización de archivos disponibles
  const handleCustomUploadWithRefresh = async (options: any) => {
    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte antes de cargar archivos');
      return;
    }

    try {
      await handleCustomUpload(options);
      await loadAvailableFiles();
      
      // Verificar si el archivo subido es predefinido
      const uploadedFilename = options.file.name;
      if (isPredefinedFile(uploadedFilename)) {
        const groupKey = getPredefinedGroupKey(uploadedFilename);
        message.success(`Archivo "${uploadedFilename}" asociado al grupo: ${groupKey}`, 3);
      } else {
        message.success(`Archivo "${uploadedFilename}" cargado como personalizado`, 2);
      }
    } catch (error) {
      // Error ya manejado en handleCustomUpload
    }
  };

  // Manejar apertura de modal de upload
  const handleShowUploadModal = () => {
    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte antes de cargar archivos');
      return;
    }
    setShowUploadModal(true);
  };

  // Wrapper para regenerar reporte con cutoffDate
  const handleRegenerateReport = () => {
    if (!cutoffDateString) {
      message.error('Debe seleccionar una fecha de corte antes de regenerar el reporte');
      return;
    }
    console.log(`🔄 Regenerando reporte con filtros geográficos:`, geographicFilters);
    console.log(`📅 Con fecha de corte: ${cutoffDateString}`);
    regenerateReport(cutoffDateString);
  };

  const handleAddKeyword = (value: string) => {
    console.log(`➕ Agregando palabra clave: ${value}`);
    onAddKeyword(value);
  };

  const handleRemoveKeyword = (keyword: string) => {
    console.log(`➖ Removiendo palabra clave: ${keyword}`);
    onRemoveKeyword(keyword);
  };

  const handleSetReportKeywords = (keywords: string[]) => {
    console.log(`🔧 Estableciendo nuevas palabras clave: ${keywords}`);
    onSetReportKeywords(keywords);
  };

  return (
    <div style={{ padding: '24px' }}>
      {/* Header Section */}
      <HeaderSection
        hasGeographicFilters={hasGeographicFilters}
        geographicSummary={geographicSummary}
        loadingFiles={loadingFiles}
        onShowUploadModal={handleShowUploadModal}
        onLoadAvailableFiles={loadAvailableFiles}
        onResetGeographicFilters={resetGeographicFilters}
      />

      {/* Selector de Fecha de Corte */}
      <CutoffDateSelector
        selectedDate={cutoffDate}
        onDateChange={handleCutoffDateChange}
      />

      {/* Loading Progress */}
      <LoadingProgress
        isVisible={loadingFiles}
        isLoadingFiles={true}
      />

      {/* File Grid Section - USANDO GRUPOS VISIBLES FILTRADOS */}
      <FileGridSection
        allFileGroups={visibleFileGroups}
        selectedFile={selectedFile}
        availableFiles={availableFiles}
        uploadedFiles={uploadedFiles}
        fileSelectionLoading={fileSelectionLoading}
        hasGeographicFilters={hasGeographicFilters}
        cutoffDateSelected={!!cutoffDate}
        onFileGroupClick={handleFileGroupClick}
        onShowUploadModal={handleShowUploadModal}
        onRemoveUploadedFile={handleRemoveUploadedFileWithConfirm}
        getFileByDisplayName={getFileByDisplayName}
      />

      {/* File Upload Modal */}
      <FileUploadModal
        visible={showUploadModal}
        uploading={uploading}
        uploadedFiles={uploadedFiles}
        fileList={fileList}
        fileSelectionLoading={fileSelectionLoading}
        allFileGroups={visibleFileGroups}
        onCancel={() => setShowUploadModal(false)}
        onCustomUpload={handleCustomUploadWithRefresh}
        onBeforeUpload={handleBeforeUpload}
        onUploadChange={handleUploadChange}
        onRemoveUploadedFile={handleRemoveUploadedFileWithConfirm}
        onFileGroupClick={handleFileGroupClick}
      />

      {/* Loading Progress for Data */}
      <LoadingProgress
        isVisible={loading && !!currentFileMetadata}
        currentPage={currentPage}
        totalPages={totalPages}
        hasGeographicFilters={hasGeographicFilters}
        geographicSummary={geographicSummary}
        isLoadingFiles={false}
      />

      {/* Main Content CON cutoffDate */}
      <MainContent
        loading={loading}
        hasData={hasData}
        availableFiles={availableFiles}
        uploadedFiles={uploadedFiles}
        loadingFiles={loadingFiles}
        currentPageInfo={currentPageInfo}
        hasGeographicFilters={hasGeographicFilters}
        geographicSummary={geographicSummary}
        
        // DataTable props
        filteredData={filteredData}
        columns={columns}
        selectedFile={selectedFile}
        pagination={pagination}
        
        // Report props
        keywordReport={keywordReport}
        loadingReport={loadingReport}
        showReport={showReport}
        hasReport={hasReport}
        reportTotalRecords={reportTotalRecords ?? 0}
        reportKeywords={reportKeywords}
        reportMinCount={reportMinCount}
        showTemporalData={showTemporalData}
        geographicFilters={geographicFilters}
        departamentosOptions={departamentosOptions}
        municipiosOptions={municipiosOptions}
        ipsOptions={ipsOptions}
        loadingGeoFilters={loadingGeoFilters}
        cutoffDate={cutoffDateString}
        
        // Event handlers
        onPaginationChange={handlePaginationChange}
        onFiltersChange={handleFiltersChange}
        onSortChange={handleSortChange}
        onDeleteRows={handleDeleteRows}
        onSearch={handleSearch}
        onToggleReportVisibility={toggleReportVisibility}
        onRegenerateReport={handleRegenerateReport}
        onSetReportKeywords={handleSetReportKeywords}
        onSetReportMinCount={onSetReportMinCount}
        onSetShowTemporalData={onSetShowTemporalData}
        onLoadKeywordAgeReport={loadKeywordAgeReport}
        onAddKeyword={handleAddKeyword}
        onRemoveKeyword={handleRemoveKeyword}
        onDepartamentoChange={onDepartamentoChange}
        onMunicipioChange={onMunicipioChange}
        onIpsChange={onIpsChange}
        resetGeographicFilters={resetGeographicFilters}
        onShowUploadModal={handleShowUploadModal}
      />
    </div>
  );
};

export default TechnicalNoteViewer;
