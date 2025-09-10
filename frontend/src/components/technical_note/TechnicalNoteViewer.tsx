// components/technical-note/TechnicalNoteViewer.tsx - ✅ COMPONENTE PRINCIPAL REFACTORIZADO
import React, { useState, useEffect, useMemo } from 'react';
import { Modal, message } from 'antd';
import { useTechnicalNote } from '../../hooks/useTechnicalNote';
import { useFileUpload } from '../../hooks/useFileUpload';
import { BASE_AGE_GROUPS, convertUploadedFilesToGroups } from '../../config/ageGroups.config';
import type { AgeGroupIcon, CustomUploadedFile } from '../../types/FileTypes';

// Componentes refactorizados
import { HeaderSection } from './HeaderSection';
import { LoadingProgress } from './LoadingProgress';
import { FileGridSection } from './FileGridSection';
import { FileUploadModal } from './FileUploadModal';
import { MainContent } from './MainContent';

const TechnicalNoteViewer: React.FC = () => {
  const [fileSelectionLoading, setFileSelectionLoading] = useState(false);
  const [showUploadModal, setShowUploadModal] = useState(false);

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
    reportItemsCount,
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

  // Combinar grupos etarios base con archivos personalizados
  const allFileGroups = useMemo(() => {
    const customGroups = convertUploadedFilesToGroups(uploadedFiles);
    return [...BASE_AGE_GROUPS, ...customGroups];
  }, [uploadedFiles]);

  // Cargar archivos personalizados al iniciar
  useEffect(() => {
    const loadCustomFiles = async () => {
      try {
        // Aquí podrías hacer una llamada al backend para obtener archivos ya subidos
      } catch (error) {
        console.error('Error cargando archivos personalizados:', error);
      }
    };
    loadCustomFiles();
  }, []);

  // Handler unificado para selección de archivos
  const handleFileGroupClick = async (group: AgeGroupIcon) => {
    if (!group.filename) return;

    try {
      setFileSelectionLoading(true);

      if (group.isCustomFile) {
        console.log(`🔍 Cargando archivo personalizado: ${group.filename}`);
        await loadFileData(group.filename);
        setShowUploadModal(false);
      } else {
        const fileInfo = getFileByDisplayName(group.displayName);
        const filename = fileInfo?.filename || group.filename;
        console.log(`🔍 Cargando archivo precargado: ${filename} para ${group.displayName}`);
        await loadFileData(filename);
      }

      console.log(`✅ Archivo cargado exitosamente: ${group.displayName}`);
    } catch (error) {
      console.error(`❌ Error cargando ${group.displayName}:`, error);
      if (group.isCustomFile) {
        message.error(`Error cargando ${group.displayName}`);
      }
    } finally {
      setFileSelectionLoading(false);
    }
  };

  // Handler para eliminar archivo personalizado
  const handleRemoveUploadedFileWithConfirm = (fileToRemove: CustomUploadedFile) => {
    Modal.confirm({
      title: '¿Eliminar archivo?',
      content: `¿Estás seguro de que quieres eliminar "${fileToRemove.name}"?`,
      onOk: async () => {
        try {
          await handleRemoveUploadedFile(fileToRemove);
          await loadAvailableFiles();
        } catch (error) {
          console.error('Error eliminando archivo:', error);
          message.error('Error eliminando archivo');
        }
      }
    });
  };

  // Handlers de upload con actualización de archivos disponibles
  const handleCustomUploadWithRefresh = async (options: any) => {
    try {
      await handleCustomUpload(options);
      await loadAvailableFiles();
    } catch (error) {
      // Error ya manejado en handleCustomUpload
    }
  };

  // Handlers para compatibilidad del reporte
  const handleRegenerateReport = () => {
    console.log(`🔄 Regenerando reporte con filtros geográficos:`, geographicFilters);
    regenerateReport();
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
        onShowUploadModal={() => setShowUploadModal(true)}
        onLoadAvailableFiles={loadAvailableFiles}
        onResetGeographicFilters={resetGeographicFilters}
      />

      {/* Loading Progress */}
      <LoadingProgress
        isVisible={loadingFiles}
        isLoadingFiles={true}
      />

      {/* File Grid Section */}
      <FileGridSection
        allFileGroups={allFileGroups}
        selectedFile={selectedFile}
        availableFiles={availableFiles}
        uploadedFiles={uploadedFiles}
        fileSelectionLoading={fileSelectionLoading}
        hasGeographicFilters={hasGeographicFilters}
        onFileGroupClick={handleFileGroupClick}
        onShowUploadModal={() => setShowUploadModal(true)}
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
        allFileGroups={allFileGroups}
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

      {/* Main Content */}
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
        reportItemsCount={reportItemsCount}
        reportTotalRecords={reportTotalRecords}
        reportKeywords={reportKeywords}
        reportMinCount={reportMinCount}
        showTemporalData={showTemporalData}
        geographicFilters={geographicFilters}
        departamentosOptions={departamentosOptions}
        municipiosOptions={municipiosOptions}
        ipsOptions={ipsOptions}
        loadingGeoFilters={loadingGeoFilters}
        
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
        onShowUploadModal={() => setShowUploadModal(true)}
      />
    </div>
  );
};

export default TechnicalNoteViewer;
