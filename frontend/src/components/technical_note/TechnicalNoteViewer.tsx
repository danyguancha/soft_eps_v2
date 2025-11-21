// components/technical-note/TechnicalNoteViewer.tsx
import React, { useState, useEffect, useMemo } from 'react';
import { Modal, message, Divider } from 'antd';
import dayjs, { Dayjs } from 'dayjs';
import 'dayjs/locale/es';
import { useTechnicalNote } from '../../hooks/useTechnicalNote';
import { useFileUpload } from '../../hooks/useFileUpload';
import { getVisibleGroups, isPredefinedFile, getPredefinedGroupKey } from '../../config/ageGroups.config';
import { TechnicalNoteService } from '../../services/TechnicalNoteService';
import type { AgeGroupIcon, CustomUploadedFile } from '../../types/FileTypes';

// Componentes refactorizados
import { HeaderSection } from './HeaderSection';
import { LoadingProgress } from './LoadingProgress';
import { FolderPathSelector } from './FolderPathSelector';
import { CutoffDateSelector } from './CutoffDateSelector';
import { FileGridSection } from './FileGridSection';
import { FileUploadModal } from './FileUploadModal';
import { MainContent } from './MainContent';

dayjs.locale('es');

const TechnicalNoteViewer: React.FC = () => {
  const [fileSelectionLoading, setFileSelectionLoading] = useState(false);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [cutoffDate, setCutoffDate] = useState<Dayjs | null>(null);

  // Estados para NT RPMS (INDEPENDIENTES de cutoffDate)
  const [folderPath, setFolderPath] = useState<string>('');
  const [processingNTRPMS, setProcessingNTRPMS] = useState<boolean>(false);
  const [, setNtRpmsProcessed] = useState<boolean>(false);

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

  // CALCULAR GRUPOS VISIBLES DINÁMICAMENTE
  const visibleFileGroups = useMemo(() => {
    const groups = getVisibleGroups(availableFiles, uploadedFiles);
    console.log('📊 Grupos visibles calculados:', groups.length);
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
      message.warning('Fecha de corte eliminada');
    }
  };

  // Handler para procesar archivos NT RPMS (SIN REQUERIR cutoffDate)
  const handleProcessNTRPMS = async () => {
    if (!folderPath.trim()) {
      message.error('Debe ingresar la ruta de la carpeta con archivos NT RPMS');
      return;
    }

    setProcessingNTRPMS(true);

    try {
      console.log('='.repeat(60));
      console.log('PROCESANDO ARCHIVOS NT RPMS');
      console.log('='.repeat(60));
      console.log(`📁 Carpeta: ${folderPath}`);

      const response = await TechnicalNoteService.processNTRPMSFolder(folderPath);

      if (!response) {
        throw new Error('No se recibió respuesta del servidor');
      }

      if (response.success) {
        // VALORES CON FALLBACK
        const filesProcessed = response.files_processed || 0;
        const totalRows = response.total_rows || 0;
        const totalColumns = response.total_columns || 0;
        const processingTime = response.processing_time_seconds; // Puede ser undefined

        // Calcular tiempo para el mensaje (con validación)
        const timeMessage = (processingTime !== undefined && processingTime !== null)
          ? `en ${processingTime.toFixed(2)}s`
          : '';

        message.success({
          content: `¡Procesamiento exitoso! ${filesProcessed} archivos procesados con ${totalRows.toLocaleString()} registros ${timeMessage}`,
          duration: 5,
        });

        console.log('✓ Procesamiento completado:');
        console.log(`  - Archivos procesados: ${filesProcessed}`);
        console.log(`  - Registros totales: ${totalRows.toLocaleString()}`);
        console.log(`  - Columnas: ${totalColumns}`);

        if (processingTime !== undefined && processingTime !== null) {
          console.log(`  - Tiempo: ${processingTime.toFixed(2)}s`);
        }

        setNtRpmsProcessed(true);
        await loadAvailableFiles();

        // Modal con validaciones
        Modal.success({
          title: '✓ Información Extraida Exitosamente',
          content: (
            <div>
              <p>Se han procesado correctamente los archivos NT RPMS.</p>
              <p>Puedes continuar con la elección de la fecha de corte</p>
            </div>
          ),
          width: 700,
        });

        setFolderPath('');
      } else {
        message.warning('El procesamiento finalizó con advertencias');
        console.warn('⚠️ Procesamiento con advertencias:', response.errors);

        Modal.warning({
          title: 'Procesamiento con Advertencias',
          content: (
            <div>
              <p>El procesamiento finalizó pero algunos archivos tuvieron problemas:</p>
              {response.errors && response.errors.length > 0 ? (
                <ul>
                  {response.errors.map((error, index) => (
                    <li key={index}>{error}</li>
                  ))}
                </ul>
              ) : (
                <p>No se especificaron detalles del error.</p>
              )}
            </div>
          ),
        });
      }
    } catch (error: any) {
      console.group('🔍 ERROR EN COMPONENTE');
      console.error('error completo:', error);
      console.error('error.message:', error.message);
      console.groupEnd();

      let errorMessage = 'Error desconocido al procesar archivos';

      if (error.message) {
        errorMessage = error.message;
        console.log('✅ Mensaje extraído de error.message:', errorMessage);
      } else if (typeof error === 'string') {
        errorMessage = error;
        console.log('✅ Error como string:', errorMessage);
      }

      console.log('📌 Mostrando en UI:', errorMessage);

      message.error({
        content: errorMessage,
        duration: 5
      });

      Modal.error({
        title: 'Error al Consolidar Archivos',
        width: 600,
        content: (
          <div>
            <p style={{ color: '#ff4d4f', fontWeight: 600, fontSize: 14, marginBottom: 12 }}>
              {errorMessage}
            </p>
            <Divider style={{ margin: '12px 0' }} />
            <p style={{ fontSize: 13, color: '#666', marginBottom: 8 }}>
              <strong>Verifique que:</strong>
            </p>
            <ul style={{ fontSize: 13, color: '#666', marginBottom: 12 }}>
              <li>La ruta de la carpeta sea correcta</li>
              <li>La carpeta contiene archivos Excel (.xlsx o .xls)</li>
              <li>Los archivos no están corruptos o protegidos con contraseña</li>
              <li>Tiene permisos de lectura en la carpeta</li>
            </ul>
            <Divider style={{ margin: '12px 0' }} />
            <div style={{ background: '#f5f5f5', padding: 8, borderRadius: 4 }}>
              <p style={{ fontSize: 12, color: '#8c8c8c', marginBottom: 0 }}>
                <strong>Ruta ingresada:</strong>
              </p>
              <code style={{ fontSize: 11 }}>{folderPath}</code>
            </div>
          </div>
        ),
      });
    } finally {
      setProcessingNTRPMS(false);
    }
  };

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

  // Handler unificado para selección de archivos (AHORA SÍ REQUIERE cutoffDate)
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

      console.log(`Archivo cargado exitosamente: ${group.displayName}`);
    } catch (error) {
      console.error(`❌ Error cargando ${group.displayName}:`, error);
      message.error(`Error cargando ${group.displayName}`);
    } finally {
      setFileSelectionLoading(false);
    }
  };

  // Handler para eliminar archivo personalizado
  const handleRemoveUploadedFileWithConfirm = (fileToRemove: CustomUploadedFile) => {
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
    console.log(`🔄 Regenerando reporte con fecha: ${cutoffDateString}`);
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

      {/* PASO 1: Procesar Archivos NT RPMS (PRIMERO, SIN cutoffDate) */}
      <FolderPathSelector
        selectedPath={folderPath}
        onPathChange={setFolderPath}
        onProcess={handleProcessNTRPMS}
        disabled={false}
        processing={processingNTRPMS}
      />

      {/* PASO 2: Selector de Fecha de Corte (DESPUÉS de procesar) */}
      <CutoffDateSelector
        selectedDate={cutoffDate}
        onDateChange={handleCutoffDateChange}
      />

      {/* Loading Progress */}
      <LoadingProgress
        isVisible={loadingFiles || processingNTRPMS}
        isLoadingFiles={true}
      />

      {/* File Grid Section */}
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

        filteredData={filteredData}
        columns={columns}
        selectedFile={selectedFile}
        pagination={pagination}

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
