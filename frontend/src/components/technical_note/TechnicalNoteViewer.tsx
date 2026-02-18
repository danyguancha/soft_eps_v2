// components/technical-note/TechnicalNoteViewer.tsx - VERSIÓN CORREGIDA

import React, { useState, useEffect, useMemo } from 'react';
import { Modal, message, Divider, Button as AntButton } from 'antd';
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

  // 🔥 NUEVO: Estado para controlar si estamos en "modo acumulación"
  const [isAccumulationMode, setIsAccumulationMode] = useState(false);

  // Estados para NT RPMS
  const [folderPath, setFolderPath] = useState<string>('');
  const [processingNTRPMS, setProcessingNTRPMS] = useState<boolean>(false);
  const [, setNtRpmsProcessed] = useState<boolean>(false);

  // Convertir cutoffDate
  const cutoffDateString = useMemo(() => {
    const result = cutoffDate ? cutoffDate.format('YYYY-MM-DD') : undefined;
    console.log('TechnicalNoteViewer - cutoffDateString calculado:', result);
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
    selectedRegimen,
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
    onRegimenChange,
    resetGeographicFilters,
    hasData,
    columns,
    currentPageInfo,
    hasGeographicFilters,
    geographicSummary,
    setSelectedFileOnly, // 🔥 NUEVA FUNCIÓN
  } = useTechnicalNote();

  // Calcular grupos visibles
  const visibleFileGroups = useMemo(() => {
    const groups = getVisibleGroups(availableFiles, uploadedFiles);
    console.log('Grupos visibles calculados:', groups.length);
    return groups;
  }, [availableFiles, uploadedFiles]);

  // Handler para cambio de fecha de corte
  const handleCutoffDateChange = (date: Dayjs | null) => {
    console.log('handleCutoffDateChange llamado con:', date?.format('YYYY-MM-DD'));
    setCutoffDate(date);
    if (date) {
      console.log(`Fecha de corte seleccionada: ${date.format('DD/MM/YYYY')}`);
      message.success(`Fecha de corte establecida: ${date.format('DD/MM/YYYY')}`);
    } else {
      console.log('Fecha de corte eliminada');
      message.warning('Fecha de corte eliminada');
    }
  };

  // Handler para procesar archivos NT RPMS
  const handleProcessNTRPMS = async (mode: 'network' | 'local') => {
    if (!folderPath.trim()) {
      message.error('Debe ingresar la ruta de la carpeta con archivos NT RPMS');
      return;
    }

    setProcessingNTRPMS(true);

    try {
      console.log('='.repeat(60));
      console.log(`PROCESANDO ARCHIVOS NT RPMS - MODO: ${mode.toUpperCase()}`);
      console.log('='.repeat(60));
      console.log(`Ruta: ${folderPath}`);

      let response;
      if (mode === 'network') {
        console.log('Procesando desde carpeta compartida en red...');
        response = await TechnicalNoteService.processNTRPMSFromNetwork(folderPath);
      } else {
        console.log('Procesando desde carpeta local del servidor...');
        response = await TechnicalNoteService.processNTRPMSFromLocal(folderPath);
      }

      if (!response) {
        throw new Error('No se recibió respuesta del servidor');
      }

      if (response.success) {
        const filesProcessed = response.extraction_summary?.archivos_procesados || 0;
        const totalRows = response.total_rows || 0;
        const totalColumns = response.total_columns || 0;
        const totalTime = response.timing?.total_time;
        const extractionTime = response.timing?.extraction_time;
        const conversionTime = response.timing?.conversion_time;

        const networkInfo = response.network_info;
        const isFromNetwork = networkInfo?.access_type === 'network_share';

        const timeMessage = totalTime ? `en ${totalTime.toFixed(2)}s` : '';
        const sourceMessage = isFromNetwork 
          ? `desde red compartida (${networkInfo?.excel_files_found || filesProcessed} archivos Excel encontrados)`
          : 'desde carpeta local';

        message.success({
          content: `Procesamiento exitoso! ${filesProcessed} archivos procesados ${sourceMessage} con ${totalRows.toLocaleString()} registros ${timeMessage}`,
          duration: 6,
        });

        console.log('Procesamiento completado:');
        console.log(`  - Archivos procesados: ${filesProcessed}`);
        console.log(`  - Registros totales: ${totalRows.toLocaleString()}`);
        console.log(`  - Columnas: ${totalColumns}`);
        
        if (extractionTime) {
          console.log(`  - Tiempo extracción: ${extractionTime.toFixed(2)}s`);
        }
        if (conversionTime) {
          console.log(`  - Tiempo conversión: ${conversionTime.toFixed(2)}s`);
        }
        if (totalTime) {
          console.log(`  - Tiempo total: ${totalTime.toFixed(2)}s`);
        }

        if (networkInfo) {
          console.log(`  - Carpeta origen: ${networkInfo.original_path}`);
          console.log(`  - Tipo acceso: ${networkInfo.access_type}`);
        }

        if (response.compression_info) {
          const compression = response.compression_info;
          console.log(`  - Tamaño CSV: ${compression.original_size_mb?.toFixed(2)} MB`);
          console.log(`  - Tamaño Parquet: ${compression.parquet_size_mb?.toFixed(2)} MB`);
          console.log(`  - Ratio compresión: ${compression.compression_ratio?.toFixed(1)}%`);
        }

        setNtRpmsProcessed(true);
        await loadAvailableFiles();

        Modal.success({
          title: 'Información Extraída Exitosamente',
          width: 700,
          content: (
            <div>
              <p><strong>Procesamiento completado correctamente</strong></p>
              
              <Divider style={{ margin: '12px 0' }} />
              
              <div style={{ marginBottom: 12 }}>
                <p style={{ marginBottom: 4, fontSize: 13 }}>
                  <strong>Resumen:</strong>
                </p>
                <ul style={{ fontSize: 13, marginBottom: 0 }}>
                  <li>Archivos procesados: <strong>{filesProcessed}</strong></li>
                  <li>Registros totales: <strong>{totalRows.toLocaleString()}</strong></li>
                  <li>Columnas: <strong>{totalColumns}</strong></li>
                  {totalTime && <li>Tiempo total: <strong>{totalTime.toFixed(2)}s</strong></li>}
                </ul>
              </div>

              {networkInfo && (
                <React.Fragment>
                  <Divider style={{ margin: '12px 0' }} />
                  <div style={{ marginBottom: 12 }}>
                    <p style={{ marginBottom: 4, fontSize: 13 }}>
                      <strong>Información de Red:</strong>
                    </p>
                    <ul style={{ fontSize: 13, marginBottom: 0 }}>
                      <li>Origen: <code style={{ fontSize: 11 }}>{networkInfo.original_path}</code></li>
                      <li>Tipo: <strong>{networkInfo.access_type === 'network_share' ? 'Carpeta compartida en red' : 'Local'}</strong></li>
                      {networkInfo.excel_files_found && (
                        <li>Archivos Excel encontrados: <strong>{networkInfo.excel_files_found}</strong></li>
                      )}
                    </ul>
                  </div>
                </React.Fragment>
              )}

              {response.compression_info && (
                <React.Fragment>
                  <Divider style={{ margin: '12px 0' }} />
                  <div style={{ marginBottom: 12 }}>
                    <p style={{ marginBottom: 4, fontSize: 13 }}>
                      <strong>Compresión:</strong>
                    </p>
                    <ul style={{ fontSize: 13, marginBottom: 0 }}>
                      <li>CSV: {response.compression_info.original_size_mb?.toFixed(2)} MB</li>
                      <li>Parquet: {response.compression_info.parquet_size_mb?.toFixed(2)} MB</li>
                      <li>Ahorro: {response.compression_info.compression_ratio?.toFixed(1)}%</li>
                    </ul>
                  </div>
                </React.Fragment>
              )}

              <Divider style={{ margin: '12px 0' }} />
              
              <p style={{ fontSize: 13, color: '#52c41a', marginBottom: 4 }}>
                <strong>Siguiente paso:</strong>
              </p>
              <p style={{ fontSize: 13, marginBottom: 0 }}>
                Puedes continuar con la elección de la fecha de corte
              </p>
            </div>
          ),
        });

        setFolderPath('');
      } else {
        message.warning('El procesamiento finalizó con advertencias');
        console.warn('Procesamiento con advertencias:', response.errors);

        Modal.warning({
          title: 'Procesamiento con Advertencias',
          width: 600,
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
              
              {response.extraction_summary?.errores && response.extraction_summary.errores.length > 0 && (
                <React.Fragment>
                  <Divider style={{ margin: '12px 0' }} />
                  <p><strong>Errores específicos por archivo:</strong></p>
                  <ul style={{ fontSize: 12 }}>
                    {response.extraction_summary.errores.map(([file, error], index) => (
                      <li key={index}>
                        <code style={{ fontSize: 11 }}>{file}</code>: {error}
                      </li>
                    ))}
                  </ul>
                </React.Fragment>
              )}
            </div>
          ),
        });
      }
    } catch (error: any) {
      console.group('ERROR EN COMPONENTE');
      console.error('error completo:', error);
      console.error('error.message:', error.message);
      console.groupEnd();

      let errorMessage = 'Error desconocido al procesar archivos';
      let suggestion = '';

      if (error.message) {
        const parts = error.message.split('\n\nSugerencia:\n');
        errorMessage = parts[0];
        if (parts.length > 1) {
          suggestion = parts[1];
        }
      } else if (typeof error === 'string') {
        errorMessage = error;
      }

      message.error({
        content: errorMessage,
        duration: 6
      });

      Modal.error({
        title: 'Error al Procesar Archivos NT RPMS',
        width: 650,
        content: (
          <div>
            <p style={{ color: '#ff4d4f', fontWeight: 600, fontSize: 14, marginBottom: 12 }}>
              {errorMessage}
            </p>
            
            {suggestion && (
              <React.Fragment>
                <Divider style={{ margin: '12px 0' }} />
                <div style={{ background: '#e6f7ff', padding: 12, borderRadius: 4, marginBottom: 12 }}>
                  <p style={{ fontSize: 13, color: '#0050b3', marginBottom: 4 }}>
                    <strong>Sugerencia:</strong>
                  </p>
                  <p style={{ fontSize: 13, color: '#0050b3', marginBottom: 0, whiteSpace: 'pre-line' }}>
                    {suggestion}
                  </p>
                </div>
              </React.Fragment>
            )}
            
            <Divider style={{ margin: '12px 0' }} />
            <p style={{ fontSize: 13, color: '#666', marginBottom: 8 }}>
              <strong>Verifique que:</strong>
            </p>
            <ul style={{ fontSize: 13, color: '#666', marginBottom: 12 }}>
              {mode === 'network' ? (
                <React.Fragment>
                  <li>La carpeta está compartida correctamente en el equipo cliente</li>
                  <li>El servidor tiene permisos de lectura sobre la carpeta compartida</li>
                  <li>El firewall permite compartir archivos (SMB/CIFS)</li>
                  <li>Ambos equipos están en la misma red</li>
                  <li>La ruta UNC tiene el formato correcto: \\IP\carpeta</li>
                </React.Fragment>
              ) : (
                <React.Fragment>
                  <li>La ruta de la carpeta sea correcta y existe en el servidor</li>
                  <li>La carpeta contiene archivos Excel (.xlsx o .xls)</li>
                  <li>Los archivos no están corruptos o protegidos con contraseña</li>
                  <li>Tiene permisos de lectura en la carpeta</li>
                </React.Fragment>
              )}
            </ul>
            <Divider style={{ margin: '12px 0' }} />
            <div style={{ background: '#f5f5f5', padding: 8, borderRadius: 4 }}>
              <p style={{ fontSize: 12, color: '#8c8c8c', marginBottom: 4 }}>
                <strong>Ruta ingresada:</strong>
              </p>
              <code style={{ fontSize: 11 }}>{folderPath}</code>
              <p style={{ fontSize: 12, color: '#8c8c8c', marginTop: 8, marginBottom: 0 }}>
                <strong>Modo:</strong> {mode === 'network' ? 'Red compartida' : 'Local del servidor'}
              </p>
            </div>
          </div>
        ),
      });
    } finally {
      setProcessingNTRPMS(false);
    }
  };

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

  // 🔥 HANDLER MODIFICADO: Click en archivo desde la grilla
  const handleFileGroupClick = async (group: AgeGroupIcon) => {
    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte antes de cargar archivos');
      return;
    }

    if (!group.filename) return;

    // 🔥 Si ya existe un reporte, solo cambiar el archivo seleccionado (modo acumulación)
    if (hasReport) {
      console.log('📊 MODO ACUMULACIÓN: Solo cambiando archivo seleccionado a', group.filename);
      setIsAccumulationMode(true);
      setSelectedFileOnly(group.filename);
      message.info({
        content: `Archivo "${group.displayName}" seleccionado. Presione "Actualizar Reporte" para acumular datos.`,
        duration: 4
      });
      return;
    }

    // Si NO hay reporte, cargar datos normalmente
    try {
      setFileSelectionLoading(true);

      console.log(`Cargando archivo: ${group.filename}`);
      console.log(`Con fecha de corte: ${cutoffDate.format('YYYY-MM-DD')}`);

      await loadFileData(group.filename, cutoffDate.format('YYYY-MM-DD'));

      if (showUploadModal) {
        setShowUploadModal(false);
      }

      console.log(`Archivo cargado exitosamente: ${group.displayName}`);
    } catch (error) {
      console.error(`Error cargando ${group.displayName}:`, error);
      message.error(`Error cargando ${group.displayName}`);
    } finally {
      setFileSelectionLoading(false);
    }
  };

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
      // Error ya manejado
    }
  };

  const handleShowUploadModal = () => {
    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte antes de cargar archivos');
      return;
    }
    setShowUploadModal(true);
  };

  const handleRegenerateReport = () => {
    if (!cutoffDateString) {
      message.error('Debe seleccionar una fecha de corte antes de regenerar el reporte');
      return;
    }
    console.log(`Regenerando reporte con fecha: ${cutoffDateString}`);
    regenerateReport(cutoffDateString);
  };

  const handleAddKeyword = (value: string) => {
    console.log(`Agregando palabra clave: ${value}`);
    onAddKeyword(value);
  };

  const handleRemoveKeyword = (keyword: string) => {
    console.log(`Removiendo palabra clave: ${keyword}`);
    onRemoveKeyword(keyword);
  };

  const handleSetReportKeywords = (keywords: string[]) => {
    console.log(`Estableciendo nuevas palabras clave: ${keywords}`);
    onSetReportKeywords(keywords);
  };

  return (
    <div style={{ padding: '24px' }}>
      <HeaderSection
        hasGeographicFilters={hasGeographicFilters}
        geographicSummary={geographicSummary}
        loadingFiles={loadingFiles}
        onShowUploadModal={handleShowUploadModal}
        onLoadAvailableFiles={loadAvailableFiles}
        onResetGeographicFilters={resetGeographicFilters}
      />

      <FolderPathSelector
        selectedPath={folderPath}
        onPathChange={setFolderPath}
        onProcess={handleProcessNTRPMS}
        disabled={false}
        processing={processingNTRPMS}
      />

      <CutoffDateSelector
        selectedDate={cutoffDate}
        onDateChange={handleCutoffDateChange}
      />

      <LoadingProgress
        isVisible={loadingFiles || processingNTRPMS}
        isLoadingFiles={true}
      />

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

      <LoadingProgress
        isVisible={loading && !!currentFileMetadata}
        currentPage={currentPage}
        totalPages={totalPages}
        hasGeographicFilters={hasGeographicFilters}
        geographicSummary={geographicSummary}
        isLoadingFiles={false}
      />

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
        selectedRegimen={selectedRegimen}
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
        onRegimenChange={onRegimenChange}
        resetGeographicFilters={resetGeographicFilters}
        onShowUploadModal={handleShowUploadModal}
      />
    </div>
  );
};

export default TechnicalNoteViewer;
