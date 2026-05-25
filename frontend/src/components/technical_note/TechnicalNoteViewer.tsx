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

import { HeaderSection }      from './HeaderSection';
import { LoadingProgress }    from './LoadingProgress';
import { FolderPathSelector } from './FolderPathSelector';
import { CutoffDateSelector } from './CutoffDateSelector';
import { FileGridSection }    from './FileGridSection';
import { FileUploadModal }    from './FileUploadModal';
import { MainContent }        from './MainContent';

dayjs.locale('es');

const TechnicalNoteViewer: React.FC = () => {
  const [fileSelectionLoading, setFileSelectionLoading] = useState(false);
  const [showUploadModal, setShowUploadModal]           = useState(false);
  const [cutoffDate, setCutoffDate]                     = useState<Dayjs | null>(null);
  const [, setIsAccumulationMode]     = useState(false);
  const [folderPath, setFolderPath]                     = useState('');
  const [processingNTRPMS, setProcessingNTRPMS]         = useState(false);
  const [, setNtRpmsProcessed]                          = useState(false);

  const cutoffDateString = useMemo(
    () => cutoffDate?.format('YYYY-MM-DD'),
    [cutoffDate]
  );

  const {
    uploadedFiles, uploading, fileList,
    handleCustomUpload, handleBeforeUpload,
    handleUploadChange, handleRemoveUploadedFile
  } = useFileUpload();

  const {
    availableFiles, currentFileMetadata, loading, loadingFiles,
    selectedFile, filteredData, pagination, currentPage, totalPages,
    keywordReport, loadingReport, showReport, hasReport,
    reportTotalRecords, reportKeywords, reportMinCount,
    showTemporalData, geographicFilters,
    departamentosOptions, municipiosOptions, ipsOptions,
    loadingGeoFilters, selectedRegimen,
    loadFileData, loadAvailableFiles, getFileByDisplayName,
    handlePaginationChange, handleFiltersChange, handleSortChange,
    handleDeleteRows, handleSearch, loadKeywordAgeReport,
    toggleReportVisibility, regenerateReport,
    onSetReportKeywords, onSetReportMinCount, onSetShowTemporalData,
    onAddKeyword, onRemoveKeyword,
    onDepartamentoChange, onMunicipioChange, onIpsChange, onRegimenChange,
    resetGeographicFilters, hasData, columns, currentPageInfo,
    hasGeographicFilters, geographicSummary, setSelectedFileOnly,
  } = useTechnicalNote();

  const visibleFileGroups = useMemo(
    () => getVisibleGroups(availableFiles, uploadedFiles),
    [availableFiles, uploadedFiles]
  );

  const handleCutoffDateChange = (date: Dayjs | null) => {
    setCutoffDate(date);
    date
      ? message.success(`Fecha de corte establecida: ${date.format('DD/MM/YYYY')}`)
      : message.warning('Fecha de corte eliminada');
  };

  // ─── HANDLER NT RPMS ─────────────────────────────────────────────────────
  const handleProcessNTRPMS = async (mode: 'network' | 'local') => {
    if (!folderPath.trim()) {
      message.error('Debe ingresar la ruta de la carpeta con archivos NT RPMS');
      return;
    }

    setProcessingNTRPMS(true);

    try {
      const response = mode === 'network'
        ? await TechnicalNoteService.processNTRPMSFromNetwork(folderPath)
        : await TechnicalNoteService.processNTRPMSFromLocal(folderPath);

      if (!response) throw new Error('No se recibió respuesta del servidor');

      if (response.success) {
        // ── CORRECCIÓN: leer datos desde response.rpms y response.rmpn ──────
        const rpms = (response as any).rpms ?? {};
        const rmpn = (response as any).rmpn ?? {};

        // Sumar archivos y registros de ambas hojas
        const filesProcessed =
          (rpms.extraction_summary?.archivos_procesados ?? 0) +
          (rmpn.extraction_summary?.archivos_procesados ?? 0);

        const totalRows =
          (rpms.total_rows ?? 0) + (rmpn.total_rows ?? 0);

        // Columnas: tomar el máximo entre ambas hojas
        const totalColumns =
          Math.max(rpms.total_columns ?? 0, rmpn.total_columns ?? 0);

        // Tiempo total viene en el nivel raíz
        const totalTime      = response.total_time as number | undefined;
        const extractionTime = rpms.timing?.extraction_time as number | undefined;
        const conversionTime = rpms.timing?.conversion_time as number | undefined;

        // Compresión: preferir rpms, fallback rmpn
        const compressionInfo = rpms.compression_info ?? rmpn.compression_info;

        // Network info: nivel raíz (viene del backend en process-network)
        const networkInfo = (response as any).network_info;
        const isFromNetwork = networkInfo?.access_type === 'network_share';

        const timeMessage   = totalTime ? `en ${totalTime.toFixed(2)}s` : '';
        const sourceMessage = isFromNetwork
          ? `desde red compartida (${networkInfo?.excel_files_found ?? filesProcessed} archivos Excel encontrados)`
          : 'desde carpeta local';

        message.success({
          content: `Procesamiento exitoso! ${filesProcessed} archivos procesados ${sourceMessage} con ${totalRows.toLocaleString()} registros ${timeMessage}`,
          duration: 6,
        });

        setNtRpmsProcessed(true);
        await loadAvailableFiles();

        Modal.success({
          title: 'Información Extraída Exitosamente',
          width: 700,
          content: (
            <div>
              <p><strong>Procesamiento completado correctamente</strong></p>

              <Divider style={{ margin: '12px 0' }} />

              <p style={{ marginBottom: 4, fontSize: 13 }}><strong>Resumen:</strong></p>
              <ul style={{ fontSize: 13, marginBottom: 0 }}>
                <li>Archivos procesados: <strong>{filesProcessed}</strong></li>
                <li>Registros totales:   <strong>{totalRows.toLocaleString()}</strong></li>
                <li>Columnas:            <strong>{totalColumns}</strong></li>
                {totalTime      && <li>Tiempo total:      <strong>{totalTime.toFixed(2)}s</strong></li>}
                {extractionTime && <li>Tiempo extracción: <strong>{extractionTime.toFixed(2)}s</strong></li>}
                {conversionTime && <li>Tiempo conversión: <strong>{conversionTime.toFixed(2)}s</strong></li>}
              </ul>

              {/* Detalle por hoja */}
              {(rpms.total_rows > 0 || rmpn.total_rows > 0) && (
                <>
                  <Divider style={{ margin: '12px 0' }} />
                  <p style={{ marginBottom: 4, fontSize: 13 }}><strong>Detalle por hoja:</strong></p>
                  <ul style={{ fontSize: 13, marginBottom: 0 }}>
                    {rpms.total_rows > 0 && (
                      <li>NT RPMS: <strong>{rpms.total_rows?.toLocaleString()}</strong> registros</li>
                    )}
                    {rmpn.total_rows > 0 && (
                      <li>NT RMPN: <strong>{rmpn.total_rows?.toLocaleString()}</strong> registros</li>
                    )}
                  </ul>
                </>
              )}

              {networkInfo && (
                <>
                  <Divider style={{ margin: '12px 0' }} />
                  <p style={{ marginBottom: 4, fontSize: 13 }}><strong>Información de Red:</strong></p>
                  <ul style={{ fontSize: 13, marginBottom: 0 }}>
                    <li>Origen: <code style={{ fontSize: 11 }}>{networkInfo.original_path}</code></li>
                    <li>Tipo: <strong>{isFromNetwork ? 'Carpeta compartida en red' : 'Local'}</strong></li>
                    {networkInfo.excel_files_found && (
                      <li>Archivos Excel encontrados: <strong>{networkInfo.excel_files_found}</strong></li>
                    )}
                  </ul>
                </>
              )}

              {compressionInfo && (
                <>
                  <Divider style={{ margin: '12px 0' }} />
                  <p style={{ marginBottom: 4, fontSize: 13 }}><strong>Compresión:</strong></p>
                  <ul style={{ fontSize: 13, marginBottom: 0 }}>
                    <li>CSV:     {compressionInfo.original_size_mb?.toFixed(2)} MB</li>
                    <li>Parquet: {compressionInfo.parquet_size_mb?.toFixed(2)} MB</li>
                    <li>Ahorro:  {compressionInfo.compression_ratio?.toFixed(1)}%</li>
                  </ul>
                </>
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

        Modal.warning({
          title: 'Procesamiento con Advertencias',
          width: 600,
          content: (
            <div>
              <p>El procesamiento finalizó pero algunos archivos tuvieron problemas:</p>
              {(response as any).errors?.length > 0 ? (
                <ul>
                  {(response as any).errors.map((err: string, i: number) => (
                    <li key={i}>{err}</li>
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
      const parts      = (error.message ?? '').split('\n\n💡 Sugerencia:\n');
      const errMsg     = parts[0] || 'Error desconocido al procesar archivos';
      const suggestion = parts[1] ?? '';

      message.error({ content: errMsg, duration: 6 });

      Modal.error({
        title: 'Error al Procesar Archivos NT RPMS',
        width: 650,
        content: (
          <div>
            <p style={{ color: '#ff4d4f', fontWeight: 600, fontSize: 14, marginBottom: 12 }}>
              {errMsg}
            </p>

            {suggestion && (
              <>
                <Divider style={{ margin: '12px 0' }} />
                <div style={{ background: '#e6f7ff', padding: 12, borderRadius: 4 }}>
                  <p style={{ fontSize: 13, color: '#0050b3', marginBottom: 4 }}>
                    <strong>Sugerencia:</strong>
                  </p>
                  <p style={{ fontSize: 13, color: '#0050b3', marginBottom: 0, whiteSpace: 'pre-line' }}>
                    {suggestion}
                  </p>
                </div>
              </>
            )}

            <Divider style={{ margin: '12px 0' }} />
            <p style={{ fontSize: 13, color: '#666', marginBottom: 8 }}>
              <strong>Verifique que:</strong>
            </p>
            <ul style={{ fontSize: 13, color: '#666', marginBottom: 12 }}>
              {mode === 'network' ? (
                <>
                  <li>La carpeta está compartida correctamente en el equipo cliente</li>
                  <li>El servidor tiene permisos de lectura sobre la carpeta compartida</li>
                  <li>El firewall permite compartir archivos (SMB/CIFS)</li>
                  <li>Ambos equipos están en la misma red</li>
                  <li>La ruta UNC tiene el formato correcto: \\IP\carpeta</li>
                </>
              ) : (
                <>
                  <li>La ruta de la carpeta sea correcta y exista en el servidor</li>
                  <li>La carpeta contiene archivos Excel (.xlsx o .xls)</li>
                  <li>Los archivos no están corruptos o protegidos con contraseña</li>
                  <li>Tiene permisos de lectura en la carpeta</li>
                </>
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
  // ─────────────────────────────────────────────────────────────────────────

  useEffect(() => {
    loadAvailableFiles().catch(console.error);
  }, [loadAvailableFiles]);

  const handleFileGroupClick = async (group: AgeGroupIcon) => {
    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte antes de cargar archivos');
      return;
    }
    if (!group.filename) return;

    if (hasReport) {
      setIsAccumulationMode(true);
      setSelectedFileOnly(group.filename);
      message.info({
        content: `Archivo "${group.displayName}" seleccionado. Presione "Actualizar Reporte" para acumular datos.`,
        duration: 4
      });
      return;
    }

    try {
      setFileSelectionLoading(true);
      await loadFileData(group.filename, cutoffDate.format('YYYY-MM-DD'));
      if (showUploadModal) setShowUploadModal(false);
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
        } catch {
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
      const name = options.file.name;
      if (isPredefinedFile(name)) {
        message.success(`Archivo "${name}" asociado al grupo: ${getPredefinedGroupKey(name)}`, 3);
      } else {
        message.success(`Archivo "${name}" cargado como personalizado`, 2);
      }
    } catch { /* ya manejado */ }
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
    regenerateReport(cutoffDateString);
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
        onSetReportKeywords={onSetReportKeywords}
        onSetReportMinCount={onSetReportMinCount}
        onSetShowTemporalData={onSetShowTemporalData}
        onLoadKeywordAgeReport={loadKeywordAgeReport}
        onAddKeyword={onAddKeyword}
        onRemoveKeyword={onRemoveKeyword}
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