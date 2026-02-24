// components/technical-note/report/Report.tsx - ACUMULACIÓN PERFECTA

import React, { memo, useCallback, useState, useEffect, useRef } from 'react';
import { Card, Typography, Button, message, Space, Select, Tag, Tooltip } from 'antd';
import {
  BarChartOutlined,
  CalendarOutlined,
  UserDeleteOutlined,
  MedicineBoxOutlined,
  ClearOutlined,
  FileOutlined
} from '@ant-design/icons';

// Componentes
import { GeographicFilters } from './GeographicFilters';
import { KeywordControls } from './KeywordControls';
import { KeywordStatistics } from './KeywordStatistics';
import { ReportTable } from './ReportTable';
import { ReportHeader } from './ReportHeader';
import { ReportControls } from './ReportControls';
import { InasistentesTable } from './InasistentesTable';
import {
  ReportLoading,
  NoResultsAlert,
  NoReportState
} from './ReportAuxiliaryComponents';

// Hooks y configuración
import { useReportData } from '../../../hooks/useReportData';
import { DEFAULT_KEYWORDS } from '../../../config/reportKeywords.config';
import type { TemporalReportProps } from './interfaces/ReportInterfaz';
import { TechnicalNoteService } from '../../../services/TechnicalNoteService';
import type { InasistentesReportResponse } from '../../../interfaces/IAbsentUser';
import { AlertModal } from '../../alerts/AlertModal';

const { Text } = Typography;
const { Option } = Select;

// Interfaz para items del reporte
interface ReportItem {
  actividad?: string;
  keyword?: string;
  edad: string;
  regimen?: string;
  consulta_procedimiento?: string;
  rango_edad?: string;
  [key: string]: any;
}

// INTERFAZ EXTENDIDA CON RÉGIMEN
interface ReportPropsExtended extends TemporalReportProps {
  cutoffDate?: string;
  selectedRegimen?: 'Subsidiado' | 'Contributivo' | null;
  onRegimenChange?: (regimen: 'Subsidiado' | 'Contributivo' | null) => void;
}

// INTERFAZ: Reporte acumulado con metadatos
interface AccumulatedReport {
  items: ReportItem[];
  sources: string[];
  total_rows: number;
  meses_reportados: number;
  corte_fecha?: string;
  global_statistics?: any;
}

export const Report: React.FC<ReportPropsExtended> = memo(({
  keywordReport,
  loadingReport,
  showReport,
  hasReport,
  reportTotalRecords,
  selectedFile,
  reportKeywords,
  reportMinCount,
  showTemporalData,
  geographicFilters,
  departamentosOptions,
  municipiosOptions,
  ipsOptions,
  loadingGeoFilters,
  cutoffDate,
  selectedRegimen,
  onToggleReportVisibility,
  onSetReportKeywords,
  onSetShowTemporalData,
  onLoadKeywordAgeReport,
  onDepartamentoChange,
  onMunicipioChange,
  onIpsChange,
  onRegimenChange,
  resetGeographicFilters,
}) => {
  console.log('🔍 Report recibió cutoffDate:', cutoffDate);
  console.log('🔍 Report recibió selectedFile:', selectedFile);
  console.log('🔍 Report recibió selectedRegimen:', selectedRegimen);

  const { keywordStats, reportTitle } = useReportData(keywordReport, reportKeywords);

  // Estados de reporte acumulado
  const [accumulatedReport, setAccumulatedReport] = useState<AccumulatedReport | null>(null);

  // Refs para tracking confiable
  const isFirstReportRef = useRef(true);
  const lastProcessedFileRef = useRef<string | null>(null);
  const lastReportTimestampRef = useRef<number>(0);

  // Estados: Manejo de reporte de inasistentes
  const [inasistentesReport, setInasistentesReport] = useState<InasistentesReportResponse | null>(null);
  const [loadingInasistentes, setLoadingInasistentes] = useState(false);
  const [showInasistentesReport, setShowInasistentesReport] = useState(false);

  // Modal de alerta para errores sin perder acumulado
  const [alertOpen, setAlertOpen] = useState(false);
  const [alertTitle, setAlertTitle] = useState('');
  const [alertMessage, setAlertMessage] = useState('');
  const [alertVariant, setAlertVariant] = useState<'info' | 'success' | 'error' | 'warning'>('warning');

  // 🔍 Determinar si hay algún reporte (normal o acumulado)
  const hasAnyReport = Boolean(
    (accumulatedReport && accumulatedReport.items.length > 0) ||
    (keywordReport && keywordReport.items && keywordReport.items.length > 0)
  );

  // 🔥 EFECTO PRINCIPAL: Acumular reportes automáticamente
  useEffect(() => {
    // Validación básica
    if (!keywordReport || !keywordReport.items) {
      console.log('⏭️ keywordReport vacío o sin items, saltando...');
      return;
    }

    // ✅ FIX: Caso especial - backend respondió sin items (estructura incorrecta)
    if (keywordReport.items.length === 0) {
      const currentFile = selectedFile || 'archivo actual';

      if (accumulatedReport && accumulatedReport.items.length > 0) {
        // Ya existe reporte acumulado: advertencia manteniendo los datos previos
        setAlertTitle('Estructura de archivo no compatible');
        setAlertMessage(
          `No se generó reporte para el archivo "${currentFile}".\n\n` +
          'Los datos del reporte acumulado se mantienen sin cambios.\n\n' +
          'Por favor, verifique que la estructura de este archivo coincida con el formato esperado por SIGIRES (columnas y encabezados).'
        );
        setAlertVariant('warning');
      } else {
        // ✅ NUEVO: Es el primer archivo y la estructura es incorrecta
        setAlertTitle('Estructura de archivo no compatible');
        setAlertMessage(
          `No se pudo generar el reporte para el archivo "${currentFile}".\n\n` +
          'El archivo no contiene datos válidos o su estructura no coincide con el formato esperado por SIGIRES.\n\n' +
          'Verifique que las columnas y encabezados del archivo sean correctos antes de continuar.'
        );
        setAlertVariant('error');
      }

      setAlertOpen(true);
      return;
    }

    const currentFile = selectedFile || 'unknown';
    const now = Date.now();

    // PROTECCIÓN: Evitar procesar el mismo reporte múltiples veces en < 500ms
    if (now - lastReportTimestampRef.current < 500) {
      console.log('⚠️ Reporte recibido demasiado rápido (< 500ms), ignorando duplicado...');
      return;
    }
    lastReportTimestampRef.current = now;

    console.log('📊 ========== PROCESANDO NUEVO REPORTE ==========');
    console.log('   Items recibidos:', keywordReport.items.length);
    console.log('   Archivo actual:', currentFile);
    console.log('   Último archivo procesado:', lastProcessedFileRef.current);
    console.log('   Es primer reporte:', isFirstReportRef.current);
    console.log('   Reporte acumulado existe:', !!accumulatedReport);
    console.log('   Items acumulados:', accumulatedReport?.items.length || 0);
    console.log('   Archivos en reporte:', accumulatedReport?.sources || []);
    console.log('   Timestamp:', now);
    console.log('===============================================');

    // 🔥 CASO 1: Primer reporte (crear base)
    if (isFirstReportRef.current || !accumulatedReport) {
      console.log('✨ CASO 1: Creando primer reporte base');

      const newReport = {
        items: keywordReport.items.map((item: any) => ({
          ...item,
          _source_file: currentFile
        })),
        sources: [currentFile],
        total_rows: keywordReport.total_rows || keywordReport.items.length,
        meses_reportados: keywordReport.meses_reportados || 12,
        corte_fecha: keywordReport.corte_fecha || cutoffDate,
        global_statistics: keywordReport.global_statistics
      };

      setAccumulatedReport(newReport);
      lastProcessedFileRef.current = currentFile;
      isFirstReportRef.current = false;

      console.log('   ✅ Reporte base creado:', newReport.items.length, 'items');
      console.log('   📅 Fecha de corte:', newReport.corte_fecha);
      message.success(`✅ Reporte creado: ${keywordReport.items.length} items de "${currentFile}"`);
      return;
    }

    // 🔥 CASO 2: Mismo archivo que el último procesado (ACTUALIZAR ese archivo)
    if (currentFile === lastProcessedFileRef.current) {
      console.log('🔄 CASO 2: Mismo archivo - Actualizando items del archivo actual');

      const itemsFromOtherFiles = accumulatedReport.items.filter((item: any) => {
        const itemFile = item._source_file || lastProcessedFileRef.current;
        return itemFile !== currentFile;
      });

      const newItemsWithSource = keywordReport.items.map((item: any) => ({
        ...item,
        _source_file: currentFile
      }));

      const updatedItems = [...itemsFromOtherFiles, ...newItemsWithSource];

      console.log('   Items de otros archivos:', itemsFromOtherFiles.length);
      console.log('   Items nuevos del archivo actual:', newItemsWithSource.length);
      console.log('   Total items después de actualizar:', updatedItems.length);

      setAccumulatedReport({
        ...accumulatedReport,
        items: updatedItems,
        total_rows: updatedItems.length,
        corte_fecha: keywordReport.corte_fecha || accumulatedReport.corte_fecha || cutoffDate,
      });

      message.info(`🔄 Reporte actualizado: ${newItemsWithSource.length} items de "${currentFile}"`);
      return;
    }

    // 🔥 CASO 3: Archivo DIFERENTE (ACUMULAR - agregar nuevo archivo)
    console.log('➕ CASO 3: Archivo diferente - Acumulando items');
    console.log('   Archivos previos:', accumulatedReport.sources);
    console.log('   Archivo nuevo:', currentFile);

    const existingKeys = new Set(
      accumulatedReport.items.map((item: any) => {
        const proc = item.consulta_procedimiento || item.actividad || item.keyword || '';
        const edad = item.edad || item.rango_edad || '';
        const reg = item.regimen || 'N/A';
        return `${proc}-${edad}-${reg}`;
      })
    );

    console.log('   Claves únicas existentes:', existingKeys.size);

    const newItems = keywordReport.items.filter((item: any) => {
      const proc = item.consulta_procedimiento || item.actividad || item.keyword || '';
      const edad = item.edad || item.rango_edad || '';
      const reg = item.regimen || 'N/A';
      const key = `${proc}-${edad}-${reg}`;
      return !existingKeys.has(key);
    }).map((item: any) => ({
      ...item,
      _source_file: currentFile
    }));

    console.log('   Items únicos a agregar:', newItems.length);
    console.log('   Items duplicados ignorados:', keywordReport.items.length - newItems.length);

    const combinedItems = [...accumulatedReport.items, ...newItems];

    const updatedSources = accumulatedReport.sources.includes(currentFile)
      ? accumulatedReport.sources
      : [...accumulatedReport.sources, currentFile];

    console.log('   Archivos en el reporte después:', updatedSources);
    console.log('   Total items combinados:', combinedItems.length);

    setAccumulatedReport({
      items: combinedItems,
      sources: updatedSources,
      total_rows: combinedItems.length,
      meses_reportados: Math.max(
        accumulatedReport.meses_reportados,
        keywordReport.meses_reportados || 12
      ),
      corte_fecha: accumulatedReport.corte_fecha || keywordReport.corte_fecha || cutoffDate,
      global_statistics: {
        numerador: (accumulatedReport.global_statistics?.numerador || 0) +
                   (keywordReport.global_statistics?.numerador || 0),
        denominador: (accumulatedReport.global_statistics?.denominador || 0) +
                     (keywordReport.global_statistics?.denominador || 0),
        cobertura_porcentaje: 0
      }
    });

    lastProcessedFileRef.current = currentFile;

    if (newItems.length > 0) {
      message.success(`✅ ${newItems.length} items nuevos agregados de "${currentFile}" (Total acumulado: ${combinedItems.length})`);
    }

  }, [keywordReport, selectedFile]);

  // EFECTO: Recalcular cobertura global del reporte acumulado
  useEffect(() => {
    if (accumulatedReport && accumulatedReport.global_statistics) {
      const { numerador, denominador } = accumulatedReport.global_statistics;
      const cobertura = denominador > 0 ? (numerador / denominador) * 100 : 0;

      setAccumulatedReport(prev => {
        if (!prev) return prev;
        return {
          ...prev,
          global_statistics: {
            ...prev.global_statistics,
            cobertura_porcentaje: parseFloat(cobertura.toFixed(2))
          }
        };
      });
    }
  }, [accumulatedReport?.global_statistics?.numerador, accumulatedReport?.global_statistics?.denominador]);

  // HANDLER: Cambio de régimen
  const handleRegimenChange = useCallback((value: 'Subsidiado' | 'Contributivo' | 'todos') => {
    console.log('🏥 Cambio de régimen:', value);
    if (onRegimenChange) {
      onRegimenChange(value === 'todos' ? null : value);
    }
  }, [onRegimenChange]);

  // HANDLER: Generar PRIMER reporte (limpia acumulación)
  const handleLoadReport = useCallback(async () => {
    console.log('📊 ========== GENERANDO PRIMER REPORTE ==========');
    console.log('   Limpiando acumulación...');

    if (!selectedFile) {
      message.error('No hay archivo seleccionado');
      return;
    }

    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte antes de generar el reporte');
      return;
    }

    setAccumulatedReport(null);
    isFirstReportRef.current = true;
    lastProcessedFileRef.current = null;
    lastReportTimestampRef.current = 0;

    console.log('✅ Estado limpiado, generando primer reporte con:', {
      selectedFile,
      cutoffDate,
      selectedRegimen
    });
    console.log('===============================================');

    try {
      await onLoadKeywordAgeReport(
        selectedFile,
        cutoffDate,
        reportKeywords.length > 0 ? reportKeywords : DEFAULT_KEYWORDS,
        reportMinCount,
        true,
        geographicFilters,
        selectedRegimen
      );
    } catch (error: any) {
      const msg =
        typeof error?.message === 'string'
          ? error.message
          : 'No se pudo generar el reporte para este archivo.';

      console.error('❌ Error al generar primer reporte:', error);

      setAlertTitle('No se pudo generar el reporte');
      setAlertMessage(
        `${msg}\n\n` +
        'Verifique que la estructura de este archivo coincida con el formato esperado por SIGIRES.'
      );
      setAlertVariant('error');
      setAlertOpen(true);
    }
  }, [selectedFile, cutoffDate, reportKeywords, reportMinCount, geographicFilters, selectedRegimen, onLoadKeywordAgeReport]);

  // 🔥 HANDLER: ACTUALIZAR/ACUMULAR reporte
  const handleRegenerateReport = useCallback(async () => {
    console.log('🔄 ========== ACTUALIZANDO/ACUMULANDO REPORTE ==========');
    console.log('   Archivo seleccionado:', selectedFile);
    console.log('   Último procesado:', lastProcessedFileRef.current);
    console.log('   Archivos en reporte:', accumulatedReport?.sources || []);

    if (!selectedFile) {
      message.error('No hay archivo seleccionado');
      return;
    }

    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte');
      return;
    }

    const isSameFile = selectedFile === lastProcessedFileRef.current;
    const action = isSameFile ? 'ACTUALIZAR' : 'ACUMULAR';

    console.log('   Acción:', action);
    console.log('   Se agregará al reporte existente:', !!accumulatedReport);
    console.log('=======================================================');

    try {
      await onLoadKeywordAgeReport(
        selectedFile,
        cutoffDate,
        reportKeywords,
        reportMinCount,
        showTemporalData,
        geographicFilters,
        selectedRegimen
      );
    } catch (error: any) {
      const msg =
        typeof error?.message === 'string'
          ? error.message
          : 'No se pudo generar el reporte para este archivo.';

      console.error('❌ Error al actualizar/acumular reporte:', error);

      setAlertTitle('Reporte no generado para este archivo');
      setAlertMessage(
        `${msg}\n\n` +
        'Los datos acumulados previamente se mantienen.\n\n' +
        'Revise que la estructura de este archivo coincida con la de SIGIRES antes de continuar.'
      );
      setAlertVariant('warning');
      setAlertOpen(true);
    }
  }, [selectedFile, cutoffDate, reportKeywords, reportMinCount, showTemporalData, geographicFilters, selectedRegimen, accumulatedReport, onLoadKeywordAgeReport]);

  // HANDLER: Limpiar reporte acumulado
  const handleClearAccumulatedReport = useCallback(() => {
    console.log('🗑️ ========== LIMPIANDO REPORTE ACUMULADO ==========');
    setAccumulatedReport(null);
    isFirstReportRef.current = true;
    lastProcessedFileRef.current = null;
    lastReportTimestampRef.current = 0;
    console.log('✅ Reporte limpiado completamente');
    console.log('==================================================');
    message.success('✅ Reporte limpiado. Puede generar un nuevo reporte desde cero.');
  }, []);

  // HANDLER: Generar reporte de inasistentes
  const handleGenerateInasistentesReport = useCallback(async () => {
    if (!selectedFile) {
      message.error('No hay archivo seleccionado');
      return;
    }

    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte');
      return;
    }

    setLoadingInasistentes(true);
    setShowInasistentesReport(true);

    try {
      console.log('🏥 Generando reporte de inasistentes...');

      const response = await TechnicalNoteService.getInasistentesReport(
        selectedFile,
        cutoffDate,
        reportKeywords.length > 0 ? reportKeywords : DEFAULT_KEYWORDS,
        {
          departamento: geographicFilters.departamento,
          municipio: geographicFilters.municipio,
          ips: geographicFilters.ips
        },
        selectedRegimen || undefined
      );

      console.log('✅ Reporte de inasistentes generado:', response);

      setInasistentesReport(response);

      if (response.success && response.resumen_general) {
        const total = response.resumen_general.total_inasistentes_global;
        message.success(`Reporte generado: ${total} inasistentes encontrados`);
      } else {
        message.warning('Reporte generado sin inasistentes');
      }

    } catch (error) {
      console.error('❌ Error generando reporte de inasistentes:', error);
      message.error('Error generando reporte de inasistentes');
      setInasistentesReport(null);
      setShowInasistentesReport(false);
    } finally {
      setLoadingInasistentes(false);
    }
  }, [selectedFile, cutoffDate, reportKeywords, geographicFilters, selectedRegimen]);

  // HANDLER: Ocultar reporte de inasistentes
  const handleHideInasistentesReport = useCallback(() => {
    setShowInasistentesReport(false);
    setInasistentesReport(null);
  }, []);

  // VALIDACIÓN: Puede generar reportes
  const canGenerateReport = Boolean(cutoffDate && selectedFile);

  // EFECTO: Log de debug del estado del componente
  useEffect(() => {
    console.log('🔍 ====== ESTADO COMPONENTE REPORT ======');
    console.log('   selectedFile:', selectedFile);
    console.log('   lastProcessedFile:', lastProcessedFileRef.current);
    console.log('   isFirstReport:', isFirstReportRef.current);
    console.log('   accumulatedSources:', accumulatedReport?.sources);
    console.log('   accumulatedItems:', accumulatedReport?.items.length);
    console.log('=======================================');
  }, [selectedFile, accumulatedReport]);

  // RENDER: Estado inicial - sin reporte
  if (!hasAnyReport && !loadingReport && !showReport) {
    return (
      <Card className="temporal-report-card temporal-empty-state">
        <div className="temporal-empty-content">
          <CalendarOutlined className="temporal-empty-icon" />
          <div className="temporal-empty-text">
            <Text className="temporal-empty-title">Generar Reporte</Text>
            <Text type="secondary" className="temporal-empty-description">
              Analiza las columnas con palabras clave y filtros geográficos
            </Text>

            {!cutoffDate && (
              <Text type="danger" style={{ display: 'block', marginTop: 8, fontSize: 12 }}>
                ⚠️ Debe seleccionar una fecha de corte antes de generar el reporte
              </Text>
            )}
            {cutoffDate && (
              <Text type="success" style={{ display: 'block', marginTop: 8, fontSize: 12 }}>
                ✓ Fecha de corte seleccionada: {cutoffDate}
              </Text>
            )}
          </div>
          <Button
            type="primary"
            icon={<BarChartOutlined />}
            onClick={handleLoadReport}
            className="temporal-generate-button"
            size="large"
            disabled={!canGenerateReport}
            title={!canGenerateReport ? "Seleccione una fecha de corte primero" : "Generar reporte"}
          >
            Generar Reporte Ahora
          </Button>
        </div>

        {/* ✅ AlertModal también disponible en el estado inicial (sin reporte) */}
        <AlertModal
          open={alertOpen}
          title={alertTitle}
          message={alertMessage}
          variant={alertVariant}
          onClose={() => setAlertOpen(false)}
        />
      </Card>
    );
  }

  const hasGeoFilters = Boolean(
    geographicFilters.departamento ||
    geographicFilters.municipio ||
    geographicFilters.ips ||
    selectedRegimen
  );

  // Usar reporte acumulado si existe, sino usar el reporte normal
  const displayReport = accumulatedReport || keywordReport;

  // RENDER: Contenido principal
  return (
    <>
      <Card
        className="temporal-report-card"
        title={
          <ReportHeader
            reportTitle={reportTitle}
            hasGeoFilters={hasGeoFilters}
            geographicFilters={geographicFilters}
            hasReport={hasAnyReport}
            loadingReport={loadingReport}
          />
        }
        extra={
          <ReportControls
            hasReport={hasAnyReport}
            reportTotalRecords={accumulatedReport?.total_rows || reportTotalRecords}
            showTemporalData={showTemporalData}
            showReport={showReport}
            onSetShowTemporalData={onSetShowTemporalData}
            onToggleReportVisibility={onToggleReportVisibility}
          />
        }
      >
        {loadingReport ? (
          <ReportLoading />
        ) : showReport ? (
          <div className="temporal-report-content">
            {/* 🔥 Indicador de reporte acumulado - MÚLTIPLES ARCHIVOS */}
            {accumulatedReport && accumulatedReport.sources.length > 1 && (
              <Card
                size="small"
                style={{
                  marginBottom: 16,
                  backgroundColor: '#e6f7ff',
                  borderColor: '#1890ff',
                  borderWidth: 2
                }}
              >
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Space>
                    <FileOutlined style={{ color: '#1890ff', fontSize: 18 }} />
                    <Text strong style={{ color: '#1890ff', fontSize: 14 }}>
                      📊 Reporte Acumulado - {accumulatedReport.sources.length} archivos combinados
                    </Text>
                  </Space>

                  <Space wrap>
                    {accumulatedReport.sources.map((source, idx) => (
                      <Tag key={idx} color="blue" icon={<FileOutlined />}>
                        {source}
                      </Tag>
                    ))}
                  </Space>

                  <Space style={{ width: '100%', justifyContent: 'space-between', marginTop: 8 }}>
                    <Space direction="vertical" size={0}>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        Total de items acumulados: <Text strong style={{ color: '#1890ff' }}>{accumulatedReport.total_rows}</Text>
                      </Text>
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        💡 Seleccione otro archivo y presione "Actualizar Reporte" para seguir acumulando
                      </Text>
                    </Space>

                    <Tooltip title="Limpiar todo y empezar de cero">
                      <Button
                        size="small"
                        icon={<ClearOutlined />}
                        onClick={handleClearAccumulatedReport}
                        danger
                      >
                        Limpiar Todo
                      </Button>
                    </Tooltip>
                  </Space>
                </Space>
              </Card>
            )}

            {/* Info de archivo único con tip de cómo acumular */}
            {accumulatedReport && accumulatedReport.sources.length === 1 && (
              <Card
                size="small"
                style={{
                  marginBottom: 16,
                  backgroundColor: '#f6ffed',
                  borderColor: '#52c41a'
                }}
              >
                <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                  <Space direction="vertical" size={2}>
                    <Space>
                      <FileOutlined style={{ color: '#52c41a' }} />
                      <Text style={{ fontSize: 12 }}>
                        Archivo actual: <Text strong>{accumulatedReport.sources[0]}</Text>
                      </Text>
                      <Tag color="green">{accumulatedReport.total_rows} items</Tag>
                    </Space>
                    <Text type="secondary" style={{ fontSize: 11, marginLeft: 24 }}>
                      💡 <Text strong>Para acumular:</Text> Seleccione otro archivo de la grilla superior y presione "Actualizar Reporte"
                    </Text>
                  </Space>

                  <Tooltip title="Reiniciar para generar desde cero">
                    <Button
                      size="small"
                      icon={<ClearOutlined />}
                      onClick={handleClearAccumulatedReport}
                    >
                      Reiniciar
                    </Button>
                  </Tooltip>
                </Space>
              </Card>
            )}

            {/* Filtros geográficos */}
            <GeographicFilters
              filters={geographicFilters}
              options={{
                departamentos: departamentosOptions,
                municipios: municipiosOptions,
                ips: ipsOptions
              }}
              loading={loadingGeoFilters}
              onDepartamentoChange={onDepartamentoChange}
              onMunicipioChange={onMunicipioChange}
              onIpsChange={onIpsChange}
              onReset={resetGeographicFilters}
              disabled={loadingReport}
            />

            {/* Selector de Régimen */}
            <Card
              size="small"
              title={
                <Space>
                  <MedicineBoxOutlined />
                  <span>Filtro de Régimen</span>
                </Space>
              }
              style={{ marginBottom: 16 }}
            >
              <Space direction="vertical" style={{ width: '100%' }}>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  Filtre el reporte por tipo de régimen de afiliación
                </Text>

                <Select
                  value={selectedRegimen || 'todos'}
                  onChange={handleRegimenChange}
                  style={{ width: '100%' }}
                  disabled={loadingReport}
                  size="large"
                >
                  <Option value="todos">
                    <Space>
                      <MedicineBoxOutlined />
                      <span>Todos los regímenes</span>
                    </Space>
                  </Option>
                  <Option value="Subsidiado">
                    <Space>
                      <MedicineBoxOutlined style={{ color: '#52c41a' }} />
                      <span>Subsidiado</span>
                    </Space>
                  </Option>
                  <Option value="Contributivo">
                    <Space>
                      <MedicineBoxOutlined style={{ color: '#1890ff' }} />
                      <span>Contributivo</span>
                    </Space>
                  </Option>
                </Select>

                {selectedRegimen && (
                  <div style={{
                    padding: '8px 12px',
                    backgroundColor: selectedRegimen === 'Subsidiado' ? '#f6ffed' : '#e6f7ff',
                    borderLeft: `3px solid ${selectedRegimen === 'Subsidiado' ? '#52c41a' : '#1890ff'}`,
                    borderRadius: 4,
                    marginTop: 8
                  }}>
                    <Text style={{ fontSize: 12 }}>
                      <MedicineBoxOutlined style={{ marginRight: 6 }} />
                      Filtrando por régimen: <Text strong>{selectedRegimen}</Text>
                    </Text>
                  </div>
                )}
              </Space>
            </Card>

            {/* Controles de keywords */}
            <KeywordControls
              reportKeywords={reportKeywords}
              hasReport={hasReport}
              loadingReport={loadingReport}
              onSetReportKeywords={onSetReportKeywords}
              onRegenerateReport={handleRegenerateReport}
            />

            {/* Alerta de sin resultados */}
            {!hasAnyReport && (
              <NoResultsAlert
                onRetry={handleRegenerateReport}
                loading={loadingReport}
              />
            )}

            {/* Estadísticas de keywords */}
            <KeywordStatistics stats={keywordStats} />

            {/* Tabla de reporte o estado vacío */}
            {hasAnyReport ? (
              <>
                <ReportTable
                  keywordReport={displayReport}
                  showTemporalData={showTemporalData}
                  filename={selectedFile || undefined}
                  selectedKeywords={reportKeywords}
                  geographicFilters={geographicFilters}
                  cutoffDate={cutoffDate}
                />

                {/* Botón para generar reporte de inasistentes */}
                {selectedFile && cutoffDate && (
                  <Card style={{ marginTop: 24 }} bodyStyle={{ padding: '16px' }}>
                    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                      <div style={{ textAlign: 'center' }}>
                        <UserDeleteOutlined
                          style={{
                            fontSize: 48,
                            color: showInasistentesReport ? '#ff4d4f' : '#8c8c8c',
                            marginBottom: 12
                          }}
                        />
                        <Text strong style={{ display: 'block', fontSize: 16, marginBottom: 8 }}>
                          Reporte de Inasistentes
                        </Text>
                        <Text type="secondary" style={{ display: 'block', fontSize: 12, marginBottom: 16 }}>
                          Genera un reporte detallado mes a mes de personas que no han asistido a consultas
                        </Text>

                        <Space>
                          {!showInasistentesReport ? (
                            <Button
                              type="primary"
                              icon={<UserDeleteOutlined />}
                              onClick={handleGenerateInasistentesReport}
                              loading={loadingInasistentes}
                              size="large"
                              style={{ backgroundColor: '#ff4d4f', borderColor: '#ff4d4f' }}
                            >
                              Generar Reporte de Inasistentes
                            </Button>
                          ) : (
                            <>
                              <Button
                                type="default"
                                onClick={handleGenerateInasistentesReport}
                                loading={loadingInasistentes}
                              >
                                Actualizar Reporte
                              </Button>
                              <Button
                                type="default"
                                onClick={handleHideInasistentesReport}
                                disabled={loadingInasistentes}
                              >
                                Ocultar Reporte
                              </Button>
                            </>
                          )}
                        </Space>

                        <div style={{ marginTop: 12 }}>
                          <Space split="|" size="small">
                            {cutoffDate && (
                              <Text type="secondary" style={{ fontSize: 11 }}>
                                📅 Fecha: <Text strong>{cutoffDate}</Text>
                              </Text>
                            )}
                            {selectedRegimen && (
                              <Text type="secondary" style={{ fontSize: 11 }}>
                                🏥 Régimen: <Text strong>{selectedRegimen}</Text>
                              </Text>
                            )}
                            {!selectedRegimen && (
                              <Text type="secondary" style={{ fontSize: 11 }}>
                                🏥 Régimen: <Text strong>Todos</Text>
                              </Text>
                            )}
                          </Space>
                        </div>
                      </div>
                    </Space>
                  </Card>
                )}

                {/* Alerta de fecha de corte requerida */}
                {selectedFile && !cutoffDate && (
                  <Card
                    style={{
                      marginTop: 24,
                      backgroundColor: '#fff7e6',
                      border: '1px solid #ffd591'
                    }}
                  >
                    <div style={{ textAlign: 'center', padding: '20px' }}>
                      <CalendarOutlined style={{ fontSize: 48, color: '#fa8c16', marginBottom: 16 }} />
                      <Text strong style={{ display: 'block', fontSize: 16, color: '#d46b08' }}>
                        Fecha de Corte Requerida
                      </Text>
                      <Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
                        Debe seleccionar una fecha de corte en la sección principal para generar reportes de inasistentes
                      </Text>
                    </div>
                  </Card>
                )}

                {/* Tabla de inasistentes */}
                {selectedFile && showInasistentesReport && cutoffDate && (
                  <div style={{ marginTop: 24 }}>
                    <InasistentesTable
                      reportData={inasistentesReport}
                      loading={loadingInasistentes}
                      cutoffDate={cutoffDate}
                      selectedRegimen={selectedRegimen}
                    />
                  </div>
                )}
              </>
            ) : (
              <NoReportState
                onGenerateReport={handleLoadReport}
                reportKeywords={reportKeywords}
                loadingReport={loadingReport}
              />
            )}
          </div>
        ) : null}
      </Card>

      <AlertModal
        open={alertOpen}
        title={alertTitle}
        message={alertMessage}
        variant={alertVariant}
        onClose={() => setAlertOpen(false)}
      />
    </>
  );
});

Report.displayName = 'Report';
export default Report;
