// components/technical-note/report/ReportTable.tsx - ✅ VERSIÓN COMPLETA CORREGIDA
import { memo, useMemo, useCallback, useState } from 'react';
import { Table, Typography, Tag, Space, Button, Tooltip, Empty, Row, Col, Divider, message, Modal, Dropdown } from 'antd';
import type { MenuProps } from 'antd';
import { 
  CalendarOutlined, 
  ExpandAltOutlined, 
  CompressOutlined,
  DownloadOutlined,
  FileTextOutlined,
  FilePdfOutlined,
  ExportOutlined,
  CloudDownloadOutlined,
  LoadingOutlined,
  DownOutlined,
  DatabaseOutlined,
  TableOutlined,
  BarChartOutlined,
  PieChartOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { KeywordAgeReportItem } from '../../../services/TechnicalNoteService';
import { TechnicalNoteService } from '../../../services/TechnicalNoteService';

const { Text, Title } = Typography;

/* ──────────────────────────── utilidades ──────────────────────────── */
const normalizeSpaces = (s: string) => s.trim().replace(/\s+/g, ' ');
const normalizeAge = (s: string) =>
  s === 'Sin especificar' ? s : s.replace(/months?/gi, 'meses').replace(/years?/gi, 'años');
const makeKey = (c: string, k: string, a: string) =>
  `${normalizeSpaces(c)}|${normalizeSpaces(k)}|${normalizeAge(a)}`;

const KEYWORD_COLORS: Record<string, string> = {
  medicina: 'blue', enfermeria: 'green', odontologia: 'purple', psicologia: 'orange',
  nutricion: 'cyan', fisioterapia: 'magenta', vacunacion: 'geekblue',
  crecimiento: 'gold', desarrollo: 'lime'
};
const kwColor = (k: string) => KEYWORD_COLORS[k.toLowerCase()] || 'default';

/* 🚦 FUNCIÓN PARA ESTILIZAR SOLO LA CELDA DE SEMAFORIZACIÓN */
const getSemaforizacionCellStyle = (color?: string, estado?: string) => {
  // Usar color del backend si está disponible
  const mainColor = color || '#6c757d'; // fallback gris
  
  // Generar color de fondo más suave para la celda
  const hex = mainColor.replace('#', '');
  const r = parseInt(hex.substr(0, 2), 16);
  const g = parseInt(hex.substr(2, 2), 16);
  const b = parseInt(hex.substr(4, 2), 16);
  const lightBg = `rgba(${r}, ${g}, ${b}, 0.15)`; // 15% transparencia para fondo suave
  
  return {
    backgroundColor: lightBg,
    color: mainColor,
    border: `2px solid ${mainColor}`,
    padding: '6px 10px',
    borderRadius: '6px',
    fontWeight: 600,
    fontSize: '11px',
    textAlign: 'center' as const,
    whiteSpace: 'nowrap' as const,
    display: 'inline-block',
    minWidth: '90px',
    boxShadow: `0 1px 3px ${mainColor}30`
  };
};

const findTemporal = (data: Record<string, any>, col: string, kw: string, age: string) => {
  const keys = [
    makeKey(col, kw, age),
    makeKey(col, kw, 'Sin especificar'),
    ...Object.keys(data).filter(k => k.includes(col) && k.includes(kw))
  ];
  for (const k of keys) if (data[k]) return { key: k, data: data[k] };
  return null;
};

/* ──────────────────────────── props ──────────────────────────── */
interface Props {
  keywordReport: {
    items: KeywordAgeReportItem[];
    totals_by_keyword: Record<string, any>;
    temporal_data?: Record<string, any>;
    filename?: string;
    geographic_filters?: Record<string, any>;
    corte_fecha?: string;
    global_statistics?: Record<string, any>;
  } | null;
  showTemporalData: boolean;
  filename?: string;
  selectedKeywords?: string[];
  geographicFilters?: {
    departamento?: string;
    municipio?: string;
    ips?: string;
  };
  onExportStart?: () => void;
  onExportComplete?: (files: Record<string, string>) => void;
  onExportError?: (error: string) => void;
}

/* ──────────────────────────── COMPONENTE DE EXPORTACIÓN MEJORADO ──────────────────────────── */
interface ExportControlsProps {
  keywordReport: NonNullable<Props['keywordReport']>;
  filename: string;
  selectedKeywords: string[];
  geographicFilters: Props['geographicFilters'];
  onExportStart?: () => void;
  onExportComplete?: (files: Record<string, string>) => void;
  onExportError?: (error: string) => void;
}

const ExportControls = memo<ExportControlsProps>(({ 
  keywordReport, 
  filename, 
  selectedKeywords,
  geographicFilters,
  onExportStart,
  onExportComplete,
  onExportError
}) => {
  const [exportLoading, setExportLoading] = useState(false);
  const [csvLoading, setCsvLoading] = useState<string | null>(null);
  const [exportModalVisible, setExportModalVisible] = useState(false);
  const [exportOptions, setExportOptions] = useState({
    export_csv: true,
    export_pdf: true,
    include_temporal: true
  });

  // ✅ CORRECCIÓN: Preparar parámetros sin doble extensión
  const prepareExportRequest = useCallback(() => {
    // Limpiar extensión .csv del nombre base para evitar duplicaciones
    const cleanFilename = filename?.replace(/\.csv$/, '') || 'reporte';
    
    return {
      data_source: filename || keywordReport.filename || 'reporte.csv',
      filename: `${cleanFilename}_${new Date().toISOString().split('T')[0]}`,
      keywords: selectedKeywords.length > 0 ? selectedKeywords : undefined,
      min_count: 0,
      include_temporal: true,
      geographic_filters: geographicFilters ? {
        departamento: geographicFilters.departamento,
        municipio: geographicFilters.municipio,
        ips: geographicFilters.ips
      } : undefined,
      corte_fecha: keywordReport.corte_fecha || "2025-07-31"
    };
  }, [filename, keywordReport, selectedKeywords, geographicFilters]);

  // 📥 EXPORTACIÓN INDIVIDUAL POR TIPO CSV
  const handleExportCSVType = useCallback(async (csvType: string, csvLabel: string) => {
    try {
      setCsvLoading(csvType);
      onExportStart?.();
      
      message.loading({ content: `Generando ${csvLabel}...`, key: `export-${csvType}`, duration: 0 });
      
      const request = prepareExportRequest();
      
      // Generar reporte completo y obtener enlaces
      const result = await TechnicalNoteService.generateAndExportAdvancedReport(
        request,
        { export_csv: true, export_pdf: false, include_temporal: true }
      );
      
      if (result.success && result.download_links) {
        // Buscar el enlace específico del tipo CSV
        const csvKey = Object.keys(result.download_links).find(key => 
          key.includes('csv') && key.includes(csvType.toLowerCase())
        );
        
        if (csvKey && result.download_links[csvKey]) {
          // Descargar archivo específico con nombre correcto
          const downloadFilename = `${request.filename}_${csvType}.csv`;
          await TechnicalNoteService.downloadFromLink(
            result.download_links[csvKey], 
            downloadFilename
          );
          
          message.success({ content: `✅ ${csvLabel} descargado exitosamente`, key: `export-${csvType}` });
          onExportComplete?.({ [csvType]: 'descargado' });
        } else {
          throw new Error(`No se encontró el archivo ${csvLabel}`);
        }
      } else {
        throw new Error(result.message || 'Error generando el reporte');
      }
      
    } catch (error) {
      console.error(`❌ Error exportando ${csvLabel}:`, error);
      const errorMsg = error instanceof Error ? error.message : 'Error desconocido';
      message.error({ content: `❌ Error ${csvLabel}: ${errorMsg}`, key: `export-${csvType}` });
      onExportError?.(errorMsg);
    } finally {
      setCsvLoading(null);
    }
  }, [prepareExportRequest, onExportStart, onExportComplete, onExportError]);

  // 📄 EXPORTACIÓN SOLO PDF
  const handleExportPDF = useCallback(async () => {
    try {
      setExportLoading(true);
      onExportStart?.();
      
      message.loading({ content: 'Generando reporte PDF...', key: 'export-pdf', duration: 0 });
      
      const request = prepareExportRequest();
      
      const result = await TechnicalNoteService.generateAndExportAdvancedReport(
        request,
        { export_csv: false, export_pdf: true, include_temporal: true }
      );
      
      if (result.success && result.download_links?.pdf) {
        await TechnicalNoteService.downloadFromLink(
          result.download_links.pdf,
          `${request.filename}.pdf`
        );
        
        message.success({ content: '✅ Archivo PDF descargado exitosamente', key: 'export-pdf' });
        onExportComplete?.({ pdf: 'descargado' });
      } else {
        throw new Error(result.message || 'Error generando PDF');
      }
      
    } catch (error) {
      console.error('❌ Error exportando PDF:', error);
      const errorMsg = error instanceof Error ? error.message : 'Error desconocido';
      message.error({ content: `❌ Error PDF: ${errorMsg}`, key: 'export-pdf' });
      onExportError?.(errorMsg);
    } finally {
      setExportLoading(false);
    }
  }, [prepareExportRequest, onExportStart, onExportComplete, onExportError]);

  // 📥 EXPORTACIÓN COMPLETA (TODOS LOS ARCHIVOS)
  const handleCompleteExport = useCallback(async () => {
    try {
      setExportLoading(true);
      onExportStart?.();
      
      message.loading({ content: 'Generando reporte completo...', key: 'export-complete', duration: 0 });
      
      const request = prepareExportRequest();
      
      const result = await TechnicalNoteService.generateAndExportAdvancedReport(
        request,
        { export_csv: true, export_pdf: true, include_temporal: true }
      );
      
      if (result.success && result.download_links) {
        // Descargar todos los archivos generados con nombres correctos
        const downloadPromises = Object.entries(result.download_links).map(([type, link]) => {
          let downloadFilename: string;
          
          if (type.includes('pdf')) {
            downloadFilename = `${request.filename}.pdf`;
          } else {
            // Para CSV, extraer el tipo específico
            const csvType = type.replace('csv_', '');
            downloadFilename = `${request.filename}_${csvType}.csv`;
          }
          
          return TechnicalNoteService.downloadFromLink(link, downloadFilename);
        });
        
        await Promise.all(downloadPromises);
        
        const fileCount = Object.keys(result.download_links).length;
        message.success({ 
          content: `✅ ${fileCount} archivos descargados exitosamente`, 
          key: 'export-complete' 
        });
        
        onExportComplete?.({ complete: 'descargado' });
      } else {
        throw new Error(result.message || 'Error generando el reporte completo');
      }
      
    } catch (error) {
      console.error('❌ Error en exportación completa:', error);
      const errorMsg = error instanceof Error ? error.message : 'Error desconocido';
      message.error({ content: `❌ Error: ${errorMsg}`, key: 'export-complete' });
      onExportError?.(errorMsg);
    } finally {
      setExportLoading(false);
    }
  }, [prepareExportRequest, onExportStart, onExportComplete, onExportError]);

  // 🔧 EXPORTACIÓN AVANZADA CON OPCIONES
  const handleAdvancedExport = useCallback(async () => {
    try {
      setExportLoading(true);
      onExportStart?.();
      
      const selectedFormats = [];
      if (exportOptions.export_csv) selectedFormats.push('CSV');
      if (exportOptions.export_pdf) selectedFormats.push('PDF');
      
      if (selectedFormats.length === 0) {
        message.warning('Selecciona al menos un formato para exportar');
        setExportLoading(false);
        return;
      }
      
      message.loading({ 
        content: `Generando reporte en ${selectedFormats.join(' y ')}...`, 
        key: 'export-advanced', 
        duration: 0 
      });
      
      const request = prepareExportRequest();
      
      const result = await TechnicalNoteService.generateAndExportAdvancedReport(
        request,
        exportOptions
      );
      
      if (result.success && result.download_links) {
        // Descargar archivos seleccionados con nombres correctos
        const downloadPromises = Object.entries(result.download_links).map(([type, link]) => {
          let downloadFilename: string;
          
          if (type.includes('pdf')) {
            downloadFilename = `${request.filename}.pdf`;
          } else {
            const csvType = type.replace('csv_', '');
            downloadFilename = `${request.filename}_${csvType}.csv`;
          }
          
          return TechnicalNoteService.downloadFromLink(link, downloadFilename);
        });
        
        await Promise.all(downloadPromises);
        
        message.success({ 
          content: `✅ Reporte en ${selectedFormats.join(' y ')} descargado exitosamente`, 
          key: 'export-advanced' 
        });
        
        const files: Record<string, string> = {};
        if (exportOptions.export_csv) files.csv = 'descargado';
        if (exportOptions.export_pdf) files.pdf = 'descargado';
        
        onExportComplete?.(files);
        setExportModalVisible(false);
      } else {
        throw new Error(result.message || 'Error en exportación avanzada');
      }
      
    } catch (error) {
      console.error('❌ Error en exportación avanzada:', error);
      const errorMsg = error instanceof Error ? error.message : 'Error desconocido';
      message.error({ content: `❌ Error: ${errorMsg}`, key: 'export-advanced' });
      onExportError?.(errorMsg);
    } finally {
      setExportLoading(false);
    }
  }, [exportOptions, prepareExportRequest, onExportStart, onExportComplete, onExportError]);

  // 🔧 MENÚ DROPDOWN PARA CSV INDIVIDUAL
  const csvMenuItems: MenuProps['items'] = [
    {
      key: 'actividades',
      label: 'Actividades Principales',
      icon: <TableOutlined style={{ color: '#1890ff' }} />,
      onClick: () => handleExportCSVType('actividades', 'CSV Actividades'),
      disabled: csvLoading !== null
    },
    {
      key: 'estadisticas',
      label: 'Estadísticas Globales',
      icon: <BarChartOutlined style={{ color: '#52c41a' }} />,
      onClick: () => handleExportCSVType('estadisticas', 'CSV Estadísticas'),
      disabled: csvLoading !== null
    },
    {
      key: 'temporal',
      label: 'Análisis Temporal',
      icon: <CalendarOutlined style={{ color: '#fa8c16' }} />,
      onClick: () => handleExportCSVType('temporal', 'CSV Temporal'),
      disabled: csvLoading !== null
    },
    {
      key: 'totales',
      label: 'Totales por Palabra',
      icon: <PieChartOutlined style={{ color: '#722ed1' }} />,
      onClick: () => handleExportCSVType('totales', 'CSV Totales'),
      disabled: csvLoading !== null
    },
    { type: 'divider' },
    {
      key: 'all-csv',
      label: 'Descargar Todos los CSV',
      icon: <DatabaseOutlined style={{ color: '#f5222d' }} />,
      disabled: csvLoading !== null,
      onClick: async () => {
        try {
          setCsvLoading('all');
          onExportStart?.();
          
          message.loading({ content: 'Generando todos los CSV...', key: 'export-all-csv', duration: 0 });
          
          const request = prepareExportRequest();
          
          const result = await TechnicalNoteService.generateAndExportAdvancedReport(
            request,
            { export_csv: true, export_pdf: false, include_temporal: true }
          );
          
          if (result.success && result.download_links) {
            // Filtrar solo enlaces CSV
            const csvLinks = Object.entries(result.download_links).filter(([key]) => 
              key.includes('csv')
            );
            
            const downloadPromises = csvLinks.map(([type, link]) => {
              const csvType = type.replace('csv_', '');
              return TechnicalNoteService.downloadFromLink(link, `${request.filename}_${csvType}.csv`);
            });
            
            await Promise.all(downloadPromises);
            
            message.success({ 
              content: `✅ ${csvLinks.length} archivos CSV descargados`, 
              key: 'export-all-csv' 
            });
            
            onExportComplete?.({ csv: 'todos descargados' });
          } else {
            throw new Error(result.message || 'Error generando CSV');
          }
          
        } catch (error) {
          console.error('❌ Error exportando todos los CSV:', error);
          const errorMsg = error instanceof Error ? error.message : 'Error desconocido';
          message.error({ content: `❌ Error CSV: ${errorMsg}`, key: 'export-all-csv' });
          onExportError?.(errorMsg);
        } finally {
          setCsvLoading(null);
        }
      }
    }
  ];

  // 📊 MOSTRAR RESUMEN DE DATOS
  const totalItems = keywordReport.items?.length || 0;
  const globalStats = keywordReport.global_statistics;
  const hasNumeradorDenominador = keywordReport.items?.some(item => 
    item.numerador !== undefined && item.denominador !== undefined
  );

  return (
    <>
      <div style={{ 
        background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
        padding: '16px',
        borderRadius: '8px',
        marginBottom: '16px',
        border: '1px solid #e8e8e8'
      }}>
        <Row gutter={[16, 12]} align="middle">
          {/* 📊 RESUMEN DE DATOS */}
          <Col flex="auto">
            <Space direction="vertical" size={4}>
              <Title level={5} style={{ margin: 0, color: '#1890ff' }}>
                📊 Reporte de Indicadores - {totalItems} actividades
              </Title>
              
              <Space size={16}>
                <Text strong>
                  📅 Corte: {keywordReport.corte_fecha || '2025-07-31'}
                </Text>
                
                {selectedKeywords.length > 0 && (
                  <Text>
                    🔍 Filtros: {selectedKeywords.join(', ')}
                  </Text>
                )}
                
                {hasNumeradorDenominador && globalStats && (
                  <Text strong style={{ color: '#52c41a' }}>
                    📈 Cobertura Global: {globalStats.cobertura_global_porcentaje?.toFixed(1) || '0.0'}%
                  </Text>
                )}
              </Space>
              
              {geographicFilters?.departamento && (
                <Text type="secondary">
                  🗺️ {geographicFilters.departamento}
                  {geographicFilters.municipio && ` → ${geographicFilters.municipio}`}
                  {geographicFilters.ips && ` → ${geographicFilters.ips}`}
                </Text>
              )}
            </Space>
          </Col>

          {/* 🚀 BOTONES DE EXPORTACIÓN INDIVIDUALES */}
          <Col>
            <Space size={8}>
              {/* ✅ BOTÓN DROPDOWN CSV INDIVIDUAL */}
              <Dropdown
                menu={{ items: csvMenuItems }}
                trigger={['click']}
                disabled={totalItems === 0 || exportLoading}
              >
                <Button
                  icon={csvLoading ? <LoadingOutlined /> : <FileTextOutlined />}
                  loading={csvLoading !== null}
                  style={{ 
                    color: '#52c41a', 
                    borderColor: '#52c41a',
                    minWidth: '100px'
                  }}
                  disabled={totalItems === 0 || exportLoading}
                >
                  {csvLoading ? 'Generando...' : 'CSV'} <DownOutlined />
                </Button>
              </Dropdown>

              {/* ✅ BOTÓN PDF INDIVIDUAL */}
              <Tooltip title="Descargar reporte en PDF">
                <Button
                  icon={exportLoading ? <LoadingOutlined /> : <FilePdfOutlined />}
                  onClick={handleExportPDF}
                  loading={exportLoading}
                  disabled={totalItems === 0 || csvLoading !== null}
                  style={{ color: '#f5222d', borderColor: '#f5222d' }}
                >
                  PDF
                </Button>
              </Tooltip>
            </Space>
          </Col>
        </Row>

        {/* 📊 ESTADÍSTICAS ADICIONALES SI ESTÁN DISPONIBLES */}
        {hasNumeradorDenominador && globalStats && (
          <>
            <Divider style={{ margin: '12px 0' }} />
            <Row gutter={16}>
              <Col span={6}>
                <Text strong style={{ color: '#1890ff' }}>
                  📊 Denominador Total: {globalStats.total_denominador_global?.toLocaleString() || '0'}
                </Text>
              </Col>
              <Col span={6}>
                <Text strong style={{ color: '#52c41a' }}>
                  ✅ Numerador Total: {globalStats.total_numerador_global?.toLocaleString() || '0'}
                </Text>
              </Col>
              <Col span={6}>
                <Text strong style={{ color: '#fa8c16' }}>
                  🎯 Actividades 100%: {globalStats.actividades_100_pct_cobertura || '0'}
                </Text>
              </Col>
              <Col span={6}>
                <Text strong style={{ color: '#ff4d4f' }}>
                  ⚠️ Actividades 
                </Text>
              </Col>
            </Row>
          </>
        )}
      </div>

      {/* 🔧 MODAL DE EXPORTACIÓN AVANZADA COMPLETO */}
      <Modal
        title={
          <Space>
            <ExportOutlined style={{ color: '#1890ff' }} />
            <span>Opciones Avanzadas de Exportación</span>
          </Space>
        }
        open={exportModalVisible}
        onCancel={() => setExportModalVisible(false)}
        onOk={handleAdvancedExport}
        okText={exportLoading ? 'Exportando...' : 'Exportar Seleccionados'}
        cancelText="Cancelar"
        confirmLoading={exportLoading}
        width={600}
      >
        <div style={{ padding: '16px 0' }}>
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <div>
              <Text strong>Selecciona los formatos a exportar:</Text>
              <div style={{ marginTop: 8 }}>
                <Space direction="vertical">
                  <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={exportOptions.export_csv}
                      onChange={(e) => setExportOptions(prev => ({ ...prev, export_csv: e.target.checked }))}
                      style={{ marginRight: 8 }}
                    />
                    <FileTextOutlined style={{ color: '#52c41a', marginRight: 4 }} />
                    <Text>CSV (4 archivos: actividades, estadísticas, temporal, totales)</Text>
                  </label>
                  
                  <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={exportOptions.export_pdf}
                      onChange={(e) => setExportOptions(prev => ({ ...prev, export_pdf: e.target.checked }))}
                      style={{ marginRight: 8 }}
                    />
                    <FilePdfOutlined style={{ color: '#f5222d', marginRight: 4 }} />
                    <Text>PDF (formato profesional con gráficos y semaforización)</Text>
                  </label>
                </Space>
              </div>
            </div>

            <div>
              <Text strong>Opciones adicionales:</Text>
              <div style={{ marginTop: 8 }}>
                <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    checked={exportOptions.include_temporal}
                    onChange={(e) => setExportOptions(prev => ({ ...prev, include_temporal: e.target.checked }))}
                    style={{ marginRight: 8 }}
                  />
                  <CalendarOutlined style={{ color: '#1890ff', marginRight: 4 }} />
                  <Text>Incluir análisis temporal mensual y anual</Text>
                </label>
              </div>
            </div>

            <div style={{ 
              background: '#f0f9ff', 
              padding: '12px', 
              borderRadius: '6px',
              border: '1px solid #bae7ff'
            }}>
              <Text type="secondary" style={{ fontSize: '12px' }}>
                <strong>📋 Archivos CSV incluidos:</strong><br />
                • <strong>Actividades:</strong> Datos principales con numerador/denominador<br />
                • <strong>Estadísticas:</strong> Métricas globales y resúmenes<br />
                • <strong>Temporal:</strong> Análisis mensual y anual detallado<br />
                • <strong>Totales:</strong> Resumen por palabras clave<br /><br />
                
                <strong>📄 Archivo PDF incluye:</strong><br />
                • Semaforización automática por desempeño<br />
                • Numeradores y denominadores por rango de edad<br />
                • Lógica Excel para cálculo de denominadores<br />
                • Filtros geográficos aplicados<br />
                • Análisis de cobertura detallado
              </Text>
            </div>
          </Space>
        </div>
      </Modal>
    </>
  );
});

ExportControls.displayName = 'ExportControls';

/* ──────────────────────────── COMPONENTE PRINCIPAL ──────────────────────────── */
export const ReportTable = memo<Props>(({ 
  keywordReport, 
  showTemporalData,
  filename,
  selectedKeywords = [],
  geographicFilters,
  onExportStart,
  onExportComplete,
  onExportError
}) => {
  if (!keywordReport?.items?.length) {
    return (
      <div style={{ padding: 16, textAlign: 'center' }}>
        <Empty
          description={!keywordReport ? 'No hay datos de reporte disponibles'
                                      : 'No se encontraron elementos en el reporte'}
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  const { items, temporal_data = {}, totals_by_keyword } = keywordReport;
  const hasND = useMemo(() => items.some(i => i.numerador !== undefined && i.denominador !== undefined), [items]);
  
  // 🚦 VERIFICAR SI HAY DATOS DE SEMAFORIZACIÓN
  const hasSemaforizacion = useMemo(() => 
    items.some(i => i.semaforizacion !== undefined), [items]);

  /* ──────────────────── EXPANSIÓN TEMPORAL CON CELDA COLORIDA ──────────────────── */
  const expandedRowRender = useCallback((rec: KeywordAgeReportItem) => {
    if (!showTemporalData) return null;

    const res = findTemporal(temporal_data, rec.column ?? '', rec.keyword ?? '', rec.age_range ?? '');
    if (!res?.data?.years) {
      return (
        <div style={{ padding: 12, textAlign: 'center', color: '#999' }}>
          <CalendarOutlined /> Sin datos temporales
        </div>
      );
    }

    /* construir filas CON SEMAFORIZACIÓN EN CELDA */
    const rows: any[] = [];
    Object.entries(res.data.years)
      .sort(([a], [b]) => parseInt(b) - parseInt(a))
      .forEach(([year, y]: any) => {
        /* fila año */
        rows.push({
          key: `y-${year}`,
          period: year,
          num: y.total_num ?? 0,
          den: y.total_den ?? 0,
          pct: y.pct ?? 0,
          semaforizacion: y.semaforizacion ?? 'NA',
          color: y.color, // 🚦 COLOR DEL BACKEND
          descripcion: y.descripcion,
          isYear: true
        });
        
        /* filas mes */
        Object.entries(y.months ?? {})
          .sort(([, a]: any, [, b]: any) => a.month - b.month)
          .forEach(([mName, m]: any) => {
            rows.push({
              key: `m-${year}-${m.month}`,
              period: `${mName} ${year}`,
              num: m.num, 
              den: m.den, 
              pct: m.pct,
              semaforizacion: m.semaforizacion ?? 'NA',
              color: m.color, // 🚦 COLOR DEL BACKEND
              descripcion: m.descripcion,
              isYear: false
            });
          });
      });

    const cols = [
      { title: 'Período', dataIndex: 'period', width: 140,
        render: (t: string, r: any) => (
          <Space size={4}>
            <Tag color={r.isYear ? 'blue' : 'green'}>{r.isYear ? 'AÑO' : 'MES'}</Tag>
            <Text strong={r.isYear}>{t}</Text>
          </Space>
        )
      },
      { title: 'Denominador', dataIndex: 'den', width: 90, align:'right' as const,
        render: (v:number)=><Text style={{color:'#1890ff'}}>{v?.toLocaleString()}</Text> },
      { title: 'Numerador', dataIndex: 'num', width: 90, align:'right' as const,
        render: (v:number)=><Text style={{color:'#52c41a'}}>{v?.toLocaleString()}</Text> },
      { title: '% Cumplimiento', dataIndex:'pct', width: 100, align:'center' as const,
        render: (v:number)=><Text strong>{v?.toFixed(1)}%</Text> },
      { title: '🚦 Estado', dataIndex: 'semaforizacion', width: 130, align: 'center' as const,
        render: (estado: string, record: any) => (
          <Tooltip title={record.descripcion || estado}>
            <div style={getSemaforizacionCellStyle(record.color, estado)}>
              {estado}
            </div>
          </Tooltip>
        )
      }
    ];

    return (
      <div style={{ padding: 8, background: '#fafafa' }}>
        <Space size={4} style={{ marginBottom: 6 }}>
          <CalendarOutlined style={{ color:'#1890ff' }} />
          <Text strong>{rec.column}</Text>
        </Space>
        <Table
          columns={cols}
          dataSource={rows}
          size="small"
          pagination={false}
          scroll={{ y:180, x: 650 }}
          rowClassName={r=>r.isYear?'temporal-year-row':'temporal-month-row'}
        />
      </div>
    );
  }, [temporal_data, showTemporalData]);

  /* ──────────────────── COLUMNAS PRINCIPALES CON CELDA COLORIDA ──────────────────── */
  const columns: ColumnsType<KeywordAgeReportItem> = useMemo(() => {
    const base: ColumnsType<KeywordAgeReportItem> = [
      { title: <div style={{textAlign:'center'}}>Procedimiento/Consulta</div>,
        dataIndex:'column', width: 200,
        render:(t:string)=><Tooltip title={t}>
          <div style={{fontSize:12,fontWeight:500,lineHeight:1.2}}>{t}</div>
        </Tooltip>,
        sorter:(a,b)=> (a.column??'').localeCompare(b.column??'')
      },
      { title:<div style={{textAlign:'center'}}>Palabra Clave</div>,
        dataIndex:'keyword', width:90, align:'center',
        render:(k:string)=><Tag color={kwColor(k)} style={{fontSize:10,fontWeight:500}}>{k?.toUpperCase()}</Tag>
      }
    ];
    
    if (hasND){
      base.push(
        { title:<div style={{textAlign:'center'}}>Denominador</div>, dataIndex:'denominador',
          width:85,align:'center',
          render:(v:number)=><Text style={{color:'#1890ff',fontSize:11}}>{v?.toLocaleString()}</Text>,
          sorter:(a,b)=>(a.denominador??0)-(b.denominador??0)
        },
        { title:<div style={{textAlign:'center'}}>Numerador</div>, dataIndex:'numerador',
          width:85,align:'center',
          render:(v:number)=><Text style={{color:'#52c41a',fontSize:11}}>{v?.toLocaleString()}</Text>,
          sorter:(a,b)=>(a.numerador??0)-(b.numerador??0)
        }
      );
    }
    
    base.push({
      title:<div style={{textAlign:'center'}}>% Cumplimiento</div>,
      width:100,align:'center',
      render:(_:any,r:KeywordAgeReportItem)=>{
        let pct:number;
        if(hasND && r.cobertura_porcentaje!==undefined) pct=r.cobertura_porcentaje;
        else{
          const tot=Object.values(totals_by_keyword??{}).reduce((a:any,c:any)=>a+(c.count||0),0);
          pct=tot? (r.count||0)/tot*100:0;
        }
        const color = pct>=70?'#52c41a': pct>=50?'#fa8c16':'#ff4d4f';
        return <Text strong style={{color,fontSize:11}}>{pct.toFixed(1)}%</Text>;
      },
      sorter: (a, b) => {
        const getPct = (r: KeywordAgeReportItem) => {
          if (hasND && r.cobertura_porcentaje !== undefined) return r.cobertura_porcentaje;
          const tot = Object.values(totals_by_keyword ?? {}).reduce((acc: any, c: any) => acc + (c.count || 0), 0);
          return tot ? (r.count || 0) / tot * 100 : 0;
        };
        return getPct(a) - getPct(b);
      }
    });

    // 🚦 AGREGAR COLUMNA DE SEMAFORIZACIÓN - SOLO CELDA COLORIDA
    if (hasSemaforizacion) {
      base.push({
        title: <div style={{textAlign:'center'}}>🚦 Estado</div>,
        dataIndex: 'semaforizacion',
        width: 140,
        align: 'center',
        render: (estado: string, record: KeywordAgeReportItem) => (
          <Tooltip title={record.descripcion || estado}>
            <div style={getSemaforizacionCellStyle(record.color, estado)}>
              {estado}
            </div>
          </Tooltip>
        ),
        sorter: (a, b) => {
          const estadoOrder = ['Óptimo', 'Aceptable', 'Deficiente', 'Muy Deficiente', 'NA', 'Error'];
          const aIndex = estadoOrder.indexOf(a.semaforizacion || 'NA');
          const bIndex = estadoOrder.indexOf(b.semaforizacion || 'NA');
          return aIndex - bIndex;
        }
      });
    }
    
    return base;
  }, [hasND, totals_by_keyword, hasSemaforizacion]);

  /* ──────────────────── helpers ──────────────────── */
  const rowExpandable = useCallback((r:KeywordAgeReportItem)=>
      !!findTemporal(temporal_data,r.column??'',r.keyword??'',r.age_range??'')?.data?.years,
    [temporal_data]);

  const rowKey = useCallback((r:KeywordAgeReportItem)=>
      `${r.column}-${r.keyword}-${r.age_range}`,[]);

  /* ──────────────────── render ──────────────────── */
  return (
    <>
      {/* 🚦 ESTILOS CSS BÁSICOS - SIN COLORES DE FILA */}
      <style>{`
        .temporal-year-row { background-color: #e6f7ff !important; font-weight: 500; }
        .temporal-month-row { background-color: #f6ffed !important; }
        
        .compact-table .ant-table-tbody > tr > td {
          padding: 6px 8px !important;
        }
        
        .compact-table .ant-table-thead > tr > th {
          padding: 8px 8px !important;
          font-size: 11px !important;
          font-weight: 600 !important;
        }
        
        .compact-table .ant-table-tbody > tr:hover > td {
          background-color: rgba(24, 144, 255, 0.05) !important;
        }
      `}</style>

      {/* 🚀 CONTROLES DE EXPORTACIÓN */}
      <ExportControls
        keywordReport={keywordReport}
        filename={filename || keywordReport.filename || 'reporte'}
        selectedKeywords={selectedKeywords}
        geographicFilters={geographicFilters}
        onExportStart={onExportStart}
        onExportComplete={onExportComplete}
        onExportError={onExportError}
      />

      {/* 📊 TABLA PRINCIPAL */}
      <Table
        dataSource={items}
        columns={columns}
        rowKey={rowKey}
        size="small"
        tableLayout="fixed"
        scroll={{x: hasSemaforizacion ? 670 : 520, y:400}}
        expandable={showTemporalData?{
          expandedRowRender, rowExpandable,
          expandIcon:({expanded,onExpand,record})=> rowExpandable(record)
            ? (<Button type="text" size="small"
                       icon={expanded?<CompressOutlined/>:<ExpandAltOutlined/>}
                       onClick={e=>onExpand(record,e)}
                       style={{padding:0,fontSize:10,color:'#1890ff'}}/>)
            : <span style={{width:12,display:'inline-block'}}/>
        }:undefined}
        pagination={{pageSize:20,showSizeChanger:true,simple:true}}
        locale={{emptyText:<Empty description="Sin datos" image={Empty.PRESENTED_IMAGE_SIMPLE}/>}}
        className="compact-table"
        style={{fontSize:11}}
      />
    </>
  );
});

ReportTable.displayName = 'ReportTable';
