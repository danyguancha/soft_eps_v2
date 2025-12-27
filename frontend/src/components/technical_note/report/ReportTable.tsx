// components/technical-note/report/ReportTable.tsx

import { memo, useMemo, useCallback, useState } from 'react';
import { Table, Typography, Tag, Space, Button, Tooltip, Empty, Row, Col, message } from 'antd';
import api from '../../../Api';
import {
  CalendarOutlined,
  FileExcelOutlined,
  FilePdfOutlined,
  LoadingOutlined
} from '@ant-design/icons';
import type { ColumnsType, ColumnGroupType } from 'antd/es/table';
import { TechnicalNoteService } from '../../../services/TechnicalNoteService';
import dayjs from 'dayjs';
import 'dayjs/locale/es';
import type { GeographicFilters } from '../../../interfaces/ITechnicalNote';
import './ReportTable.css';

dayjs.locale('es');

const { Text, Title } = Typography;

/* ──────────────────────────── CONSTANTES ──────────────────────────── */
const MESES_NOMBRES = [
  'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
  'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
];

const MESES_CAPITALIZADOS = [
  'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
  'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
];

const KEYWORD_COLORS: Record<string, string> = {
  medicina: 'blue',
  enfermeria: 'green',
  odontologia: 'purple',
  psicologia: 'orange',
  nutricion: 'cyan',
  fisioterapia: 'magenta',
  vacunacion: 'geekblue',
  crecimiento: 'gold',
  desarrollo: 'lime'
};

const kwColor = (k: string | undefined | null): string => {
  if (!k || typeof k !== 'string') return 'default';
  return KEYWORD_COLORS[k.toLowerCase()] || 'default';
};

const getSemaforizacionStyle = (color?: string): React.CSSProperties => {
  const mainColor = color || '#6c757d';
  return {
    backgroundColor: mainColor,
    color: '#fff',
    padding: '4px 8px',
    borderRadius: '4px',
    fontWeight: 600,
    fontSize: '9px',
    textAlign: 'center',
    whiteSpace: 'nowrap',
    display: 'inline-block',
    minWidth: '80px'
  };
};

/* ──────────────────────────── INTERFACES ──────────────────────────── */
interface MensualData {
  poblacion_objeto: number;
  numerador: number;
  denominador: number;
  cobertura: number;
  semaforizacion?: string;
  color?: string;
}

interface ConsolidadoData {
  poblacion_objeto: number;
  numerador: number;
  denominador: number;
  cobertura: number;
  semaforizacion?: string;
  color?: string;
}

interface ReportItem {
  // Columnas base
  consulta_procedimiento: string;
  rango_edad: string;
  poblacion_objeto: number;
  frecuencia_uso: number;
  poblacion_susceptible: number;
  meta: number;
  proyeccion_tiempo: number;
  valor_mensual: number;

  // Datos mensuales (enero, febrero, etc.)
  [key: string]: any;

  // Trimestres (T1, T2, T3, T4)
  T1?: ConsolidadoData;
  T2?: ConsolidadoData;
  T3?: ConsolidadoData;
  T4?: ConsolidadoData;

  // Semestres (S1, S2)
  S1?: ConsolidadoData;
  S2?: ConsolidadoData;

  // Anual
  anual?: ConsolidadoData;
}

interface Props {
  keywordReport: {
    items: ReportItem[];
    filename?: string;
    geographic_filters?: Record<string, any>;
    corte_fecha?: string;
    global_statistics?: Record<string, any>;
    meses_reportados?: number;
  } | null;
  showTemporalData: boolean;
  filename?: string;
  selectedKeywords?: string[];
  geographicFilters?: GeographicFilters;
  cutoffDate?: string;
  onExportStart?: () => void;
  onExportComplete?: (files: Record<string, string>) => void;
  onExportError?: (error: string) => void;
}

interface ExportControlsProps {
  keywordReport: NonNullable<Props['keywordReport']>;
  filename: string;
  selectedKeywords: string[];
  geographicFilters: Props['geographicFilters'];
  cutoffDate?: string;
  onExportStart?: () => void;
  onExportComplete?: (files: Record<string, string>) => void;
  onExportError?: (error: string) => void;
}

/* ──────────────────────────── EXPORTACIÓN ──────────────────────────── */
const ExportControls = memo<ExportControlsProps>(({
  keywordReport,
  filename,
  selectedKeywords,
  geographicFilters,
  cutoffDate,
  onExportStart,
  onExportComplete,
  onExportError
}) => {
  const [excelLoading, setExcelLoading] = useState(false);
  const [pdfLoading, setPdfLoading] = useState(false);

  const effectiveCutoffDate = cutoffDate || keywordReport.corte_fecha || "2025-07-31";

  const handleExportExcel = useCallback(async () => {
    try {
      setExcelLoading(true);
      onExportStart?.();

      message.loading({ content: 'Exportando Excel...', key: 'export-excel', duration: 0 });

      const cleanFilename = filename?.replace(/\.csv$/, '') || 'reporte';
      const timestampedFilename = `${cleanFilename}_${new Date().toISOString().split('T')[0]}`;

      const exportData = {
        report_data: keywordReport,
        filename: timestampedFilename,
        export_type: 'excel',
        export_options: {
          export_csv: true,
          export_pdf: false,
          include_detailed: true
        }
      };

      const response = await api.post(
        '/technical-note/reports/export-current',
        exportData,
        { timeout: 90000 }
      );

      const result = response.data;

      if (result.success && result.download_links) {
        // 🔥 Buscar 'excel' en lugar de 'csv'
        const excelLink = result.download_links.excel;

        if (excelLink) {
          await TechnicalNoteService.downloadFromLink(
            excelLink,
            `${timestampedFilename}.xlsx`
          );

          message.success({ content: '✅ Excel descargado', key: 'export-excel' });
          onExportComplete?.({ excel: 'descargado' });
        } else {
          console.error('Links disponibles:', result.download_links);
          throw new Error('No se encontró enlace Excel');
        }
      } else {
        throw new Error(result.message || 'Error en exportación');
      }

    } catch (error) {
      console.error('Error exportando Excel:', error);
      const errorMsg = error instanceof Error ? error.message : 'Error desconocido';
      message.error({ content: `❌ ${errorMsg}`, key: 'export-excel' });
      onExportError?.(errorMsg);
    } finally {
      setExcelLoading(false);
    }
  }, [keywordReport, filename, onExportStart, onExportComplete, onExportError]);

  const handleExportPDF = useCallback(async () => {
    try {
      setPdfLoading(true);
      onExportStart?.();

      message.loading({ content: 'Generando PDF...', key: 'export-pdf', duration: 0 });

      const cleanFilename = filename?.replace(/\.csv$/, '') || 'reporte';
      const timestampedFilename = `${cleanFilename}_${new Date().toISOString().split('T')[0]}`;

      const exportData = {
        report_data: keywordReport,
        filename: timestampedFilename,
        export_type: 'pdf',
        export_options: {
          export_csv: false,
          export_pdf: true,
          include_detailed: true
        }
      };

      const response = await api.post(
        '/technical-note/reports/export-current',
        exportData,
        { timeout: 120000 }
      );

      const result = response.data;

      if (result.success && result.download_links?.pdf) {
        await TechnicalNoteService.downloadFromLink(
          result.download_links.pdf,
          `${timestampedFilename}.pdf`
        );

        message.success({ content: '✅ PDF descargado', key: 'export-pdf' });
        onExportComplete?.({ pdf: 'descargado' });
      } else {
        throw new Error('Error generando PDF');
      }

    } catch (error) {
      console.error('Error exportando PDF:', error);
      const errorMsg = error instanceof Error ? error.message : 'Error desconocido';
      message.error({ content: `❌ ${errorMsg}`, key: 'export-pdf' });
      onExportError?.(errorMsg);
    } finally {
      setPdfLoading(false);
    }
  }, [keywordReport, filename, onExportStart, onExportComplete, onExportError]);

  const totalItems = keywordReport.items?.length || 0;

  return (
    <div className="export-controls-wrapper">
      <Row gutter={[12, 8]} align="middle">
        <Col flex="auto">
          <Space direction="vertical" size={4}>
            <Title level={4} className="export-controls-title">
              📊 Reporte de Indicadores - {totalItems} actividades
            </Title>

            <Space size={12} wrap>
              <div className="export-controls-date-badge">
                <Space size={3}>
                  <CalendarOutlined className="export-controls-date-icon" />
                  <Text strong className="export-controls-date-text">
                    Corte: {dayjs(effectiveCutoffDate).format('DD/MM/YYYY')}
                  </Text>
                </Space>
              </div>

              {keywordReport.meses_reportados && (
                <Tag color="green">
                  {keywordReport.meses_reportados} meses de análisis
                </Tag>
              )}

              {selectedKeywords.length > 0 && (
                <Text className="export-controls-filter-text">
                  🔍 Filtros: {selectedKeywords.join(', ')}
                </Text>
              )}
            </Space>

            {geographicFilters?.departamento && (
              <Text type="secondary" className="export-controls-geo-text">
                🗺️ {geographicFilters.departamento}
                {geographicFilters.municipio && ` → ${geographicFilters.municipio}`}
                {geographicFilters.ips && ` → ${geographicFilters.ips}`}
              </Text>
            )}
          </Space>
        </Col>

        <Col>
          <Space size={8}>
            <Button
              type="primary"
              icon={excelLoading ? <LoadingOutlined /> : <FileExcelOutlined />}
              onClick={handleExportExcel}
              loading={excelLoading}
              disabled={totalItems === 0 || pdfLoading}
            >
              {excelLoading ? 'Exportando...' : 'Descargar Excel'}
            </Button>

            <Button
              danger
              icon={pdfLoading ? <LoadingOutlined /> : <FilePdfOutlined />}
              onClick={handleExportPDF}
              loading={pdfLoading}
              disabled={totalItems === 0 || excelLoading}
            >
              Descargar PDF
            </Button>
          </Space>
        </Col>
      </Row>
    </div>
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
  cutoffDate,
  onExportStart,
  onExportComplete,
  onExportError
}) => {
  if (!keywordReport?.items?.length) {
    return (
      <Empty
        description="No hay datos de reporte disponibles"
        image={Empty.PRESENTED_IMAGE_SIMPLE}
      />
    );
  }

  const { items } = keywordReport;

  console.log('📊 ReportTable - Items recibidos:', items.length);
  console.log('📊 Estructura del primer item:', items[0]);

  // 🔥 Función helper para obtener datos mensuales
  const getMensualData = (item: ReportItem, mes: string): MensualData | null => {
    const mesData = item[mes];
    if (!mesData || typeof mesData !== 'object') return null;
    return mesData as MensualData;
  };

  // 🔥 Función helper para crear columna de mes
  const createMonthColumn = (mesIndex: number): ColumnGroupType<ReportItem> => {
    const mesNombre = MESES_NOMBRES[mesIndex];
    const mesCapitalizado = MESES_CAPITALIZADOS[mesIndex];

    return {
      title: mesCapitalizado,
      className: 'month-group-header',
      children: [
        {
          title: 'Numerador',
          dataIndex: `${mesNombre}`,
          key: `${mesNombre}_numerador`,
          width: 90,
          align: 'center',
          className: 'month-subheader-numerador',
          render: (_: any, record: ReportItem) => {
            const data = getMensualData(record, mesNombre);
            return (
              <span className="month-cell-numerador">
                {data?.numerador?.toLocaleString() || '0'}
              </span>
            );
          }
        },
        {
          title: 'Denominador',
          dataIndex: `${mesNombre}`,
          key: `${mesNombre}_denominador`,
          width: 90,
          align: 'center',
          className: 'month-subheader-denominador',
          render: (_: any, record: ReportItem) => {
            const data = getMensualData(record, mesNombre);
            return (
              <span className="month-cell-denominador">
                {data?.denominador?.toLocaleString() || '0'}
              </span>
            );
          }
        }
      ]
    };
  };

  // 🔥 Función helper para crear columna de trimestre
  const createTrimestreColumn = (trimNum: number): ColumnGroupType<ReportItem> => {
    const trimKey = `trim${trimNum}`;  // 🔥 trim1, trim2, trim3, trim4

    return {
      title: `Trimestre ${trimNum}`,
      className: 'consolidado-trimestre-header',
      children: [
        {
          title: 'Numerador',
          dataIndex: trimKey,
          key: `${trimKey}_Numerador`,
          width: 70,
          align: 'center',
          render: (_: any, record: ReportItem) => {
            const data = record[trimKey] as ConsolidadoData | undefined;
            return <Text>{data?.numerador?.toLocaleString() || '0'}</Text>;
          }
        },
        {
          title: 'Denominador',
          dataIndex: trimKey,
          key: `${trimKey}_Denominador`,
          width: 70,
          align: 'center',
          render: (_: any, record: ReportItem) => {
            const data = record[trimKey] as ConsolidadoData | undefined;
            return <Text>{data?.denominador?.toLocaleString() || '0'}</Text>;
          }
        },
        {
          title: '%',
          dataIndex: trimKey,
          key: `${trimKey}_cob`,
          width: 60,
          align: 'center',
          render: (_: any, record: ReportItem) => {
            const data = record[trimKey] as ConsolidadoData | undefined;
            if (!data) return '-';

            // 🔥 Si denominador = 0, mostrar 0.0%
            const cobertura = data.cobertura || 0;
            const color = data.denominador === 0
              ? '#808080'  // Gris si denominador = 0
              : cobertura >= 70 ? '#52c41a' : cobertura >= 50 ? '#fa8c16' : '#ff4d4f';

            return <Text strong style={{ color }}>{cobertura.toFixed(1)}%</Text>;
          }
        },
        {
          title: 'Calificación obtenida',
          dataIndex: trimKey,
          key: `${trimKey}_semaf`,
          width: 90,
          align: 'center',
          render: (_: any, record: ReportItem) => {
            const data = record[trimKey] as ConsolidadoData | undefined;
            if (!data?.semaforizacion) return '-';
            return <div style={getSemaforizacionStyle(data.color)}>{data.semaforizacion}</div>;
          }
        }
      ]
    };
  };

  // 🔥 Función helper para crear columna de semestre
  const createSemestreColumn = (semNum: number): ColumnGroupType<ReportItem> => {
    const semKey = `sem${semNum}`;  // 🔥 sem1, sem2

    return {
      title: `Semestre ${semNum}`,
      className: 'consolidado-semestre-header',
      children: [
        {
          title: 'Numerador',
          dataIndex: semKey,
          key: `${semKey}_Numerador`,
          width: 70,
          align: 'center',
          render: (_: any, record: ReportItem) => {
            const data = record[semKey] as ConsolidadoData | undefined;
            return <Text>{data?.numerador?.toLocaleString() || '0'}</Text>;
          }
        },
        {
          title: 'Denominador',
          dataIndex: semKey,
          key: `${semKey}_Denominador`,
          width: 70,
          align: 'center',
          render: (_: any, record: ReportItem) => {
            const data = record[semKey] as ConsolidadoData | undefined;
            return <Text>{data?.denominador?.toLocaleString() || '0'}</Text>;
          }
        },
        {
          title: '%',
          dataIndex: semKey,
          key: `${semKey}_cob`,
          width: 60,
          align: 'center',
          render: (_: any, record: ReportItem) => {
            const data = record[semKey] as ConsolidadoData | undefined;
            if (!data) return '-';

            // 🔥 Si denominador = 0, mostrar 0.0%
            const cobertura = data.cobertura || 0;
            const color = data.denominador === 0
              ? '#808080'  // Gris si denominador = 0
              : cobertura >= 70 ? '#52c41a' : cobertura >= 50 ? '#fa8c16' : '#ff4d4f';

            return <Text strong style={{ color }}>{cobertura.toFixed(1)}%</Text>;
          }
        },
        {
          title: 'Calificación obtenida',
          dataIndex: semKey,
          key: `${semKey}_semaf`,
          width: 90,
          align: 'center',
          render: (_: any, record: ReportItem) => {
            const data = record[semKey] as ConsolidadoData | undefined;
            if (!data?.semaforizacion) return '-';
            return <div style={getSemaforizacionStyle(data.color)}>{data.semaforizacion}</div>;
          }
        }
      ]
    };
  };
  // 🔥 Generar columnas
  const columns: ColumnsType<ReportItem> = useMemo(() => {
    const cols: ColumnsType<ReportItem> = [];

    // ✨ COLUMNAS FIJAS INICIALES
    cols.push({
      title: 'Procedimiento/Consulta',
      dataIndex: 'consulta_procedimiento',
      key: 'consulta_procedimiento',
      width: 250,
      fixed: 'left',
      render: (text: string) => (
        <Tooltip title={text}>
          <Text strong>{text || 'Sin especificar'}</Text>
        </Tooltip>
      )
    });

    cols.push({
      title: 'Rango Edad',
      dataIndex: 'rango_edad',
      key: 'rango_edad',
      width: 120,
      fixed: 'left',
      align: 'center',
      render: (text: string) => (
        <Text strong style={{ color: '#940ceeff', fontSize: '12px' }}>
          {text || '-'}
        </Text>
      )
    });

    cols.push({
      title: 'Cups',
      dataIndex: 'cups',
      key: 'cups',
      width: 120,
      align: 'center',
      render: (text: string) => (
        <Text strong style={{ color: '#940ceeff', fontSize: '12px' }}>
          {text || '-'}
        </Text>
      )
    });

    cols.push({
      title: 'Frecuencia indicada',
      dataIndex: 'frecuencia_indicada',
      key: 'frecuencia_indicada',
      width: 120,
      align: 'center',
      render: (text: string) => (
        <Text strong style={{ color: '#940ceeff', fontSize: '12px' }}>
          {text || '-'}
        </Text>
      )
    });

    cols.push({
      title: 'Población Objeto',
      dataIndex: 'poblacion_objeto',
      key: 'poblacion_objeto',
      width: 130,
      align: 'center',
      render: (val: number) => (
        <Text strong style={{ color: '#1890ff' }}>
          {val?.toLocaleString() || '0'}
        </Text>
      )
    });

    cols.push({
      title: 'Periodo',
      dataIndex: 'periodo',
      key: 'periodo',
      width: 120,
      align: 'center',
      render: (text: string) => (
        <Text strong style={{ color: '#940ceeff', fontSize: '12px' }}>
          {text || '-'}
        </Text>
      )
    });

    cols.push({
      title: 'Frecuencia uso_ips',
      dataIndex: 'frecuencia_uso_ips',
      key: 'frecuencia_uso_ips',
      width: 120,
      align: 'center',
      render: (val: number) => (
        <Text>{val?.toFixed(1) || '0'}</Text>
      )
    });

    cols.push({
      title: 'Frecuencia ajustada anual',
      dataIndex: 'fecuencia_ajustada_anual',
      key: 'fecuencia_ajustada_anual',
      width: 120,
      align: 'center',
      render: (val: number) => (
        <Text>{val?.toFixed(1) || '0'}</Text>
      )
    });

    cols.push({
      title: 'Meta',
      dataIndex: 'meta',
      key: 'meta',
      width: 80,
      align: 'center',
      render: (val: number) => (
        <Text strong style={{ color: '#52c41a' }}>
          {val?.toFixed(1) || '0'}
        </Text>
      )
    });

    cols.push({
      title: 'Pobl. Susceptible anual',
      dataIndex: 'poblacion_susceptible_anual',
      key: 'poblacion_susceptible_anual',
      width: 140,
      align: 'center',
      render: (val: number) => (
        <Text strong style={{ color: '#722ed1' }}>
          {val?.toLocaleString() || '0'}
        </Text>
      )
    });

    cols.push({
      title: 'Pobl. Susceptible mensual',
      dataIndex: 'poblacion_susceptible_mensual',
      key: 'poblacion_susceptible_mensual',
      width: 140,
      align: 'center',
      render: (val: number) => (
        <Text strong style={{ color: '#722ed1' }}>
          {val?.toLocaleString() || '0'}
        </Text>
      )
    });

    cols.push({
      title: 'Proyección Tiempo',
      dataIndex: 'proyeccion_tiempo',
      key: 'proyeccion_tiempo',
      width: 130,
      align: 'center',
      render: (val: number) => (
        <Text strong style={{ color: '#eb2f96' }}>
          {val?.toFixed(0) || '0'}
        </Text>
      )
    });


    // ✨ COLUMNAS MENSUALES Y CONSOLIDADOS
    if (showTemporalData) {
      // Enero, Febrero, Marzo → Trimestre 1
      for (let i = 0; i < 3; i++) cols.push(createMonthColumn(i));
      cols.push(createTrimestreColumn(1));

      // Abril, Mayo, Junio → Trimestre 2 + Semestre 1
      for (let i = 3; i < 6; i++) cols.push(createMonthColumn(i));
      cols.push(createTrimestreColumn(2));
      cols.push(createSemestreColumn(1));

      // Julio, Agosto, Septiembre → Trimestre 3
      for (let i = 6; i < 9; i++) cols.push(createMonthColumn(i));
      cols.push(createTrimestreColumn(3));

      // Octubre, Noviembre, Diciembre → Trimestre 4 + Semestre 2
      for (let i = 9; i < 12; i++) cols.push(createMonthColumn(i));
      cols.push(createTrimestreColumn(4));
      cols.push(createSemestreColumn(2));

      // Consolidado Anual (CORREGIDO)
      cols.push({
        title: 'Consolidado Anual',
        className: 'consolidado-anual-header',
        children: [
          {
            title: 'Numerador',
            dataIndex: 'anual',
            key: 'anual_Numerador',
            width: 80,
            align: 'center',
            render: (_: any, record: ReportItem) => {
              const data = record.anual;
              return <Text>{data?.numerador?.toLocaleString() || '0'}</Text>;
            }
          },
          {
            title: 'Denominador',
            dataIndex: 'anual',
            key: 'anual_Denominador',
            width: 80,
            align: 'center',
            render: (_: any, record: ReportItem) => {
              const data = record.anual;
              return <Text>{data?.denominador?.toLocaleString() || '0'}</Text>;
            }
          },
          {
            title: '%',
            dataIndex: 'anual',
            key: 'anual_cob',
            width: 70,
            align: 'center',
            render: (_: any, record: ReportItem) => {
              const data = record.anual;
              if (!data) return '-';

              // 🔥 Si denominador = 0, mostrar 0.0%
              const cobertura = data.cobertura || 0;
              const color = data.denominador === 0
                ? '#808080'  // Gris si denominador = 0
                : cobertura >= 70 ? '#52c41a' : cobertura >= 50 ? '#fa8c16' : '#ff4d4f';

              return <Text strong style={{ color, fontSize: '11px' }}>{cobertura.toFixed(1)}%</Text>;
            }
          },
          {
            title: 'Calificación obtenida',
            dataIndex: 'anual',
            key: 'anual_semaf',
            width: 100,
            align: 'center',
            render: (_: any, record: ReportItem) => {
              const data = record.anual;
              if (!data?.semaforizacion) return '-';
              return <div style={getSemaforizacionStyle(data.color)}>{data.semaforizacion}</div>;
            }
          }
        ]
      } as ColumnGroupType<ReportItem>);

    }

    return cols;
  }, [showTemporalData]);

  return (
    <>
      <ExportControls
        keywordReport={keywordReport}
        filename={filename || keywordReport.filename || 'reporte'}
        selectedKeywords={selectedKeywords}
        geographicFilters={geographicFilters}
        cutoffDate={cutoffDate}
        onExportStart={onExportStart}
        onExportComplete={onExportComplete}
        onExportError={onExportError}
      />

      <Table<ReportItem>
        dataSource={items}
        columns={columns}
        rowKey={(record, index) => `${record.consulta_procedimiento}-${index}`}
        size="small"
        scroll={{ x: showTemporalData ? 5500 : 1300, y: 500 }}
        pagination={{
          pageSize: 20,
          showSizeChanger: true,
          showTotal: (total) => `Total: ${total} actividades`,
          pageSizeOptions: ['10', '20', '50', '100']
        }}
        locale={{
          emptyText: <Empty description="Sin datos" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        }}
        className="ultra-compact-table"
        bordered
      />
    </>
  );
});

ReportTable.displayName = 'ReportTable';
