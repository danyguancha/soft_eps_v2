// components/technical-note/report/ReportTable.tsx
import { memo, useMemo, useCallback, useState } from 'react';
import { Table, Typography, Tag, Space, Button, Tooltip, Empty, Row, Col, message } from 'antd';
import api from '../../../Api';
import { 
  CalendarOutlined, 
  ExpandAltOutlined, 
  CompressOutlined,
  FileTextOutlined,
  FilePdfOutlined,
  LoadingOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { TechnicalNoteService } from '../../../services/TechnicalNoteService';
import dayjs from 'dayjs';
import 'dayjs/locale/es';
import type { GeographicFilters, KeywordAgeReportItem } from '../../../interfaces/ITechnicalNote';

dayjs.locale('es');

const { Text, Title } = Typography;

/* ──────────────────────────── UTILIDADES ──────────────────────────── */
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

const getSemaforizacionCellStyle = (color?: string, _?: string) => {
  const mainColor = color || '#6c757d';
  const hex = mainColor.replace('#', '');
  const r = parseInt(hex.substr(0, 2), 16);
  const g = parseInt(hex.substr(2, 2), 16);
  const b = parseInt(hex.substr(4, 2), 16);
  const lightBg = `rgba(${r}, ${g}, ${b}, 0.12)`;
  
  return {
    backgroundColor: lightBg,
    color: mainColor,
    border: `1px solid ${mainColor}`,
    padding: '2px 6px',
    borderRadius: '3px',
    fontWeight: 600,
    fontSize: '9px',
    textAlign: 'center' as const,
    whiteSpace: 'nowrap' as const,
    display: 'inline-block',
    minWidth: '70px',
    lineHeight: '1.2'
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

/* ──────────────────────────── PROPS ──────────────────────────── */
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
  geographicFilters?: GeographicFilters;
  cutoffDate?: string;
  onExportStart?: () => void;
  onExportComplete?: (files: Record<string, string>) => void;
  onExportError?: (error: string) => void;
}

/* ──────────────────────────── COMPONENTE DE EXPORTACIÓN SIMPLIFICADO ──────────────────────────── */
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
  const [csvLoading, setCsvLoading] = useState(false);
  const [pdfLoading, setPdfLoading] = useState(false);

  const effectiveCutoffDate = cutoffDate || keywordReport.corte_fecha || "2025-07-31";

  // EXPORTAR CSV TEMPORAL DIRECTAMENTE
  const handleExportCSV = useCallback(async () => {
    try {
      setCsvLoading(true);
      onExportStart?.();
      
      message.loading({ content: 'Exportando CSV...', key: 'export-csv', duration: 0 });

      const cleanFilename = filename?.replace(/\.csv$/, '') || 'reporte';
      const timestampedFilename = `${cleanFilename}_${new Date().toISOString().split('T')[0]}`;
      
      const exportData = {
        report_data: keywordReport,
        filename: timestampedFilename,
        export_type: 'temporal',
        export_options: {
          export_csv: true,
          export_pdf: false,
          include_temporal: true
        }
      };

      console.log('📤 Exportando CSV:', {
        items: keywordReport.items?.length,
        fecha: effectiveCutoffDate
      });

      const response = await api.post(
        '/technical-note/reports/export-current',
        exportData,
        { timeout: 90000 }
      );

      const result = response.data;

      if (result.success && result.download_links) {
        console.log('✅ Enlaces recibidos:', result.download_links);

        // Buscar el enlace CSV temporal
        const csvLink = result.download_links.csv_temporal || result.download_links.temporal;
        
        if (csvLink) {
          await TechnicalNoteService.downloadFromLink(
            csvLink,
            `${timestampedFilename}_temporal.csv`
          );
          
          message.success({ 
            content: 'CSV descargado exitosamente', 
            key: 'export-csv' 
          });
          
          onExportComplete?.({ csv: 'descargado' });
        } else {
          throw new Error('No se encontró el enlace de descarga CSV');
        }
      } else {
        throw new Error(result.message || 'Error en la exportación');
      }

    } catch (error) {
      console.error('❌ Error exportando CSV:', error);
      const errorMsg = error instanceof Error ? error.message : 'Error desconocido';
      message.error({ content: `❌ Error: ${errorMsg}`, key: 'export-csv' });
      onExportError?.(errorMsg);
    } finally {
      setCsvLoading(false);
    }
  }, [keywordReport, filename, effectiveCutoffDate, onExportStart, onExportComplete, onExportError]);

  // ✅ EXPORTAR PDF
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
          include_temporal: true
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
        
        message.success({ content: '✅ PDF descargado exitosamente', key: 'export-pdf' });
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
      setPdfLoading(false);
    }
  }, [keywordReport, filename, onExportStart, onExportComplete, onExportError]);

  const totalItems = keywordReport.items?.length || 0;

  return (
    <div style={{ 
      background: 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)',
      padding: '8px 10px',
      borderRadius: '4px',
      marginBottom: '8px',
      border: '1px solid #e8e8e8'
    }}>
      <Row gutter={[8, 6]} align="middle">
        <Col flex="auto">
          <Space direction="vertical" size={2}>
            <Title level={5} style={{ margin: 0, color: '#1890ff', fontSize: '13px', lineHeight: '1.3' }}>
              📊 Reporte de Indicadores - {totalItems} actividades
            </Title>
            
            <Space size={8}>
              <div style={{
                padding: '2px 6px',
                backgroundColor: '#e6f7ff',
                border: '1px solid #91d5ff',
                borderRadius: 3,
                fontSize: '10px'
              }}>
                <Space size={3}>
                  <CalendarOutlined style={{ color: '#1890ff', fontSize: '10px' }} />
                  <Text strong style={{ color: '#1890ff', fontSize: '10px' }}>
                    Corte: {dayjs(effectiveCutoffDate).format('DD/MM/YYYY')}
                  </Text>
                </Space>
              </div>
              
              {selectedKeywords.length > 0 && (
                <Text style={{ fontSize: '10px' }}>
                  🔍 Filtros: {selectedKeywords.join(', ')}
                </Text>
              )}
            </Space>
            
            {geographicFilters?.departamento && (
              <Text type="secondary" style={{ fontSize: '9px' }}>
                🗺️ {geographicFilters.departamento}
                {geographicFilters.municipio && ` → ${geographicFilters.municipio}`}
                {geographicFilters.ips && ` → ${geographicFilters.ips}`}
              </Text>
            )}
          </Space>
        </Col>

        <Col>
          <Space size={6}>
            {/* ✅ BOTÓN CSV DIRECTO SIN DROPDOWN */}
            <Tooltip title="Descargar análisis temporal en CSV">
              <Button
                icon={csvLoading ? <LoadingOutlined style={{ fontSize: '11px' }} /> : <FileTextOutlined style={{ fontSize: '11px' }} />}
                onClick={handleExportCSV}
                loading={csvLoading}
                size="small"
                disabled={totalItems === 0 || pdfLoading}
                style={{ 
                  color: '#52c41a', 
                  borderColor: '#52c41a',
                  fontSize: '11px',
                  height: '26px',
                  padding: '0 8px'
                }}
              >
                {csvLoading ? 'Exportando...' : 'Descarga CSV'}
              </Button>
            </Tooltip>

            {/* ✅ BOTÓN PDF */}
            <Tooltip title="Descargar reporte completo en PDF">
              <Button
                icon={pdfLoading ? <LoadingOutlined style={{ fontSize: '11px' }} /> : <FilePdfOutlined style={{ fontSize: '11px' }} />}
                onClick={handleExportPDF}
                loading={pdfLoading}
                size="small"
                disabled={totalItems === 0 || csvLoading}
                style={{ 
                  color: '#f5222d', 
                  borderColor: '#f5222d',
                  fontSize: '11px',
                  height: '26px',
                  padding: '0 8px'
                }}
              >
                Descargar PDF
              </Button>
            </Tooltip>
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
      <div style={{ padding: 12, textAlign: 'center' }}>
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
  
  const hasSemaforizacion = useMemo(() => 
    items.some(i => i.semaforizacion !== undefined), [items]);

  const expandedRowRender = useCallback((rec: KeywordAgeReportItem) => {
    if (!showTemporalData) return null;

    const res = findTemporal(temporal_data, rec.column ?? '', rec.keyword ?? '', rec.age_range ?? '');
    if (!res?.data?.years) {
      return (
        <div style={{ padding: 8, textAlign: 'center', color: '#999', fontSize: '10px' }}>
          <CalendarOutlined style={{ fontSize: '10px' }} /> Sin datos temporales
        </div>
      );
    }

    const rows: any[] = [];
    Object.entries(res.data.years)
      .sort(([a], [b]) => parseInt(b) - parseInt(a))
      .forEach(([year, y]: any) => {
        rows.push({
          key: `y-${year}`,
          period: year,
          num: y.total_num ?? 0,
          den: y.total_den ?? 0,
          pct: y.pct ?? 0,
          semaforizacion: y.semaforizacion ?? 'NA',
          color: y.color,
          descripcion: y.descripcion,
          isYear: true
        });
        
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
              color: m.color,
              descripcion: m.descripcion,
              isYear: false
            });
          });
      });

    const cols = [
      { title: 'Período', dataIndex: 'period', width: 120,
        render: (t: string, r: any) => (
          <Space size={2}>
            <Tag color={r.isYear ? 'blue' : 'green'} style={{ fontSize: '9px', padding: '0 4px', lineHeight: '16px' }}>
              {r.isYear ? 'AÑO' : 'MES'}
            </Tag>
            <Text strong={r.isYear} style={{ fontSize: '10px' }}>{t}</Text>
          </Space>
        )
      },
      { title: 'Denominador', dataIndex: 'den', width: 75, align:'right' as const,
        render: (v:number)=><Text style={{color:'#1890ff', fontSize: '10px'}}>{v?.toLocaleString()}</Text> },
      { title: 'Numerador', dataIndex: 'num', width: 75, align:'right' as const,
        render: (v:number)=><Text style={{color:'#52c41a', fontSize: '10px'}}>{v?.toLocaleString()}</Text> },
      { title: '% Cumplimiento', dataIndex:'pct', width: 85, align:'center' as const,
        render: (v:number)=><Text strong style={{ fontSize: '10px' }}>{v?.toFixed(1)}%</Text> },
      { title: '🚦 Estado', dataIndex: 'semaforizacion', width: 110, align: 'center' as const,
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
      <div style={{ padding: 6, background: '#fafafa' }}>
        <Space size={2} style={{ marginBottom: 4 }}>
          <CalendarOutlined style={{ color:'#1890ff', fontSize: '10px' }} />
          <Text strong style={{ fontSize: '10px' }}>{rec.column}</Text>
        </Space>
        <Table
          columns={cols}
          dataSource={rows}
          size="small"
          pagination={false}
          scroll={{ y:160, x: 600 }}
          rowClassName={r=>r.isYear?'temporal-year-row':'temporal-month-row'}
        />
      </div>
    );
  }, [temporal_data, showTemporalData]);

  const columns: ColumnsType<KeywordAgeReportItem> = useMemo(() => {
    const base: ColumnsType<KeywordAgeReportItem> = [
      { title: <div style={{textAlign:'center', fontSize: '10px'}}>Procedimiento/Consulta</div>,
        dataIndex:'column', width: 180,
        render:(t:string)=><Tooltip title={t}>
          <div style={{fontSize:9,fontWeight:500,lineHeight:1.1}}>{t}</div>
        </Tooltip>,
        sorter:(a,b)=> (a.column??'').localeCompare(b.column??'')
      },
      { title:<div style={{textAlign:'center', fontSize: '10px'}}>Palabra Clave</div>,
        dataIndex:'keyword', width:75, align:'center',
        render:(k:string)=><Tag color={kwColor(k)} style={{fontSize:8,fontWeight:500,padding:'0 4px',lineHeight:'16px'}}>{k?.toUpperCase()}</Tag>
      }
    ];
    
    if (hasND){
      base.push(
        { title:<div style={{textAlign:'center', fontSize: '10px'}}>Denominador</div>, dataIndex:'denominador',
          width:70,align:'center',
          render:(v:number)=><Text style={{color:'#1890ff',fontSize:9}}>{v?.toLocaleString()}</Text>,
          sorter:(a,b)=>(a.denominador??0)-(b.denominador??0)
        },
        { title:<div style={{textAlign:'center', fontSize: '10px'}}>Numerador</div>, dataIndex:'numerador',
          width:70,align:'center',
          render:(v:number)=><Text style={{color:'#52c41a',fontSize:9}}>{v?.toLocaleString()}</Text>,
          sorter:(a,b)=>(a.numerador??0)-(b.numerador??0)
        }
      );
    }
    
    base.push({
      title:<div style={{textAlign:'center', fontSize: '10px'}}>% Cumplimiento</div>,
      width:85,align:'center',
      render:(_:any,r:KeywordAgeReportItem)=>{
        let pct:number;
        if(hasND && r.cobertura_porcentaje!==undefined) pct=r.cobertura_porcentaje;
        else{
          const tot=Object.values(totals_by_keyword??{}).reduce((a:any,c:any)=>a+(c.count||0),0);
          pct=tot? (r.count||0)/tot*100:0;
        }
        const color = pct>=70?'#52c41a': pct>=50?'#fa8c16':'#ff4d4f';
        return <Text strong style={{color,fontSize:9}}>{pct.toFixed(1)}%</Text>;
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

    if (hasSemaforizacion) {
      base.push({
        title: <div style={{textAlign:'center', fontSize: '10px'}}>🚦 Estado</div>,
        dataIndex: 'semaforizacion',
        width: 110,
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

  const rowExpandable = useCallback((r:KeywordAgeReportItem)=>
      !!findTemporal(temporal_data,r.column??'',r.keyword??'',r.age_range??'')?.data?.years,
    [temporal_data]);

  const rowKey = useCallback((r:KeywordAgeReportItem)=>
      `${r.column}-${r.keyword}-${r.age_range}`,[]);

  return (
    <>
      <style>{`
        .temporal-year-row { background-color: #e6f7ff !important; font-weight: 500; }
        .temporal-month-row { background-color: #f6ffed !important; }
        
        .ultra-compact-table .ant-table-tbody > tr > td {
          padding: 2px 4px !important;
          height: 22px !important;
          line-height: 1.2 !important;
          font-size: 9px !important;
        }
        
        .ultra-compact-table .ant-table-thead > tr > th {
          padding: 3px 4px !important;
          font-size: 10px !important;
          font-weight: 600 !important;
          height: 26px !important;
          line-height: 1.2 !important;
        }
        
        .ultra-compact-table .ant-table-tbody > tr:hover > td {
          background-color: rgba(24, 144, 255, 0.05) !important;
        }
        
        .ultra-compact-table .ant-table-expanded-row > td {
          padding: 4px !important;
        }
        
        .ultra-compact-table .ant-btn-sm {
          height: 20px !important;
          padding: 0 4px !important;
          font-size: 10px !important;
        }
        
        .ultra-compact-table .ant-tag {
          margin: 0 !important;
          padding: 0 4px !important;
          font-size: 8px !important;
          line-height: 16px !important;
        }
        
        .ultra-compact-table .ant-empty {
          margin: 8px 0 !important;
        }
        
        .ultra-compact-table .ant-empty-description {
          font-size: 10px !important;
        }
        
        .ultra-compact-table .ant-pagination {
          margin: 8px 0 !important;
        }
        
        .ultra-compact-table .ant-pagination-item,
        .ultra-compact-table .ant-pagination-prev,
        .ultra-compact-table .ant-pagination-next {
          min-width: 24px !important;
          height: 24px !important;
          line-height: 22px !important;
          font-size: 11px !important;
        }
      `}</style>

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

      <Table
        dataSource={items}
        columns={columns}
        rowKey={rowKey}
        size="small"
        tableLayout="fixed"
        scroll={{x: hasSemaforizacion ? 600 : 480, y:400}}
        expandable={showTemporalData?{
          expandedRowRender, rowExpandable,
          expandIcon:({expanded,onExpand,record})=> rowExpandable(record)
            ? (<Button type="text" size="small"
                       icon={expanded?<CompressOutlined style={{fontSize:'10px'}}/>:<ExpandAltOutlined style={{fontSize:'10px'}}/>}
                       onClick={e=>onExpand(record,e)}
                       style={{padding:0,fontSize:9,color:'#1890ff',height:'18px',width:'18px'}}/>)
            : <span style={{width:10,display:'inline-block'}}/>
        }:undefined}
        pagination={{pageSize:25,showSizeChanger:true,simple:true,size:'small'}}
        locale={{emptyText:<Empty description="Sin datos" image={Empty.PRESENTED_IMAGE_SIMPLE}/>}}
        className="ultra-compact-table"
        style={{fontSize:9}}
      />
    </>
  );
});

ReportTable.displayName = 'ReportTable';
