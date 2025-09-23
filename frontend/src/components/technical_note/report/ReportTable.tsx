// components/technical-note/report/ReportTable.tsx
import { memo, useMemo, useCallback } from 'react';
import { Table, Typography, Tag, Space, Button, Tooltip, Empty } from 'antd';
import { CalendarOutlined, ExpandAltOutlined, CompressOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { KeywordAgeReportItem } from '../../../services/TechnicalNoteService';


const { Text } = Typography;


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
  } | null;
  showTemporalData: boolean;
}


export const ReportTable = memo<Props>(({ keywordReport, showTemporalData }) => {
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
