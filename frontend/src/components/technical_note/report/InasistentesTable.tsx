// components/report/InasistentesTable.tsx
import React, { memo, useState, useMemo } from 'react';
import { Table, Typography, Tag, Card, Button, Space, message, Badge, Empty } from 'antd';
import {
    UserDeleteOutlined,
    EnvironmentOutlined,
    BankOutlined,
    CalendarOutlined,
    DownloadOutlined,
    InfoCircleOutlined,
    TeamOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { 
    InasistentesReportResponse, 
    InasistenteRecord,
    ActivityReport,
    MesData 
} from '../../../interfaces/IAbsentUser';
import './InasistentesTable.css';
import { TechnicalNoteService } from '../../../services/TechnicalNoteService';
import dayjs from 'dayjs';
import 'dayjs/locale/es';


dayjs.locale('es');


const { Text } = Typography;


const MESES_INFO = [
    { key: 'enero', nombre: 'Enero', num: 1 },
    { key: 'febrero', nombre: 'Febrero', num: 2 },
    { key: 'marzo', nombre: 'Marzo', num: 3 },
    { key: 'abril', nombre: 'Abril', num: 4 },
    { key: 'mayo', nombre: 'Mayo', num: 5 },
    { key: 'junio', nombre: 'Junio', num: 6 },
    { key: 'julio', nombre: 'Julio', num: 7 },
    { key: 'agosto', nombre: 'Agosto', num: 8 },
    { key: 'septiembre', nombre: 'Septiembre', num: 9 },
    { key: 'octubre', nombre: 'Octubre', num: 10 },
    { key: 'noviembre', nombre: 'Noviembre', num: 11 },
    { key: 'diciembre', nombre: 'Diciembre', num: 12 }
];


interface InasistentesTableProps {
    reportData: InasistentesReportResponse | null;
    loading: boolean;
    cutoffDate?: string;
    selectedRegimen?: 'Subsidiado' | 'Contributivo' | null;  // ← NUEVO PROP
}


export const InasistentesTable: React.FC<InasistentesTableProps> = memo(({ 
    reportData, 
    loading,
    cutoffDate,
    selectedRegimen  // ← NUEVO PROP
}) => {
    // ========================================
    // HOOKS
    // ========================================
    const [selectedRangoEdad, setSelectedRangoEdad] = useState<string>('todos');
    const [selectedMes, setSelectedMes] = useState<string | null>(null);


    const displayCutoffDate = cutoffDate || reportData?.corte_fecha;


    // Extraer rangos de edad únicos
    const rangosEdad = useMemo(() => {
        if (!reportData || !reportData.inasistentes_por_actividad) {
            return [];
        }


        const rangos = new Set<string>();
        reportData.inasistentes_por_actividad.forEach(activity => {
            rangos.add(activity.rango_edad);
        });


        return Array.from(rangos).sort();
    }, [reportData]);


    // Filtrar actividades por rango de edad
    const actividadesFiltradas = useMemo(() => {
        if (!reportData || !reportData.inasistentes_por_actividad) {
            return [];
        }


        if (selectedRangoEdad === 'todos') {
            return reportData.inasistentes_por_actividad;
        }


        return reportData.inasistentes_por_actividad.filter(
            activity => activity.rango_edad === selectedRangoEdad
        );
    }, [reportData, selectedRangoEdad]);


    // Calcular totales por mes (según filtro de rango de edad)
    const totalesPorMes = useMemo(() => {
        const totales: { [key: string]: number } = {};


        MESES_INFO.forEach(mes => {
            totales[mes.key] = 0;
        });


        actividadesFiltradas.forEach(activity => {
            MESES_INFO.forEach(mes => {
                const mesData = activity[mes.key as keyof ActivityReport] as MesData | undefined;
                if (mesData && mesData.cantidad) {
                    totales[mes.key] += mesData.cantidad;
                }
            });
        });


        return totales;
    }, [actividadesFiltradas]);


    // Obtener inasistentes del mes seleccionado
    const inasistentesDelMes = useMemo(() => {
        if (!selectedMes || !actividadesFiltradas) {
            return [];
        }


        const inasistentes: InasistenteRecord[] = [];


        actividadesFiltradas.forEach(activity => {
            const mesData = activity[selectedMes as keyof ActivityReport] as MesData | undefined;
            if (mesData && mesData.inasistentes) {
                inasistentes.push(...mesData.inasistentes.map(ins => ({
                    ...ins,
                    columna_evaluada: activity.actividad,
                    rango_edad_actividad: activity.rango_edad
                })));
            }
        });


        return inasistentes;
    }, [selectedMes, actividadesFiltradas]);


    // ========================================
    // VALIDACIONES
    // ========================================


    if (loading) {
        return (
            <Card loading className="loading-card">
                <div className="loading-card-body">
                    <div className="loading-text">
                        <Text className="loading-text-content">
                            Generando reporte mensual de inasistentes...
                        </Text>
                    </div>
                </div>
            </Card>
        );
    }


    if (!reportData || !reportData.success) {
        return (
            <Card className="loading-card">
                <div className="loading-card-body">
                    <div className="loading-text">
                        <UserDeleteOutlined className="icon-empty-gray" />
                        <br />
                        <Text type="secondary">No se ha generado el reporte de inasistentes</Text>
                    </div>
                </div>
            </Card>
        );
    }


    // ========================================
    // COLUMNAS DE LA TABLA
    // ========================================


    const columns: ColumnsType<InasistenteRecord> = [
        {
            title: 'Actividad',
            dataIndex: 'columna_evaluada',
            key: 'actividad',
            width: '20%',
            render: (text: string, record: any) => (
                <div>
                    <Text strong className="col-actividad">{text}</Text>
                    {record.rango_edad_actividad && (
                        <Tag color="blue" className="col-tag-rango">
                            {record.rango_edad_actividad}
                        </Tag>
                    )}
                </div>
            )
        },
        {
            title: 'Ubicación',
            key: 'ubicacion',
            width: '18%',
            render: (_: any, record: InasistenteRecord) => (
                <div className="col-ubicacion-container">
                    <div className="col-ubicacion-dept">
                        <EnvironmentOutlined className="icon-dept" />
                        <Text strong className="col-ubicacion-dept-text">
                            {record.departamento}
                        </Text>
                    </div>
                    <div className="col-ubicacion-muni">
                        <Text className="col-ubicacion-muni-text">
                            {record.municipio}
                        </Text>
                    </div>
                    <div className="col-ubicacion-ips">
                        <BankOutlined className="icon-ips" />
                        <Text className="col-ubicacion-ips-text">
                            {record.nombre_ips}
                        </Text>
                    </div>
                </div>
            )
        },
        {
            title: 'Identificación',
            dataIndex: 'nro_identificacion',
            key: 'nro_identificacion',
            width: '10%',
            render: (text: string) => (
                <Tag color="blue" className="col-tag-identificacion">{text}</Tag>
            )
        },
        {
            title: 'Nombre Completo',
            key: 'nombre_completo',
            width: '22%',
            render: (_: any, record: InasistenteRecord) => (
                <div className="col-nombre-container">
                    <div className="col-nombre-primer">
                        <Text strong className="col-nombre-primer-text">
                            {record.primer_nombre} {record.segundo_nombre}
                        </Text>
                    </div>
                    <div>
                        <Text className="col-nombre-apellido-text">
                            {record.primer_apellido} {record.segundo_apellido}
                        </Text>
                    </div>
                </div>
            )
        },
        {
            title: 'F. Nacimiento',
            dataIndex: 'fecha_nacimiento',
            key: 'fecha_nacimiento',
            width: '10%',
            render: (text: string) => (
                <Text className="col-fecha-text">{text}</Text>
            )
        },
        {
            title: 'Edad',
            key: 'edad',
            width: '8%',
            render: (_: any, record: InasistenteRecord) => (
                <Text className="col-edad-text">
                    <strong>{record.edad_anos !== null ? record.edad_anos : 'N/A'}</strong> años
                </Text>
            )
        },
        {
            title: 'Estado',
            key: 'estado',
            width: '12%',
            render: () => (
                <Tag
                    color="red"
                    icon={<UserDeleteOutlined className="icon-estado" />}
                    className="col-tag-estado"
                >
                    INASISTENTE
                </Tag>
            )
        }
    ];


    // ========================================
    // HANDLERS
    // ========================================


    const handleExportCSV = async () => {
        if (!reportData || !displayCutoffDate) {
            message.error('No hay fecha de corte disponible');
            return;
        }


        try {
            const { filtros_aplicados, filename } = reportData;

            // ← LOG para debugging
            console.log('📥 Exportando CSV con régimen:', selectedRegimen);


            const csvBlob = await TechnicalNoteService.exportInasistentesCSV(
                filename,
                displayCutoffDate,
                filtros_aplicados.keywords,
                {
                    departamento: filtros_aplicados.departamento,
                    municipio: filtros_aplicados.municipio,
                    ips: filtros_aplicados.ips
                },
                selectedRegimen || undefined  // ← NUEVO: Pasar régimen
            );


            const now = new Date();
            const timestamp = now.toISOString().replace(/[:.]/g, '-').slice(0, -5);
            
            // ← NUEVO: Incluir régimen en nombre del archivo
            const regimenSuffix = selectedRegimen ? `_${selectedRegimen}` : '';
            const fileName = `inasistentes_${filename.replace('.csv', '')}${regimenSuffix}_${timestamp}.csv`;
            
            const url = window.URL.createObjectURL(
                new Blob([csvBlob], { type: 'text/csv; charset=utf-8' })
            );
            const link = document.createElement('a');
            link.href = url;
            link.download = fileName;


            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            window.URL.revokeObjectURL(url);


            message.success('CSV exportado correctamente');
        } catch (error) {
            console.error('❌ Error exportando CSV:', error);
            message.error('Error exportando CSV');
        }
    };


    const handleRangoEdadChange = (rango: string) => {
        setSelectedRangoEdad(rango);
        setSelectedMes(null);
    };


    const handleMesClick = (mesKey: string) => {
        if (totalesPorMes[mesKey] > 0) {
            setSelectedMes(mesKey);
        } else {
            message.info(`No hay inasistentes en ${MESES_INFO.find(m => m.key === mesKey)?.nombre}`);
        }
    };


    // ========================================
    // RENDER
    // ========================================


    return (
        <Card
            className="inasistentes-filter-card"
            title={
                <Space>
                    <UserDeleteOutlined className="icon-title-red" />
                    <Text strong>Reporte de Inasistentes</Text>
                    <Badge
                        count={reportData.resumen_general.total_inasistentes_global}
                        overflowCount={Infinity}
                        className="badge-title"
                    />
                    {displayCutoffDate && (
                        <Tag color="blue" icon={<CalendarOutlined />}>
                            Corte: {dayjs(displayCutoffDate).format('DD/MM/YYYY')}
                        </Tag>
                    )}
                    {/* ← NUEVO: Mostrar régimen seleccionado */}
                    {selectedRegimen && (
                        <Tag color="green">
                            Régimen: {selectedRegimen}
                        </Tag>
                    )}
                </Space>
            }
            extra={
                <Button
                    type="primary"
                    icon={<DownloadOutlined />}
                    onClick={handleExportCSV}
                    size="small"
                    className="btn-export-csv"
                >
                    Exportar CSV
                </Button>
            }
        >
            {/* Información general */}
            <div className="info-box">
                <Space align="center" size="middle" className="space-info">
                    <InfoCircleOutlined className="icon-info-blue" />
                    <div>
                        <Text strong className="text-info-title">
                            Fecha de Corte: {dayjs(displayCutoffDate).format('DD/MM/YYYY')}
                        </Text>
                        {reportData.filtros_aplicados.keywords?.length > 0 && (
                            <Text type="secondary" className="text-info-subtitle">
                                Búsqueda: {reportData.filtros_aplicados.keywords.join(', ')}
                            </Text>
                        )}
                    </div>
                </Space>
            </div>


            {/* SECCIÓN 1: Filtros de Rango de Edad */}
            <div className="filter-section">
                <div className="filter-section-header">
                    <Space align="center" size="small">
                        <TeamOutlined className="icon-filter-blue" />
                        <Text strong className="text-filter-title">
                            Filtrar por Rango de Edad
                        </Text>
                    </Space>
                </div>
                
                <Space size="small" wrap className="space-filter-buttons">
                    <Button
                        className={`filter-btn ${selectedRangoEdad === 'todos' ? 'filter-btn-active' : ''}`}
                        onClick={() => handleRangoEdadChange('todos')}
                    >
                        <Space size={4}>
                            <span>Todos</span>
                        </Space>
                    </Button>


                    {rangosEdad.map(rango => {
                        return (
                            <Button
                                key={rango}
                                className={`filter-btn ${selectedRangoEdad === rango ? 'filter-btn-active' : ''}`}
                                onClick={() => handleRangoEdadChange(rango)}
                            >
                                <Space size={4}>
                                    <span>{rango}</span>
                                </Space>
                            </Button>
                        );
                    })}
                </Space>
            </div>


            <div className="section-divider" />


            {/* SECCIÓN 2: Filtros de Meses */}
            <div className="filter-section">
                <div className="filter-section-header">
                    <Space align="center" size="small">
                        <CalendarOutlined className="icon-calendar-red" />
                        <Text strong className="text-filter-title">Seleccionar Mes</Text>
                        <Text type="secondary" className="text-filter-subtitle">
                            (Mostrando datos para: {selectedRangoEdad === 'todos' ? 'Todos los rangos' : selectedRangoEdad})
                        </Text>
                    </Space>
                </div>
                
                <Space size="small" wrap className="space-filter-buttons">
                    {MESES_INFO.map(mes => {
                        const total = totalesPorMes[mes.key];
                        const isDisabled = total === 0;
                        const isActive = selectedMes === mes.key;


                        return (
                            <Button
                                key={mes.key}
                                className={`mes-btn ${isActive ? 'mes-btn-active' : ''} ${isDisabled ? 'mes-btn-disabled' : ''}`}
                                onClick={() => handleMesClick(mes.key)}
                                disabled={isDisabled}
                            >
                                <Space direction="vertical" size={0} className="space-mes-content">
                                    <Text 
                                        strong 
                                        className={
                                            isActive ? 'mes-btn-text-active' : 
                                            (isDisabled ? 'mes-btn-text-disabled' : 'mes-btn-text-normal')
                                        }
                                    >
                                        {mes.nombre}
                                    </Text>
                                </Space>
                            </Button>
                        );
                    })}
                </Space>
            </div>


            <div className="section-divider" />


            {/* TABLA DE INASISTENTES */}
            {selectedMes ? (
                <div>
                    <Table
                        className="inasistentes-detail-table"
                        dataSource={inasistentesDelMes}
                        columns={columns}
                        rowKey={(record, index) => `${record.nro_identificacion}-${index}`}
                        pagination={{
                            pageSize: 20,
                            showSizeChanger: true,
                            pageSizeOptions: ['10', '20', '50', '100'],
                            showTotal: (total, range) => `${range[0]}-${range[1]} de ${total} inasistentes`,
                            size: 'small'
                        }}
                        scroll={{ x: 1200, y: 500 }}
                        size="small"
                        bordered
                    />
                </div>
            ) : (
                <Empty
                    className="empty-state"
                    description={
                        <Space direction="vertical" size="small" className="space-empty">
                            <CalendarOutlined className="icon-empty-gray" />
                            <Text type="secondary" className="text-empty-secondary">
                                Seleccione un mes para ver los inasistentes
                            </Text>
                            <Text type="secondary" className="text-empty-small">
                                Los meses con inasistentes están resaltados
                            </Text>
                        </Space>
                    }
                />
            )}


            {/* Resumen inferior */}
            <div className="summary-box">
                <Space size="large" className="space-summary">
                    <div>
                        <Text type="secondary" className="text-summary-label">
                            Total Inasistentes
                        </Text>
                        <br />
                        <Text strong className="text-summary-value-red">
                            {reportData.resumen_general.total_inasistentes_global}
                        </Text>
                    </div>
                    <div>
                        <Text type="secondary" className="text-summary-label">
                            Actividades Evaluadas
                        </Text>
                        <br />
                        <Text strong className="text-summary-value-blue">
                            {reportData.resumen_general.total_actividades_evaluadas}
                        </Text>
                    </div>
                    <div>
                        <Text type="secondary" className="text-summary-label">
                            Con Inasistencias
                        </Text>
                        <br />
                        <Text strong className="text-summary-value-orange">
                            {reportData.resumen_general.actividades_con_inasistentes}
                        </Text>
                    </div>
                    {selectedMes && (
                        <div>
                            <Text type="secondary" className="text-summary-label">
                                Mostrando ({MESES_INFO.find(m => m.key === selectedMes)?.nombre})
                            </Text>
                            <br />
                            <Text strong className="text-summary-value-green">
                                {inasistentesDelMes.length}
                            </Text>
                        </div>
                    )}
                </Space>
            </div>
        </Card>
    );
});


InasistentesTable.displayName = 'InasistentesTable';
