// components/report/InasistentesTable.tsx - VERSIÓN ULTRA COMPACTADA

import React, { memo, useState } from 'react';
import { Table, Typography, Tag, Card, Statistic, Row, Col, Empty, Badge, Tabs, Button, Space } from 'antd';
import {
    UserDeleteOutlined,
    EnvironmentOutlined,
    BankOutlined,
    CalendarOutlined,
    UnorderedListOutlined,
    CheckCircleOutlined,
    ExclamationCircleOutlined,
    DownloadOutlined,
    InfoCircleOutlined
} from '@ant-design/icons';
import type { InasistentesReportResponse, InasistenteRecord, ActivityReport } from '../../../interfaces/IAbsentUser';
import './InasistentesTable.css';
import { TechnicalNoteService } from '../../../services/TechnicalNoteService';
import dayjs from 'dayjs';
import 'dayjs/locale/es';

dayjs.locale('es');

const { Text } = Typography;
const { TabPane } = Tabs;

interface InasistentesTableProps {
    reportData: InasistentesReportResponse | null;
    loading: boolean;
    cutoffDate?: string;
}

export const InasistentesTable: React.FC<InasistentesTableProps> = memo(({ 
    reportData, 
    loading,
    cutoffDate 
}) => {
    const [activeTab, setActiveTab] = useState<string>('resumen');

    if (loading) {
        return (
            <Card loading style={{ height: '600px' }} bodyStyle={{ padding: '12px' }}>
                <div style={{ padding: '12px', textAlign: 'center' }}>
                    <Text style={{ fontSize: '11px' }}>Generando reporte dinámico de inasistentes...</Text>
                </div>
            </Card>
        );
    }

    if (!reportData || !reportData.success) {
        return (
            <Card style={{ height: '600px' }} bodyStyle={{ padding: '12px' }}>
                <Empty
                    description="No se ha generado el reporte de inasistentes"
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                    style={{ fontSize: '11px' }}
                />
            </Card>
        );
    }

    const displayCutoffDate = cutoffDate || reportData.corte_fecha;

    const getTableColumns = (showActivityColumn: boolean = false) => [
        {
            title: 'Ubicación',
            key: 'ubicacion',
            width: showActivityColumn ? '18%' : '20%',
            render: (_: any, record: InasistenteRecord) => (
                <div style={{ padding: '1px 0' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 2, marginBottom: 1 }}>
                        <EnvironmentOutlined style={{ color: '#1890ff', fontSize: 9 }} />
                        <Text strong style={{ fontSize: 9, lineHeight: 1.1 }}>{record.departamento}</Text>
                    </div>
                    <div style={{ marginBottom: 1 }}>
                        <Text style={{ fontSize: 8, color: '#666', lineHeight: 1 }}>{record.municipio}</Text>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                        <BankOutlined style={{ color: '#52c41a', fontSize: 8 }} />
                        <Text style={{ fontSize: 8, color: '#52c41a', lineHeight: 1 }}>{record.nombre_ips}</Text>
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
                <Tag color="blue" style={{ fontSize: 9, padding: '0 4px', margin: 0, lineHeight: '16px' }}>
                    {text}
                </Tag>
            )
        },
        {
            title: 'Nombre Completo',
            key: 'nombre_completo',
            width: showActivityColumn ? '20%' : '25%',
            render: (_: any, record: InasistenteRecord) => (
                <div style={{ padding: '1px 0' }}>
                    <div style={{ marginBottom: 1 }}>
                        <Text strong style={{ fontSize: 10, lineHeight: 1.1 }}>
                            {record.primer_nombre} {record.segundo_nombre}
                        </Text>
                    </div>
                    <div>
                        <Text style={{ fontSize: 9, color: '#666', lineHeight: 1 }}>
                            {record.primer_apellido} {record.segundo_apellido}
                        </Text>
                    </div>
                </div>
            )
        },
        {
            title: 'Edad',
            key: 'edad_info',
            width: '15%',
            render: (_: any, record: InasistenteRecord) => (
                <div style={{ padding: '1px 0' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 2, marginBottom: 1 }}>
                        <CalendarOutlined style={{ color: '#fa8c16', fontSize: 8 }} />
                        <Text style={{ fontSize: 9, lineHeight: 1 }}>
                            <strong>{record.edad_anos || 'N/A'}</strong> años
                        </Text>
                    </div>
                    <div>
                        <Text style={{ fontSize: 8, color: '#666', lineHeight: 1 }}>
                            <strong>{record.edad_meses || 'N/A'}</strong> meses
                        </Text>
                    </div>
                </div>
            )
        },
        ...(showActivityColumn ? [{
            title: 'Actividad Faltante',
            key: 'actividad',
            width: '22%',
            render: (_: any, record: InasistenteRecord) => (
                <div style={{ padding: '1px 0' }}>
                    <Tag color="orange" style={{ fontSize: 8, padding: '0 4px', margin: 0, marginBottom: 1, lineHeight: '15px' }}>
                        {record.columna_evaluada}
                    </Tag>
                    <div>
                        <Text style={{ fontSize: 7, color: '#999', lineHeight: 1 }}>
                            Estado: {record.actividad_valor}
                        </Text>
                    </div>
                </div>
            )
        }] : []),
        {
            title: 'Estado',
            key: 'estado',
            width: '15%',
            render: () => (
                <Tag
                    color="red"
                    icon={<UserDeleteOutlined style={{ fontSize: 9 }} />}
                    style={{ fontSize: 8, padding: '0 4px', margin: 0, lineHeight: '16px' }}
                >
                    INASISTENTE
                </Tag>
            )
        }
    ];

    const renderActivityTable = (activityReport: ActivityReport) => (
        <Card
            size="small"
            title={
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <span style={{ fontSize: 11 }}>{activityReport.actividad}</span>
                    <Badge
                        count={activityReport.statistics.total_inasistentes}
                        overflowCount={Infinity}
                        style={{ backgroundColor: '#ff4d4f', fontSize: '9px', height: '18px', lineHeight: '18px', padding: '0 6px' }}
                    />
                </div>
            }
            style={{ marginBottom: 10 }}
            bodyStyle={{ padding: '8px' }}
            headStyle={{ padding: '4px 10px', minHeight: '32px' }}
        >
            <Row gutter={8} style={{ marginBottom: 8 }}>
                <Col span={6}>
                    <Statistic
                        title="Inasistentes"
                        value={activityReport.statistics.total_inasistentes}
                        valueStyle={{ fontSize: 12, color: '#ff4d4f' }}
                        style={{ fontSize: '9px' }}
                    />
                </Col>
                <Col span={6}>
                    <Statistic
                        title="Departamentos"
                        value={activityReport.statistics.departamentos_afectados}
                        valueStyle={{ fontSize: 12, color: '#1890ff' }}
                        style={{ fontSize: '9px' }}
                    />
                </Col>
                <Col span={6}>
                    <Statistic
                        title="Municipios"
                        value={activityReport.statistics.municipios_afectados}
                        valueStyle={{ fontSize: 12, color: '#52c41a' }}
                        style={{ fontSize: '9px' }}
                    />
                </Col>
                <Col span={6}>
                    <Statistic
                        title="IPS"
                        value={activityReport.statistics.ips_afectadas}
                        valueStyle={{ fontSize: 12, color: '#fa8c16' }}
                        style={{ fontSize: '9px' }}
                    />
                </Col>
            </Row>

            <Table
                dataSource={activityReport.inasistentes}
                columns={getTableColumns(true)}
                rowKey={(record) => `${record.nro_identificacion}-${activityReport.actividad}`}
                pagination={{
                    pageSize: 10,
                    size: 'small',
                    showTotal: (total, range) => `${range[0]}-${range[1]} de ${total}`,
                    showSizeChanger: false,
                    style: { marginBottom: 0 }
                }}
                size="small"
                scroll={{ x: 900, y: 180 }}
                locale={{
                    emptyText: (
                        <Empty
                            description={`No hay inasistentes para: ${activityReport.actividad}`}
                            image={Empty.PRESENTED_IMAGE_SIMPLE}
                            style={{ margin: '6px 0', fontSize: '10px' }}
                        />
                    )
                }}
                className="ultra-compact-table"
            />
        </Card>
    );

    const renderResumenGeneral = () => (
        <div>
            <Row gutter={8} style={{ marginBottom: 10 }}>
                <Col span={4}>
                    <Statistic
                        title="Total Inasistentes"
                        value={reportData.resumen_general.total_inasistentes_global}
                        prefix={<UserDeleteOutlined style={{ fontSize: '11px' }} />}
                        valueStyle={{ color: '#ff4d4f', fontSize: 14 }}
                        style={{ fontSize: '10px' }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="Actividades Evaluadas"
                        value={reportData.resumen_general.total_actividades_evaluadas}
                        prefix={<UnorderedListOutlined style={{ fontSize: '11px' }} />}
                        valueStyle={{ color: '#1890ff', fontSize: 14 }}
                        style={{ fontSize: '10px' }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="Con Inasistencias"
                        value={reportData.resumen_general.actividades_con_inasistentes}
                        prefix={<ExclamationCircleOutlined style={{ fontSize: '11px' }} />}
                        valueStyle={{ color: '#fa8c16', fontSize: 14 }}
                        style={{ fontSize: '10px' }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="Sin Inasistencias"
                        value={reportData.resumen_general.actividades_sin_inasistentes}
                        prefix={<CheckCircleOutlined style={{ fontSize: '11px' }} />}
                        valueStyle={{ color: '#52c41a', fontSize: 14 }}
                        style={{ fontSize: '10px' }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="Departamentos"
                        value={reportData.resumen_general.departamentos_afectados}
                        prefix={<EnvironmentOutlined style={{ fontSize: '11px' }} />}
                        valueStyle={{ color: '#722ed1', fontSize: 14 }}
                        style={{ fontSize: '10px' }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="IPS Afectadas"
                        value={reportData.resumen_general.ips_afectadas}
                        prefix={<BankOutlined style={{ fontSize: '11px' }} />}
                        valueStyle={{ color: '#eb2f96', fontSize: 14 }}
                        style={{ fontSize: '10px' }}
                    />
                </Col>
            </Row>

            <Card size="small" title="Columnas Descubiertas Dinámicamente" style={{ marginBottom: 10 }} bodyStyle={{ padding: '8px' }} headStyle={{ padding: '4px 10px', minHeight: '32px' }}>
                {Object.entries(reportData.columnas_descubiertas).map(([keyword, columns]) => (
                    <div key={keyword} style={{ marginBottom: 6 }}>
                        <Text strong style={{ fontSize: 10, color: '#1890ff' }}>
                            {keyword.toUpperCase()}:
                        </Text>
                        <div style={{ marginLeft: 12, marginTop: 2 }}>
                            {columns.map((col, idx) => (
                                <Tag key={idx} color="blue" style={{ fontSize: 8, margin: '1px 3px 1px 0', padding: '0 4px', lineHeight: '16px' }}>
                                    {col}
                                </Tag>
                            ))}
                        </div>
                    </div>
                ))}
            </Card>

            <Card size="small" title="Resumen por Actividades" bodyStyle={{ padding: '8px' }} headStyle={{ padding: '4px 10px', minHeight: '32px' }}>
                <Row gutter={6}>
                    {reportData.inasistentes_por_actividad.map((activity, idx) => (
                        <Col span={6} key={idx} style={{ marginBottom: 6 }}>
                            <Card
                                size="small"
                                style={{
                                    textAlign: 'center',
                                    backgroundColor: activity.statistics.total_inasistentes > 0 ? '#fff2e8' : '#f6ffed',
                                    padding: '4px'
                                }}
                                bodyStyle={{ padding: '4px' }}
                            >
                                <Text strong style={{ fontSize: 9, display: 'block', marginBottom: 2, lineHeight: 1.2 }}>
                                    {activity.actividad.length > 25
                                        ? `${activity.actividad.substring(0, 25)}...`
                                        : activity.actividad}
                                </Text>
                                <Badge
                                    count={activity.statistics.total_inasistentes}
                                    overflowCount={Infinity}
                                    style={{
                                        backgroundColor: activity.statistics.total_inasistentes > 0 ? '#ff4d4f' : '#52c41a',
                                        fontSize: '9px',
                                        height: '18px',
                                        lineHeight: '18px',
                                        padding: '0 6px'
                                    }}
                                />
                            </Card>
                        </Col>
                    ))}
                </Row>
            </Card>
        </div>
    );

    const handleExportCSV = async () => {
        if (!reportData) return;

        if (!displayCutoffDate) {
            console.error('❌ No hay fecha de corte disponible para exportar');
            return;
        }

        try {
            console.log('📥 Iniciando exportación CSV con fecha de corte:', displayCutoffDate);

            const { filtros_aplicados, filename } = reportData;

            const csvBlob = await TechnicalNoteService.exportInasistentesCSV(
                filename,
                displayCutoffDate,
                filtros_aplicados.selected_months,
                filtros_aplicados.selected_years,
                filtros_aplicados.selected_keywords,
                {
                    departamento: filtros_aplicados.departamento,
                    municipio: filtros_aplicados.municipio,
                    ips: filtros_aplicados.ips
                }
            );

            const now = new Date();
            const pad = (n: number) => String(n).padStart(2, '0');
            const yyyy = now.getFullYear();
            const MM = pad(now.getMonth() + 1);
            const dd = pad(now.getDate());
            const HH = pad(now.getHours());
            const mm = pad(now.getMinutes());
            const ss = pad(now.getSeconds());

            const today = `${yyyy}-${MM}-${dd}`;
            const hms = `${HH}-${mm}-${ss}`;

            const url = window.URL.createObjectURL(
                new Blob([csvBlob], { type: 'text/csv; charset=utf-8' })
            );
            const link = document.createElement('a');
            link.href = url;

            const filters: string[] = [];
            if (filtros_aplicados.selected_keywords?.length > 0) {
                filters.push(filtros_aplicados.selected_keywords.join('-'));
            }
            if (filtros_aplicados.selected_months?.length > 0) {
                filters.push(`meses-${filtros_aplicados.selected_months.join('-')}`);
            }
            if (filtros_aplicados.selected_years?.length > 0) {
                filters.push(`años-${filtros_aplicados.selected_years.join('-')}`);
            }
            const filterSuffix = filters.length > 0 ? `_${filters.join('_')}` : '';

            link.download = `inasistentes_${filename.replace('.csv', '')}${filterSuffix}_${today}_${hms}.csv`;

            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            window.URL.revokeObjectURL(url);

            console.log('CSV descargado exitosamente con fecha de corte:', displayCutoffDate);
        } catch (error) {
            console.error('❌ Error exportando CSV:', error);
        }
    };

    return (
        <>
            <style>{`
                .inasistentes-compact .ant-card-head {
                    padding: 0 10px !important;
                    min-height: 36px !important;
                }
                
                .inasistentes-compact .ant-card-head-title {
                    padding: 6px 0 !important;
                    font-size: 13px !important;
                }
                
                .inasistentes-compact .ant-card-body {
                    padding: 10px !important;
                }
                
                .inasistentes-compact .ant-statistic-title {
                    font-size: 10px !important;
                    margin-bottom: 2px !important;
                }
                
                .inasistentes-compact .ant-statistic-content {
                    font-size: 14px !important;
                }
                
                .inasistentes-compact .ant-tabs-nav {
                    margin-bottom: 6px !important;
                }
                
                .inasistentes-compact .ant-tabs-tab {
                    padding: 6px 12px !important;
                    font-size: 11px !important;
                }
                
                .inasistentes-compact .ant-badge-count {
                    font-size: 9px !important;
                    height: 18px !important;
                    line-height: 18px !important;
                    padding: 0 6px !important;
                    min-width: 18px !important;
                }
                
                .inasistentes-compact .ant-tag {
                    font-size: 9px !important;
                    padding: 0 5px !important;
                    line-height: 18px !important;
                    margin: 0 3px 0 0 !important;
                }
                
                .inasistentes-compact .ant-btn-sm {
                    height: 26px !important;
                    padding: 0 10px !important;
                    font-size: 11px !important;
                }
                
                .inasistentes-compact .ant-empty-description {
                    font-size: 11px !important;
                }
                
                .ultra-compact-table .ant-table-tbody > tr > td {
                    padding: 2px 4px !important;
                    font-size: 9px !important;
                    line-height: 1.2 !important;
                }
                
                .ultra-compact-table .ant-table-thead > tr > th {
                    padding: 4px 4px !important;
                    font-size: 10px !important;
                    font-weight: 600 !important;
                    line-height: 1.2 !important;
                }
            `}</style>

            <Card
                className="inasistentes-compact"
                title={
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        <UserDeleteOutlined style={{ color: '#ff4d4f', fontSize: 13 }} />
                        <span style={{ fontSize: 13 }}>Reporte Dinámico de Inasistentes</span>
                        <Badge
                            count={reportData.resumen_general.total_inasistentes_global}
                            overflowCount={Infinity}
                            style={{ backgroundColor: '#ff4d4f', fontSize: '9px', height: '18px', lineHeight: '18px', padding: '0 6px' }}
                        />
                        {displayCutoffDate && (
                            <Tag color="blue" icon={<CalendarOutlined style={{ fontSize: '9px' }} />} style={{ fontSize: 9, padding: '0 5px', lineHeight: '18px' }}>
                                Corte: {dayjs(displayCutoffDate).format('DD/MM/YYYY')}
                            </Tag>
                        )}
                    </div>
                }
                size="small"
                style={{ height: '650px', display: 'flex', flexDirection: 'column' }}
                bodyStyle={{ padding: '10px', flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}
                headStyle={{ padding: '0 10px', minHeight: '36px' }}
                extra={
                    <Button
                        type="primary"
                        icon={<DownloadOutlined style={{ fontSize: '11px' }} />}
                        onClick={handleExportCSV}
                        size="small"
                        style={{ backgroundColor: '#52c41a', borderColor: '#52c41a', fontSize: '11px', height: '26px', padding: '0 10px' }}
                        disabled={!displayCutoffDate}
                        title={!displayCutoffDate ? "Se requiere fecha de corte para exportar" : "Exportar CSV"}
                    >
                        Exportar CSV
                    </Button>
                }
            >
                <div style={{
                    marginBottom: 8,
                    padding: 6,
                    backgroundColor: '#f0f5ff',
                    border: '1px solid #adc6ff',
                    borderRadius: 4
                }}>
                    <Space direction="vertical" size={3} style={{ width: '100%' }}>
                        <div style={{
                            padding: '4px 8px',
                            backgroundColor: '#e6f7ff',
                            border: '1px solid #91d5ff',
                            borderRadius: 3,
                            marginBottom: 4
                        }}>
                            <Space align="center" size={4}>
                                <CalendarOutlined style={{ color: '#1890ff', fontSize: 11 }} />
                                <Text strong style={{ fontSize: 10, color: '#1890ff' }}>
                                    Fecha de Corte:
                                </Text>
                                <Text style={{ fontSize: 11, fontWeight: 600, color: '#1890ff' }}>
                                    {dayjs(displayCutoffDate).format('DD/MM/YYYY')}
                                </Text>
                                <InfoCircleOutlined style={{ color: '#8c8c8c', fontSize: 10 }} />
                                <Text type="secondary" style={{ fontSize: 9 }}>
                                    Las edades se calcularon a esta fecha
                                </Text>
                            </Space>
                        </div>

                        <div>
                            <Text strong style={{ color: '#595959', fontSize: 9 }}>
                                📋 Filtros Aplicados:
                            </Text>
                            <div style={{ marginTop: 3, display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                                {reportData.filtros_aplicados.selected_keywords?.length > 0 && (
                                    <Tag color="purple" style={{ fontSize: 8, padding: '0 4px', lineHeight: '16px' }}>
                                        🔍 Búsqueda: {reportData.filtros_aplicados.selected_keywords.join(', ')}
                                    </Tag>
                                )}
                                {reportData.filtros_aplicados.selected_years?.length > 0 && (
                                    <Tag color="blue" style={{ fontSize: 8, padding: '0 4px', lineHeight: '16px' }}>
                                        🗓️ Años: {reportData.filtros_aplicados.selected_years.join(', ')}
                                    </Tag>
                                )}
                                {reportData.filtros_aplicados.selected_months?.length > 0 && (
                                    <Tag color="cyan" style={{ fontSize: 8, padding: '0 4px', lineHeight: '16px' }}>
                                        📅 Meses: {reportData.filtros_aplicados.selected_months.join(', ')}
                                    </Tag>
                                )}
                                {reportData.filtros_aplicados.departamento && (
                                    <Tag color="green" style={{ fontSize: 8, padding: '0 4px', lineHeight: '16px' }}>
                                        📍 Depto: {reportData.filtros_aplicados.departamento}
                                    </Tag>
                                )}
                                {reportData.filtros_aplicados.municipio && (
                                    <Tag color="orange" style={{ fontSize: 8, padding: '0 4px', lineHeight: '16px' }}>
                                        🏘️ Mpio: {reportData.filtros_aplicados.municipio}
                                    </Tag>
                                )}
                                {reportData.filtros_aplicados.ips && (
                                    <Tag color="red" style={{ fontSize: 8, padding: '0 4px', lineHeight: '16px' }}>
                                        🏥 IPS: {reportData.filtros_aplicados.ips}
                                    </Tag>
                                )}
                            </div>
                        </div>
                    </Space>
                </div>

                <div style={{ flex: 1, overflow: 'hidden' }}>
                    <Tabs
                        activeKey={activeTab}
                        onChange={setActiveTab}
                        size="small"
                        style={{ height: '100%', display: 'flex', flexDirection: 'column' }}
                        tabBarStyle={{ marginBottom: 6, flexShrink: 0 }}
                    >
                        <TabPane
                            tab={
                                <span style={{ fontSize: '11px' }}>
                                    📊 Resumen General
                                    <Badge
                                        count={reportData.resumen_general.total_actividades_evaluadas}
                                        size="small"
                                        overflowCount={Infinity}
                                        style={{ marginLeft: 3, backgroundColor: '#1890ff', fontSize: '8px', height: '16px', lineHeight: '16px', padding: '0 5px' }}
                                    />
                                </span>
                            }
                            key="resumen"
                        >
                            <div style={{ height: '460px', overflow: 'auto' }}>
                                {renderResumenGeneral()}
                            </div>
                        </TabPane>

                        <TabPane
                            tab={
                                <span style={{ fontSize: '11px' }}>
                                    📋 Por Actividades
                                    <Badge
                                        count={reportData.resumen_general.actividades_con_inasistentes}
                                        size="small"
                                        overflowCount={Infinity}
                                        style={{ marginLeft: 3, backgroundColor: '#ff4d4f', fontSize: '8px', height: '16px', lineHeight: '16px', padding: '0 5px' }}
                                    />
                                </span>
                            }
                            key="actividades"
                        >
                            <div style={{ height: '460px', overflow: 'auto' }}>
                                {reportData.inasistentes_por_actividad
                                    .filter(activity => activity.statistics.total_inasistentes > 0)
                                    .map((activity, idx) => (
                                        <div key={idx}>
                                            {renderActivityTable(activity)}
                                        </div>
                                    ))
                                }
                                {reportData.resumen_general.actividades_con_inasistentes === 0 && (
                                    <Empty
                                        description="¡Excelente! No hay inasistencias en ninguna actividad"
                                        image={<CheckCircleOutlined style={{ fontSize: 48, color: '#52c41a' }} />}
                                        style={{ fontSize: '11px', padding: '20px' }}
                                    />
                                )}
                            </div>
                        </TabPane>
                    </Tabs>
                </div>
            </Card>
        </>
    );
});

InasistentesTable.displayName = 'InasistentesTable';
