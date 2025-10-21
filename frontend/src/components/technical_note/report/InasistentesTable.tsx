// components/report/InasistentesTable.tsx - ✅ CORREGIDO

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
    cutoffDate?: string; // NUEVA PROP: Fecha de corte desde el padre (formato YYYY-MM-DD)
}

export const InasistentesTable: React.FC<InasistentesTableProps> = memo(({ 
    reportData, 
    loading,
    cutoffDate 
}) => {
    const [activeTab, setActiveTab] = useState<string>('resumen');

    if (loading) {
        return (
            <Card loading style={{ height: '600px' }}>
                <div style={{ padding: '20px', textAlign: 'center' }}>
                    <Text>Generando reporte dinámico de inasistentes...</Text>
                </div>
            </Card>
        );
    }

    if (!reportData || !reportData.success) {
        return (
            <Card style={{ height: '600px' }}>
                <Empty
                    description="No se ha generado el reporte de inasistentes"
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                />
            </Card>
        );
    }

    // ✅ Usar cutoffDate del padre o corte_fecha del reporte como fallback
    const displayCutoffDate = cutoffDate || reportData.corte_fecha;

    // ✅ COLUMNAS PARA TABLA DE INASISTENTES
    const getTableColumns = (showActivityColumn: boolean = false) => [
        {
            title: 'Ubicación',
            key: 'ubicacion',
            width: showActivityColumn ? '18%' : '20%',
            render: (_: any, record: InasistenteRecord) => (
                <div style={{ padding: '2px 0' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 2, marginBottom: 1 }}>
                        <EnvironmentOutlined style={{ color: '#1890ff', fontSize: 10 }} />
                        <Text strong style={{ fontSize: 10, lineHeight: 1.2 }}>{record.departamento}</Text>
                    </div>
                    <div style={{ marginBottom: 1 }}>
                        <Text style={{ fontSize: 9, color: '#666', lineHeight: 1.1 }}>{record.municipio}</Text>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                        <BankOutlined style={{ color: '#52c41a', fontSize: 9 }} />
                        <Text style={{ fontSize: 9, color: '#52c41a', lineHeight: 1.1 }}>{record.nombre_ips}</Text>
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
                <Tag color="blue" style={{ fontSize: 10, padding: '1px 6px', margin: 0 }}>
                    {text}
                </Tag>
            )
        },
        {
            title: 'Nombre Completo',
            key: 'nombre_completo',
            width: showActivityColumn ? '20%' : '25%',
            render: (_: any, record: InasistenteRecord) => (
                <div style={{ padding: '2px 0' }}>
                    <div style={{ marginBottom: 1 }}>
                        <Text strong style={{ fontSize: 11, lineHeight: 1.2 }}>
                            {record.primer_nombre} {record.segundo_nombre}
                        </Text>
                    </div>
                    <div>
                        <Text style={{ fontSize: 10, color: '#666', lineHeight: 1.1 }}>
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
                <div style={{ padding: '2px 0' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 2, marginBottom: 1 }}>
                        <CalendarOutlined style={{ color: '#fa8c16', fontSize: 9 }} />
                        <Text style={{ fontSize: 10, lineHeight: 1.1 }}>
                            <strong>{record.edad_anos || 'N/A'}</strong> años
                        </Text>
                    </div>
                    <div>
                        <Text style={{ fontSize: 9, color: '#666', lineHeight: 1.1 }}>
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
                <div style={{ padding: '2px 0' }}>
                    <Tag color="orange" style={{ fontSize: 9, padding: '1px 6px', margin: 0, marginBottom: 2 }}>
                        {record.columna_evaluada}
                    </Tag>
                    <div>
                        <Text style={{ fontSize: 8, color: '#999', lineHeight: 1.1 }}>
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
                    icon={<UserDeleteOutlined style={{ fontSize: 10 }} />}
                    style={{ fontSize: 9, padding: '1px 6px', margin: 0 }}
                >
                    INASISTENTE
                </Tag>
            )
        }
    ];

    // ✅ RENDERIZAR TABLA PARA UNA ACTIVIDAD ESPECÍFICA
    const renderActivityTable = (activityReport: ActivityReport) => (
        <Card
            size="small"
            title={
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <span style={{ fontSize: 14 }}>{activityReport.actividad}</span>
                    <Badge
                        count={activityReport.statistics.total_inasistentes}
                        overflowCount={Infinity}
                        style={{ backgroundColor: '#ff4d4f' }}
                    />
                </div>
            }
            style={{ marginBottom: 16 }}
        >
            <Row gutter={16} style={{ marginBottom: 12 }}>
                <Col span={6}>
                    <Statistic
                        title="Inasistentes"
                        value={activityReport.statistics.total_inasistentes}
                        valueStyle={{ fontSize: 14, color: '#ff4d4f' }}
                    />
                </Col>
                <Col span={6}>
                    <Statistic
                        title="Departamentos"
                        value={activityReport.statistics.departamentos_afectados}
                        valueStyle={{ fontSize: 14, color: '#1890ff' }}
                    />
                </Col>
                <Col span={6}>
                    <Statistic
                        title="Municipios"
                        value={activityReport.statistics.municipios_afectados}
                        valueStyle={{ fontSize: 14, color: '#52c41a' }}
                    />
                </Col>
                <Col span={6}>
                    <Statistic
                        title="IPS"
                        value={activityReport.statistics.ips_afectadas}
                        valueStyle={{ fontSize: 14, color: '#fa8c16' }}
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
                    showSizeChanger: false
                }}
                size="small"
                scroll={{ x: 900, y: 200 }}
                locale={{
                    emptyText: (
                        <Empty
                            description={`No hay inasistentes para: ${activityReport.actividad}`}
                            image={Empty.PRESENTED_IMAGE_SIMPLE}
                            style={{ margin: '10px 0' }}
                        />
                    )
                }}
                className="activity-table-compact"
            />
        </Card>
    );

    // ✅ RENDERIZAR RESUMEN GENERAL
    const renderResumenGeneral = () => (
        <div>
            <Row gutter={16} style={{ marginBottom: 16 }}>
                <Col span={4}>
                    <Statistic
                        title="Total Inasistentes"
                        value={reportData.resumen_general.total_inasistentes_global}
                        prefix={<UserDeleteOutlined />}
                        valueStyle={{ color: '#ff4d4f', fontSize: 18 }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="Actividades Evaluadas"
                        value={reportData.resumen_general.total_actividades_evaluadas}
                        prefix={<UnorderedListOutlined />}
                        valueStyle={{ color: '#1890ff', fontSize: 18 }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="Con Inasistencias"
                        value={reportData.resumen_general.actividades_con_inasistentes}
                        prefix={<ExclamationCircleOutlined />}
                        valueStyle={{ color: '#fa8c16', fontSize: 18 }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="Sin Inasistencias"
                        value={reportData.resumen_general.actividades_sin_inasistentes}
                        prefix={<CheckCircleOutlined />}
                        valueStyle={{ color: '#52c41a', fontSize: 18 }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="Departamentos"
                        value={reportData.resumen_general.departamentos_afectados}
                        prefix={<EnvironmentOutlined />}
                        valueStyle={{ color: '#722ed1', fontSize: 18 }}
                    />
                </Col>
                <Col span={4}>
                    <Statistic
                        title="IPS Afectadas"
                        value={reportData.resumen_general.ips_afectadas}
                        prefix={<BankOutlined />}
                        valueStyle={{ color: '#eb2f96', fontSize: 18 }}
                    />
                </Col>
            </Row>

            <Card size="small" title="Columnas Descubiertas Dinámicamente" style={{ marginBottom: 16 }}>
                {Object.entries(reportData.columnas_descubiertas).map(([keyword, columns]) => (
                    <div key={keyword} style={{ marginBottom: 8 }}>
                        <Text strong style={{ fontSize: 12, color: '#1890ff' }}>
                            {keyword.toUpperCase()}:
                        </Text>
                        <div style={{ marginLeft: 16, marginTop: 4 }}>
                            {columns.map((col, idx) => (
                                <Tag key={idx} color="blue" style={{ fontSize: 10, margin: '2px 4px 2px 0' }}>
                                    {col}
                                </Tag>
                            ))}
                        </div>
                    </div>
                ))}
            </Card>

            <Card size="small" title="Resumen por Actividades">
                <Row gutter={8}>
                    {reportData.inasistentes_por_actividad.map((activity, idx) => (
                        <Col span={6} key={idx} style={{ marginBottom: 8 }}>
                            <Card
                                size="small"
                                style={{
                                    textAlign: 'center',
                                    backgroundColor: activity.statistics.total_inasistentes > 0 ? '#fff2e8' : '#f6ffed'
                                }}
                            >
                                <Text strong style={{ fontSize: 10, display: 'block', marginBottom: 4 }}>
                                    {activity.actividad.length > 25
                                        ? `${activity.actividad.substring(0, 25)}...`
                                        : activity.actividad}
                                </Text>
                                <Badge
                                    count={activity.statistics.total_inasistentes}
                                    overflowCount={Infinity}
                                    style={{
                                        backgroundColor: activity.statistics.total_inasistentes > 0 ? '#ff4d4f' : '#52c41a',
                                    }}
                                />
                            </Card>
                        </Col>
                    ))}
                </Row>
            </Card>
        </div>
    );

    // ✅ HANDLER DE EXPORTACIÓN CORREGIDO
    const handleExportCSV = async () => {
        if (!reportData) return;

        // ✅ VALIDAR QUE HAYA FECHA DE CORTE
        if (!displayCutoffDate) {
            console.error('❌ No hay fecha de corte disponible para exportar');
            return;
        }

        try {
            console.log('📥 Iniciando exportación CSV con fecha de corte:', displayCutoffDate);

            const { filtros_aplicados, filename } = reportData;

            // ✅ ORDEN CORRECTO DE PARÁMETROS:
            // exportInasistentesCSV(filename, cutoffDate, months, years, keywords, geoFilters)
            const csvBlob = await TechnicalNoteService.exportInasistentesCSV(
                filename,
                displayCutoffDate,                          // ✅ SEGUNDO PARÁMETRO: fecha de corte
                filtros_aplicados.selected_months,          // ✅ TERCER PARÁMETRO: meses
                filtros_aplicados.selected_years,           // ✅ CUARTO PARÁMETRO: años
                filtros_aplicados.selected_keywords,        // ✅ QUINTO PARÁMETRO: keywords
                {                                           // ✅ SEXTO PARÁMETRO: filtros geográficos
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

            // Descargar preservando encoding
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

            // Nombre final único con fecha y hora locales
            link.download = `inasistentes_${filename.replace('.csv', '')}${filterSuffix}_${today}_${hms}.csv`;

            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            window.URL.revokeObjectURL(url);

            console.log('✅ CSV descargado exitosamente con fecha de corte:', displayCutoffDate);
        } catch (error) {
            console.error('❌ Error exportando CSV:', error);
        }
    };

    return (
        <Card
            title={
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <UserDeleteOutlined style={{ color: '#ff4d4f', fontSize: 16 }} />
                    <span style={{ fontSize: 16 }}>Reporte Dinámico de Inasistentes</span>
                    <Badge
                        count={reportData.resumen_general.total_inasistentes_global}
                        overflowCount={Infinity}
                        style={{ backgroundColor: '#ff4d4f' }}
                    />
                    {displayCutoffDate && (
                        <Tag color="blue" icon={<CalendarOutlined />} style={{ fontSize: 11 }}>
                            Corte: {dayjs(displayCutoffDate).format('DD/MM/YYYY')}
                        </Tag>
                    )}
                </div>
            }
            size="small"
            style={{ height: '700px', display: 'flex', flexDirection: 'column' }}
            extra={
                <Button
                    type="primary"
                    icon={<DownloadOutlined />}
                    onClick={handleExportCSV}
                    size="small"
                    style={{ backgroundColor: '#52c41a', borderColor: '#52c41a' }}
                    disabled={!displayCutoffDate}
                    title={!displayCutoffDate ? "Se requiere fecha de corte para exportar" : "Exportar CSV"}
                >
                    Exportar CSV
                </Button>
            }
        >
            <div style={{
                marginBottom: 12,
                padding: 10,
                backgroundColor: '#f0f5ff',
                border: '1px solid #adc6ff',
                borderRadius: 6
            }}>
                <Space direction="vertical" size={4} style={{ width: '100%' }}>
                    <div style={{
                        padding: '6px 10px',
                        backgroundColor: '#e6f7ff',
                        border: '1px solid #91d5ff',
                        borderRadius: 4,
                        marginBottom: 6
                    }}>
                        <Space align="center">
                            <CalendarOutlined style={{ color: '#1890ff', fontSize: 14 }} />
                            <Text strong style={{ fontSize: 12, color: '#1890ff' }}>
                                Fecha de Corte:
                            </Text>
                            <Text style={{ fontSize: 13, fontWeight: 600, color: '#1890ff' }}>
                                {dayjs(displayCutoffDate).format('DD/MM/YYYY')}
                            </Text>
                            <InfoCircleOutlined style={{ color: '#8c8c8c', fontSize: 12 }} />
                            <Text type="secondary" style={{ fontSize: 11 }}>
                                Las edades se calcularon a esta fecha
                            </Text>
                        </Space>
                    </div>

                    <div>
                        <Text strong style={{ color: '#595959', fontSize: 11 }}>
                            📋 Filtros Aplicados:
                        </Text>
                        <div style={{ marginTop: 4, display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                            {reportData.filtros_aplicados.selected_keywords?.length > 0 && (
                                <Tag color="purple" style={{ fontSize: 10 }}>
                                    🔍 Búsqueda: {reportData.filtros_aplicados.selected_keywords.join(', ')}
                                </Tag>
                            )}
                            {reportData.filtros_aplicados.selected_years?.length > 0 && (
                                <Tag color="blue" style={{ fontSize: 10 }}>
                                    🗓️ Años: {reportData.filtros_aplicados.selected_years.join(', ')}
                                </Tag>
                            )}
                            {reportData.filtros_aplicados.selected_months?.length > 0 && (
                                <Tag color="cyan" style={{ fontSize: 10 }}>
                                    📅 Meses: {reportData.filtros_aplicados.selected_months.join(', ')}
                                </Tag>
                            )}
                            {reportData.filtros_aplicados.departamento && (
                                <Tag color="green" style={{ fontSize: 10 }}>
                                    📍 Depto: {reportData.filtros_aplicados.departamento}
                                </Tag>
                            )}
                            {reportData.filtros_aplicados.municipio && (
                                <Tag color="orange" style={{ fontSize: 10 }}>
                                    🏘️ Mpio: {reportData.filtros_aplicados.municipio}
                                </Tag>
                            )}
                            {reportData.filtros_aplicados.ips && (
                                <Tag color="red" style={{ fontSize: 10 }}>
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
                    style={{ height: '100%' }}
                    tabBarStyle={{ marginBottom: 8 }}
                >
                    <TabPane
                        tab={
                            <span>
                                📊 Resumen General
                                <Badge
                                    count={reportData.resumen_general.total_actividades_evaluadas}
                                    size="small"
                                    overflowCount={Infinity}
                                    style={{ marginLeft: 4, backgroundColor: '#1890ff' }}
                                />
                            </span>
                        }
                        key="resumen"
                    >
                        <div style={{ height: '500px', overflow: 'auto' }}>
                            {renderResumenGeneral()}
                        </div>
                    </TabPane>

                    <TabPane
                        tab={
                            <span>
                                📋 Por Actividades
                                <Badge
                                    count={reportData.resumen_general.actividades_con_inasistentes}
                                    size="small"
                                    overflowCount={Infinity}
                                    style={{ marginLeft: 4, backgroundColor: '#ff4d4f' }}
                                />
                            </span>
                        }
                        key="actividades"
                    >
                        <div style={{ height: '500px', overflow: 'auto' }}>
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
                                    image={<CheckCircleOutlined style={{ fontSize: 64, color: '#52c41a' }} />}
                                />
                            )}
                        </div>
                    </TabPane>
                </Tabs>
            </div>
        </Card>
    );
});

InasistentesTable.displayName = 'InasistentesTable';
