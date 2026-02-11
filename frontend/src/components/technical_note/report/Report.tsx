// components/technical-note/report/Report.tsx - CON FILTRO DE RÉGIMEN


import React, { memo, useCallback, useState } from 'react';
import { Card, Typography, Button, message, Space, Select } from 'antd';
import {
  BarChartOutlined,
  CalendarOutlined,
  UserDeleteOutlined,
  MedicineBoxOutlined
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


const { Text } = Typography;
const { Option } = Select;


// INTERFAZ EXTENDIDA CON RÉGIMEN
interface ReportPropsExtended extends TemporalReportProps {
  cutoffDate?: string; // Fecha de corte desde componente padre (formato YYYY-MM-DD)
  selectedRegimen?: 'Subsidiado' | 'Contributivo' | null; // ← NUEVO
  onRegimenChange?: (regimen: 'Subsidiado' | 'Contributivo' | null) => void; // ← NUEVO
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
  selectedRegimen, // ← NUEVO
  onToggleReportVisibility,
  onSetReportKeywords,
  onSetShowTemporalData,
  onLoadKeywordAgeReport,
  onDepartamentoChange,
  onMunicipioChange,
  onIpsChange,
  onRegimenChange, // ← NUEVO
  resetGeographicFilters,
}) => {
  // DEBUG: Log inmediato al recibir props
  console.log('🔍 Report recibió cutoffDate:', cutoffDate);
  console.log('🔍 Report recibió selectedFile:', selectedFile);
  console.log('🔍 Report recibió selectedRegimen:', selectedRegimen); // ← NUEVO


  const { keywordStats, reportTitle } = useReportData(keywordReport, reportKeywords);


  // ESTADOS: Manejo de reporte de inasistentes
  const [inasistentesReport, setInasistentesReport] = useState<InasistentesReportResponse | null>(null);
  const [loadingInasistentes, setLoadingInasistentes] = useState(false);
  const [showInasistentesReport, setShowInasistentesReport] = useState(false);


  // ← NUEVO: HANDLER para cambio de régimen
  const handleRegimenChange = useCallback((value: 'Subsidiado' | 'Contributivo' | 'todos') => {
    console.log('🏥 Cambio de régimen:', value);
    
    if (onRegimenChange) {
      onRegimenChange(value === 'todos' ? null : value);
    }
  }, [onRegimenChange]);


  // HANDLER: Generar reporte de cobertura
  const handleLoadReport = useCallback(() => {
    console.log('📊 handleLoadReport ejecutado');
    console.log('   - selectedFile:', selectedFile);
    console.log('   - cutoffDate:', cutoffDate);
    console.log('   - selectedRegimen:', selectedRegimen); // ← NUEVO


    if (!selectedFile) {
      console.error('❌ No hay archivo seleccionado');
      message.error('No hay archivo seleccionado');
      return;
    }


    if (!cutoffDate) {
      console.error('❌ No hay fecha de corte');
      message.error('Debe seleccionar una fecha de corte antes de generar el reporte');
      return;
    }


    console.log('✅ Generando reporte de cobertura con:', {
      selectedFile,
      cutoffDate,
      reportKeywords,
      reportMinCount,
      geographicFilters,
      selectedRegimen // ← NUEVO
    });


    onLoadKeywordAgeReport(
      selectedFile,
      cutoffDate,
      reportKeywords.length > 0 ? reportKeywords : DEFAULT_KEYWORDS,
      reportMinCount,
      true,
      geographicFilters,
      selectedRegimen // ← NUEVO
    );
  }, [selectedFile, cutoffDate, reportKeywords, reportMinCount, geographicFilters, selectedRegimen, onLoadKeywordAgeReport]);


  // HANDLER: Regenerar reporte de cobertura
  const handleRegenerateReport = useCallback(() => {
    console.log('🔄 handleRegenerateReport ejecutado');


    if (!selectedFile) {
      message.error('No hay archivo seleccionado');
      return;
    }


    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte antes de regenerar el reporte');
      return;
    }


    console.log('✅ Regenerando reporte con:', {
      selectedFile,
      cutoffDate,
      reportKeywords,
      geographicFilters,
      selectedRegimen // ← NUEVO
    });


    onLoadKeywordAgeReport(
      selectedFile,
      cutoffDate,
      reportKeywords,
      reportMinCount,
      showTemporalData,
      geographicFilters,
      selectedRegimen // ← NUEVO
    );
  }, [selectedFile, cutoffDate, reportKeywords, reportMinCount, showTemporalData, geographicFilters, selectedRegimen, onLoadKeywordAgeReport]);


  // HANDLER: Generar reporte de inasistentes
  const handleGenerateInasistentesReport = useCallback(async () => {
    if (!selectedFile) {
      message.error('No hay archivo seleccionado');
      return;
    }


    if (!cutoffDate) {
      message.error('Debe seleccionar una fecha de corte antes de generar el reporte de inasistentes');
      return;
    }


    setLoadingInasistentes(true);
    setShowInasistentesReport(true);


    try {
      console.log('🏥 Generando reporte de inasistentes...');
      console.log('   - Archivo:', selectedFile);
      console.log('   - Fecha corte:', cutoffDate);
      console.log('   - Keywords:', reportKeywords);
      console.log('   - Filtros geográficos:', geographicFilters);
      console.log('   - Régimen:', selectedRegimen); // ← NUEVO


      // ← MODIFICADO: Llamada con régimen
      const response = await TechnicalNoteService.getInasistentesReport(
        selectedFile,
        cutoffDate,
        reportKeywords.length > 0 ? reportKeywords : DEFAULT_KEYWORDS,
        {
          departamento: geographicFilters.departamento,
          municipio: geographicFilters.municipio,
          ips: geographicFilters.ips
        },
        selectedRegimen || undefined // ← NUEVO
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


  // LOG DE DEBUG
  React.useEffect(() => {
    console.log('🔍 ====== Estado actual del componente Report ======');
    console.log('   cutoffDate:', cutoffDate);
    console.log('   selectedFile:', selectedFile);
    console.log('   selectedRegimen:', selectedRegimen); // ← NUEVO
    console.log('   canGenerateReport:', canGenerateReport);
    console.log('   hasReport:', hasReport);
    console.log('   showReport:', showReport);
    console.log('================================================');
  }, [cutoffDate, selectedFile, selectedRegimen, canGenerateReport, hasReport, showReport]);


  // Estado inicial - sin reporte
  if (!hasReport && !loadingReport && !showReport) {
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
      </Card>
    );
  }


  const hasGeoFilters = Boolean(
    geographicFilters.departamento ||
    geographicFilters.municipio ||
    geographicFilters.ips ||
    selectedRegimen // ← NUEVO
  );


  return (
    <Card
      className="temporal-report-card"
      title={
        <ReportHeader
          reportTitle={reportTitle}
          hasGeoFilters={hasGeoFilters}
          geographicFilters={geographicFilters}
          hasReport={hasReport}
          loadingReport={loadingReport}
        />
      }
      extra={
        <ReportControls
          hasReport={hasReport}
          reportTotalRecords={reportTotalRecords}
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


          {/* ← NUEVO: Selector de Régimen */}
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


          <KeywordControls
            reportKeywords={reportKeywords}
            hasReport={hasReport}
            loadingReport={loadingReport}
            onSetReportKeywords={onSetReportKeywords}
            onRegenerateReport={handleRegenerateReport}
          />


          {!hasReport && (
            <NoResultsAlert
              onRetry={handleRegenerateReport}
              loading={loadingReport}
            />
          )}


          <KeywordStatistics stats={keywordStats} />


          {hasReport ? (
            <>
              <ReportTable
                keywordReport={keywordReport}
                showTemporalData={showTemporalData}
                filename={selectedFile || undefined}
                selectedKeywords={reportKeywords}
                geographicFilters={geographicFilters}
                cutoffDate={cutoffDate}
              />


              {/* Botón para generar reporte de inasistentes */}
              {selectedFile && cutoffDate && (
                <Card
                  style={{ marginTop: 24 }}
                  bodyStyle={{ padding: '16px' }}
                >
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


                      {/* ← MODIFICADO: Mostrar fecha de corte y régimen */}
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


              {/* Validación de fecha de corte */}
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
  );
});


Report.displayName = 'Report';
export default Report;
