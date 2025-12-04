// components/technical-note/report/Report.tsx - VERSIÓN SIMPLIFICADA SIN SELECCIÓN DE EDADES

import React, { memo, useCallback, useState } from 'react';
import { Card, Typography, Button, message, Space } from 'antd';
import {
  BarChartOutlined,
  CalendarOutlined,
  UserDeleteOutlined
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

// INTERFAZ EXTENDIDA
interface ReportPropsExtended extends TemporalReportProps {
  cutoffDate?: string; // Fecha de corte desde componente padre (formato YYYY-MM-DD)
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
  cutoffDate, // PROP RECIBIDA
  onToggleReportVisibility,
  onSetReportKeywords,
  onSetShowTemporalData,
  onLoadKeywordAgeReport,
  onDepartamentoChange,
  onMunicipioChange,
  onIpsChange,
  resetGeographicFilters,
}) => {
  // DEBUG: Log inmediato al recibir props
  console.log('🔍 Report recibió cutoffDate:', cutoffDate);
  console.log('🔍 Report recibió selectedFile:', selectedFile);

  const { keywordStats, reportTitle } = useReportData(keywordReport, reportKeywords);

  // ESTADOS: Manejo de reporte de inasistentes
  const [inasistentesReport, setInasistentesReport] = useState<InasistentesReportResponse | null>(null);
  const [loadingInasistentes, setLoadingInasistentes] = useState(false);
  const [showInasistentesReport, setShowInasistentesReport] = useState(false);

  // HANDLER: Generar reporte de cobertura
  const handleLoadReport = useCallback(() => {
    console.log('📊 handleLoadReport ejecutado');
    console.log('   - selectedFile:', selectedFile);
    console.log('   - cutoffDate:', cutoffDate);

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
      geographicFilters
    });

    onLoadKeywordAgeReport(
      selectedFile,
      cutoffDate,
      reportKeywords.length > 0 ? reportKeywords : DEFAULT_KEYWORDS,
      reportMinCount,
      true,
      geographicFilters
    );
  }, [selectedFile, cutoffDate, reportKeywords, reportMinCount, geographicFilters, onLoadKeywordAgeReport]);

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
      geographicFilters
    });

    onLoadKeywordAgeReport(
      selectedFile,
      cutoffDate,
      reportKeywords,
      reportMinCount,
      showTemporalData,
      geographicFilters
    );
  }, [selectedFile, cutoffDate, reportKeywords, reportMinCount, showTemporalData, geographicFilters, onLoadKeywordAgeReport]);

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

      // Llamada actualizada con la nueva firma
      const response = await TechnicalNoteService.getInasistentesReport(
        selectedFile,
        cutoffDate,
        reportKeywords.length > 0 ? reportKeywords : DEFAULT_KEYWORDS,
        {
          departamento: geographicFilters.departamento,
          municipio: geographicFilters.municipio,
          ips: geographicFilters.ips
        }
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
  }, [selectedFile, cutoffDate, reportKeywords, geographicFilters]);

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
    console.log('   canGenerateReport:', canGenerateReport);
    console.log('   hasReport:', hasReport);
    console.log('   showReport:', showReport);
    console.log('================================================');
  }, [cutoffDate, selectedFile, canGenerateReport, hasReport, showReport]);

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
    geographicFilters.ips
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

                      {cutoffDate && (
                        <div style={{ marginTop: 12 }}>
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            📅 Fecha de corte: <Text strong>{cutoffDate}</Text>
                          </Text>
                        </div>
                      )}
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
