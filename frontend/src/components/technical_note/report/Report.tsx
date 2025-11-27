// components/technical-note/report/Report.tsx - VERSIÓN CON DEBUG

import React, { memo, useCallback, useState } from 'react';
import { Card, Typography, Button, message } from 'antd';
import {
  BarChartOutlined,
  CalendarOutlined
} from '@ant-design/icons';

// Componentes
import { GeographicFilters } from './GeographicFilters';
import { KeywordControls } from './KeywordControls';
import { KeywordStatistics } from './KeywordStatistics';
import { ReportTable } from './ReportTable';
import { ReportHeader } from './ReportHeader';
import { ReportControls } from './ReportControls';
import { AgeRangeSelector } from './AgeRangeSelector';
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

  // ESTADOS: Manejo de selección de edades
  const [, setAgeSelection] = useState({
    selectedYears: [] as number[],
    selectedMonths: [] as number[],
    corteFecha: cutoffDate || "2025-07-31"
  });

  // ESTADOS: Manejo de reporte de inasistentes DINÁMICO
  const [inasistentesReport, setInasistentesReport] = useState<InasistentesReportResponse | null>(null);
  const [loadingInasistentes, setLoadingInasistentes] = useState(false);
  const [showInasistentesReport, setShowInasistentesReport] = useState(false);

  // EFECTO: Sincronizar ageSelection cuando cambia cutoffDate
  React.useEffect(() => {
    console.log('📅 useEffect cutoffDate cambió a:', cutoffDate);
    if (cutoffDate) {
      setAgeSelection(prev => ({
        ...prev,
        corteFecha: cutoffDate
      }));
      console.log(`Fecha de corte actualizada desde padre: ${cutoffDate}`);
    }
  }, [cutoffDate]);

  // HANDLER CORREGIDO: handleLoadReport CON VALIDACIÓN Y FECHA
  const handleLoadReport = useCallback(() => {
    console.log('📊 handleLoadReport ejecutado');
    console.log('   - selectedFile:', selectedFile);
    console.log('   - cutoffDate:', cutoffDate);
    console.log('   - cutoffDate type:', typeof cutoffDate);
    console.log('   - cutoffDate Boolean:', Boolean(cutoffDate));

    if (!selectedFile) {
      console.error('❌ No hay archivo seleccionado');
      message.error('No hay archivo seleccionado');
      return;
    }

    if (!cutoffDate) {
      console.error('❌ cutoffDate es:', cutoffDate);
      console.error('❌ cutoffDate evaluado como falsy');
      message.error('Debe seleccionar una fecha de corte antes de generar el reporte');
      return;
    }

    console.log('Todas las validaciones pasadas, generando reporte...');
    console.log('📊 Generando reporte con:', {
      selectedFile,
      cutoffDate,
      reportKeywords,
      reportMinCount,
      geographicFilters
    });

    // LLAMADA CON 6 PARÁMETROS EN ORDEN CORRECTO
    onLoadKeywordAgeReport(
      selectedFile,
      cutoffDate,
      reportKeywords.length > 0 ? reportKeywords : DEFAULT_KEYWORDS,
      reportMinCount,
      true,
      geographicFilters
    );
  }, [selectedFile, cutoffDate, reportKeywords, reportMinCount, geographicFilters, onLoadKeywordAgeReport]);

  // HANDLER CORREGIDO: handleRegenerateReport CON FECHA
  const handleRegenerateReport = useCallback(() => {
    console.log('🔄 handleRegenerateReport ejecutado');
    console.log('   - selectedFile:', selectedFile);
    console.log('   - cutoffDate:', cutoffDate);

    if (!selectedFile) {
      console.error('❌ No hay archivo seleccionado');
      message.error('No hay archivo seleccionado');
      return;
    }

    if (!cutoffDate) {
      console.error('❌ No hay fecha de corte');
      message.error('Debe seleccionar una fecha de corte antes de regenerar el reporte');
      return;
    }

    console.log('Regenerando reporte con:', {
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

  // HANDLER: Generación de reporte dinámico de inasistentes
  const handleAgeSelectionChange = useCallback(async (selection: {
    selectedYears: number[];
    selectedMonths: number[];
    corteFecha: string;
  }) => {
    setAgeSelection(selection);

    const hasActiveSelection = selection.selectedYears.length > 0 || selection.selectedMonths.length > 0;

    if (hasActiveSelection && selectedFile) {
      const effectiveCutoffDate = selection.corteFecha || cutoffDate;
      
      if (!effectiveCutoffDate) {
        console.error('❌ No hay fecha de corte disponible para generar el reporte');
        message.error('Debe seleccionar una fecha de corte antes de generar el reporte de inasistentes');
        return;
      }

      setLoadingInasistentes(true);
      setShowInasistentesReport(true);

      try {
        console.log('🏥 Generando reporte DINÁMICO de inasistentes...');

        const response = await TechnicalNoteService.getInasistentesReport(
          selectedFile,
          effectiveCutoffDate,
          selection.selectedMonths,
          selection.selectedYears,
          reportKeywords,
          geographicFilters
        );

        setInasistentesReport(response);
        
        if (response.success && response.resumen_general) {
          console.log(`Reporte dinámico generado`);
        }

      } catch (error) {
        console.error('❌ Error generando reporte dinámico:', error);
        message.error('Error generando reporte de inasistentes');
        setInasistentesReport(null);
      } finally {
        setLoadingInasistentes(false);
      }
    } else {
      setShowInasistentesReport(false);
      setInasistentesReport(null);
    }
  }, [selectedFile, geographicFilters, reportKeywords, cutoffDate]);

  // VALIDACIÓN: No permitir generar reporte sin fecha de corte
  const canGenerateReport = Boolean(cutoffDate);

  // LOG DE DEBUG DETALLADO
  React.useEffect(() => {
    console.log('🔍 ====== Estado actual del componente Report ======');
    console.log('   cutoffDate:', cutoffDate);
    console.log('   cutoffDate type:', typeof cutoffDate);
    console.log('   cutoffDate truthy:', !!cutoffDate);
    console.log('   selectedFile:', selectedFile);
    console.log('   canGenerateReport:', canGenerateReport);
    console.log('   hasReport:', hasReport);
    console.log('   showReport:', showReport);
    console.log('================================================');
  }, [cutoffDate, selectedFile, canGenerateReport, hasReport, showReport]);

  // Estado inicial - sin reporte
  if (!hasReport && !loadingReport && !showReport) {
    console.log('🎨 Renderizando estado inicial - sin reporte');
    console.log('   - canGenerateReport:', canGenerateReport);
    console.log('   - cutoffDate actual:', cutoffDate);

    return (
      <Card className="temporal-report-card temporal-empty-state">
        <div className="temporal-empty-content">
          <CalendarOutlined className="temporal-empty-icon" />
          <div className="temporal-empty-text">
            <Text className="temporal-empty-title">Generar Reporte</Text>
            <Text type="secondary" className="temporal-empty-description">
              Analiza las columnas con palabras clave y filtros geográficos
            </Text>

            {!canGenerateReport && (
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

              {selectedFile && cutoffDate && (
                <div style={{ marginTop: 24 }}>
                  <AgeRangeSelector
                    filename={selectedFile}
                    cutoffDate={cutoffDate}
                    onAgeSelectionChange={handleAgeSelectionChange}
                  />
                </div>
              )}

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
