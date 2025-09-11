// components/technical-note/report/Report.tsx
import React, { memo, useCallback } from 'react';
import { Card, Typography, Button } from 'antd';
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
import { 
  ReportLoading, 
  NoResultsAlert, 
  NoReportState 
} from './ReportAuxiliaryComponents';

// Hooks y configuración
import { useReportData } from '../../../hooks/useReportData';
import { DEFAULT_KEYWORDS } from '../../../config/reportKeywords.config';
import type { TemporalReportProps } from './interfaces/ReportInterfaz';

import './Report.css';

const { Text } = Typography;

export const Report: React.FC<TemporalReportProps> = memo(({
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
  onToggleReportVisibility,
  onSetReportKeywords,
  onSetShowTemporalData,
  onLoadKeywordAgeReport,
  onDepartamentoChange,
  onMunicipioChange,
  onIpsChange,
  resetGeographicFilters,
}) => {
  const { keywordStats, reportTitle } = useReportData(keywordReport, reportKeywords);

  const handleLoadReport = useCallback(() => {
    if (selectedFile) {
      onLoadKeywordAgeReport(
        selectedFile, 
        reportKeywords.length > 0 ? reportKeywords : DEFAULT_KEYWORDS, 
        reportMinCount, 
        true,
        geographicFilters
      );
    }
  }, [selectedFile, reportKeywords, reportMinCount, onLoadKeywordAgeReport, geographicFilters]);

  const handleRegenerateReport = useCallback(() => {
    if (selectedFile) {
      onLoadKeywordAgeReport(
        selectedFile,
        reportKeywords,
        reportMinCount,
        showTemporalData,
        geographicFilters
      );
    }
  }, [selectedFile, reportKeywords, reportMinCount, showTemporalData, onLoadKeywordAgeReport, geographicFilters]);

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
          </div>
          <Button
            type="primary"
            icon={<BarChartOutlined />}
            onClick={handleLoadReport}
            className="temporal-generate-button"
            size="large"
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
          {/* Filtros Geográficos */}
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

          {/* Controles de Palabras Clave */}
          <KeywordControls
            reportKeywords={reportKeywords}
            hasReport={hasReport}
            loadingReport={loadingReport}
            onSetReportKeywords={onSetReportKeywords}
            onRegenerateReport={handleRegenerateReport}
          />

          {/* Alerta cuando no hay resultados */}
          {!hasReport && (
            <NoResultsAlert 
              onRetry={handleRegenerateReport}
              loading={loadingReport}
            />
          )}

          {/* Estadísticas de palabras clave */}
          <KeywordStatistics stats={keywordStats} />

          {/* Tabla principal o estado sin reporte */}
          {hasReport ? (
            <ReportTable
              keywordReport={keywordReport}
              showTemporalData={showTemporalData}
            />
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
