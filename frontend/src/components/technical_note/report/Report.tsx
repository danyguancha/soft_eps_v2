// components/technical-note/report/Report.tsx - VERSIÓN ACTUALIZADA PARA SISTEMA DINÁMICO

import React, { memo, useCallback, useState } from 'react';
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
import { AgeRangeSelector } from './AgeRangeSelector';
import { InasistentesTable } from './InasistentesTable'; // ✅ COMPONENTE ACTUALIZADO
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

  // ✅ ESTADOS EXISTENTES: Manejo de selección de edades
  const [ageSelection, setAgeSelection] = useState({
    selectedYears: [] as number[],
    selectedMonths: [] as number[],
    corteFecha: "2025-07-31"
  });

  // ✅ ESTADOS: Manejo de reporte de inasistentes DINÁMICO
  const [inasistentesReport, setInasistentesReport] = useState<InasistentesReportResponse | null>(null);
  const [loadingInasistentes, setLoadingInasistentes] = useState(false);
  const [showInasistentesReport, setShowInasistentesReport] = useState(false);

  // ✅ HANDLERS EXISTENTES
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

  // ✅ HANDLER ACTUALIZADO: Generación automática de reporte dinámico de inasistentes
  const handleAgeSelectionChange = useCallback(async (selection: {
    selectedYears: number[];
    selectedMonths: number[];
    corteFecha: string;
  }) => {
    setAgeSelection(selection);

    const hasActiveSelection = selection.selectedYears.length > 0 || selection.selectedMonths.length > 0;

    if (hasActiveSelection && selectedFile) {
      setLoadingInasistentes(true);
      setShowInasistentesReport(true);

      try {
        console.log('🏥 Generando reporte DINÁMICO de inasistentes...');
        console.log('🔍 Palabras clave del reporte:', reportKeywords);
        console.log('🔍 Selección de edades:', selection);

        const response = await TechnicalNoteService.getInasistentesReport(
          selectedFile,
          selection.selectedMonths,
          selection.selectedYears,
          reportKeywords,  // ← Palabras clave del reporte activo
          selection.corteFecha,
          geographicFilters
        );

        setInasistentesReport(response);
        
        // ✅ LOGS ACTUALIZADOS PARA NUEVA ESTRUCTURA
        if (response.success && response.resumen_general) {
          console.log(`✅ Reporte dinámico generado:`);
          console.log(`   👥 ${response.resumen_general.total_inasistentes_global} inasistentes totales`);
          console.log(`   📋 ${response.resumen_general.total_actividades_evaluadas} actividades evaluadas`);
          console.log(`   🎯 ${response.resumen_general.actividades_con_inasistentes} actividades con inasistencias`);
          
          // Log de actividades específicas
          const actividadesConInasistencias = response.inasistentes_por_actividad.filter(
            activity => activity.statistics.total_inasistentes > 0
          );
          console.log(`🔍 Actividades con inasistencias:`, actividadesConInasistencias.map(a => ({
            actividad: a.actividad,
            inasistentes: a.statistics.total_inasistentes
          })));
        }

      } catch (error) {
        console.error('❌ Error generando reporte dinámico:', error);
        setInasistentesReport(null);
      } finally {
        setLoadingInasistentes(false);
      }
    } else {
      setShowInasistentesReport(false);
      setInasistentesReport(null);
    }
  }, [selectedFile, geographicFilters, reportKeywords]);

  // ✅ FUNCIÓN AUXILIAR: Calcular totales del reporte dinámico
  const getInasistentesTotals = useCallback(() => {
    if (!inasistentesReport?.resumen_general) {
      return {
        totalInasistentes: 0,
        totalActividades: 0,
        actividadesConInasistencias: 0
      };
    }

    return {
      totalInasistentes: inasistentesReport.resumen_general.total_inasistentes_global,
      totalActividades: inasistentesReport.resumen_general.total_actividades_evaluadas,
      actividadesConInasistencias: inasistentesReport.resumen_general.actividades_con_inasistentes
    };
  }, [inasistentesReport]);

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
            <>
              {/* ✅ TABLA PRINCIPAL DE REPORTES */}
              <ReportTable
                keywordReport={keywordReport}
                showTemporalData={showTemporalData}
              />

              {/* ✅ SELECTOR DE EDADES PARA INASISTENTES DINÁMICOS */}
              {selectedFile && (
                <div style={{ marginTop: 24 }}>
                  <AgeRangeSelector
                    filename={selectedFile}
                    onAgeSelectionChange={handleAgeSelectionChange}
                  />
                </div>
              )}

              {/* ✅ TABLA DE REPORTE DINÁMICO DE INASISTENTES */}
              {selectedFile && showInasistentesReport && (
                <div style={{ marginTop: 24 }}>
                  <InasistentesTable
                    reportData={inasistentesReport}
                    loading={loadingInasistentes}
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

          {/* ✅ INFORMACIÓN ACTUALIZADA DE ESTADO DE SELECCIÓN DE EDADES */}
          {hasReport && (ageSelection.selectedYears.length > 0 || ageSelection.selectedMonths.length > 0) && (
            <div style={{
              marginTop: 16,
              padding: 12,
              backgroundColor: '#e6f7ff',
              border: '1px solid #91d5ff',
              borderRadius: 6
            }}>
              <Text strong style={{ color: '#0958d9' }}>
                🎯 Filtros de Edad Activos para Análisis Dinámico:
              </Text>
              <div style={{ marginTop: 8 }}>
                {ageSelection.selectedYears.length > 0 && (
                  <div>
                    <Text style={{ fontSize: 12 }}>
                      📅 <strong>Años seleccionados:</strong> {ageSelection.selectedYears.join(', ')}
                    </Text>
                  </div>
                )}
                {ageSelection.selectedMonths.length > 0 && (
                  <div>
                    <Text style={{ fontSize: 12 }}>
                      🗓️ <strong>Meses seleccionados:</strong> {ageSelection.selectedMonths.slice(0, 10).join(', ')}{ageSelection.selectedMonths.length > 10 ? '...' : ''}
                    </Text>
                  </div>
                )}
                <div style={{ marginTop: 4 }}>
                  <Text style={{ fontSize: 11, color: '#666' }}>
                    Fecha de corte: {ageSelection.corteFecha}
                  </Text>
                </div>
                
                {/* ✅ INFORMACIÓN ACTUALIZADA DEL REPORTE DINÁMICO */}
                {inasistentesReport?.success && (() => {
                  const totals = getInasistentesTotals();
                  return (
                    <div style={{ marginTop: 6 }}>
                      <Text style={{ fontSize: 11, color: '#52c41a', fontWeight: 'bold' }}>
                        ✅ {totals.totalInasistentes} inasistentes en {totals.totalActividades} actividades
                      </Text>
                      {totals.actividadesConInasistencias > 0 && (
                        <div style={{ marginTop: 2 }}>
                          <Text style={{ fontSize: 10, color: '#fa8c16' }}>
                            🎯 {totals.actividadesConInasistencias} actividades requieren atención
                          </Text>
                        </div>
                      )}
                      {inasistentesReport.metodo && (
                        <div style={{ marginTop: 2 }}>
                          <Text style={{ fontSize: 9, color: '#999' }}>
                            Método: {inasistentesReport.metodo}
                          </Text>
                        </div>
                      )}
                    </div>
                  );
                })()}

                {/* ✅ MOSTRAR PALABRAS CLAVE UTILIZADAS */}
                {inasistentesReport?.filtros_aplicados?.selected_keywords && inasistentesReport.filtros_aplicados.selected_keywords.length > 0 && (
                  <div style={{ marginTop: 4 }}>
                    <Text style={{ fontSize: 10, color: '#722ed1' }}>
                      🔑 <strong>Palabras clave evaluadas:</strong> {inasistentesReport.filtros_aplicados.selected_keywords.join(', ')}
                    </Text>
                  </div>
                )}

                {/* ✅ RESUMEN DE COLUMNAS DESCUBIERTAS */}
                {inasistentesReport?.columnas_descubiertas && Object.keys(inasistentesReport.columnas_descubiertas).length > 0 && (
                  <div style={{ marginTop: 4 }}>
                    <Text style={{ fontSize: 10, color: '#13c2c2' }}>
                      🔍 <strong>Actividades descubiertas:</strong> {Object.values(inasistentesReport.columnas_descubiertas).flat().length} columnas en total
                    </Text>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      ) : null}
    </Card>
  );
});

Report.displayName = 'Report';
export default Report;
