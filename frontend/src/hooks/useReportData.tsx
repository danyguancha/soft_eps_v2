// hooks/useReportData.ts - ✅ ACTUALIZADO PARA NUMERADOR/DENOMINADOR
import { useMemo } from 'react';
import { getKeywordConfig, getKeywordLabel } from '../config/reportKeywords.config';
import type { KeywordAgeReport, TotalsByKeyword, GlobalStatistics } from '../services/TechnicalNoteService';

interface KeywordReport {
  totals_by_keyword: Record<string, TotalsByKeyword>;
  items: Array<{ keyword: string; [key: string]: any }>;
  global_statistics?: GlobalStatistics;  // ✅ NUEVO
}

export const useReportData = (keywordReport: KeywordReport | null, reportKeywords: string[]) => {
  const keywordStats = useMemo(() => {
    if (!keywordReport?.totals_by_keyword) return [];

    return Object.entries(keywordReport.totals_by_keyword).map(([keyword, totals]) => ({
      keyword,
      // Campos tradicionales
      total: totals.count || 0,
      itemsCount: keywordReport.items.filter((item: any) => item.keyword === keyword).length,
      config: getKeywordConfig(keyword),
      
      // ✅ NUEVOS CAMPOS NUMERADOR/DENOMINADOR
      numerador: totals.numerador || 0,
      denominador: totals.denominador || 0,
      actividades: totals.actividades || 0,
      cobertura_promedio: totals.cobertura_promedio || 0,
      
      // Campos calculados
      sin_datos: (totals.denominador || 0) - (totals.numerador || 0),
      tiene_numerador_denominador: (totals.numerador !== undefined && totals.denominador !== undefined)
    }));
  }, [keywordReport]);

  // ✅ NUEVO: Estadísticas globales procesadas
  const globalStats = useMemo(() => {
    if (!keywordReport?.global_statistics) return null;
    
    const stats = keywordReport.global_statistics;
    return {
      totalActividades: stats.total_actividades || 0,
      totalDenominador: stats.total_denominador_global || 0,
      totalNumerador: stats.total_numerador_global || 0,
      totalSinDatos: (stats.total_denominador_global || 0) - (stats.total_numerador_global || 0),
      coberturaGlobal: stats.cobertura_global_porcentaje || 0,
      actividades100Pct: stats.actividades_100_pct_cobertura || 0,
      actividadesMenos50Pct: stats.actividades_menos_50_pct_cobertura || 0,
      mejorCobertura: stats.mejor_cobertura || 0,
      peorCobertura: stats.peor_cobertura || 0,
      coberturaPromedio: stats.cobertura_promedio || 0,
      tieneNumeradorDenominador: true
    } ;
  } , [keywordReport]);

  const reportTitle = useMemo(() => {
    let baseTitle = "Reporte";
    
    if (reportKeywords?.length > 0) {
      const keywordNames = reportKeywords.map(k => getKeywordLabel(k));
      
      if (keywordNames.length === 1) {
        baseTitle = `Reporte: ${keywordNames[0]}`;
      } else if (keywordNames.length === 2) {
        baseTitle = `Reporte: ${keywordNames.join(' y ')}`;
      } else {
        baseTitle = `Reporte: ${keywordNames.slice(0, -1).join(', ')} y ${keywordNames[keywordNames.length - 1]}`;
      }
    }

    // ✅ AGREGAR INDICADOR DE NUMERADOR/DENOMINADOR
    if (globalStats?.tieneNumeradorDenominador) {
      baseTitle += " (N/D)";
    }

    return baseTitle;
  }, [reportKeywords, globalStats]);

  return { 
    keywordStats, 
    reportTitle,
    globalStats  // ✅ NUEVO CAMPO
  };
};
