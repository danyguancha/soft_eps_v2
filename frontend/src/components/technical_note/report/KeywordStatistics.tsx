// components/technical-note/report/KeywordStatistics.tsx - ✅ ACTUALIZADO PARA NUMERADOR/DENOMINADOR
import React, { memo } from 'react';

// ✅ INTERFACES ACTUALIZADAS
interface KeywordStat {
  keyword: string;
  total: number;
  itemsCount: number;
  config?: any;
  // ✅ NUEVOS CAMPOS NUMERADOR/DENOMINADOR
  numerador: number;
  denominador: number;
  actividades: number;
  cobertura_promedio: number;
  sin_datos: number;
  tiene_numerador_denominador: boolean;
}

interface GlobalStats {
  totalActividades: number;
  totalDenominador: number;
  totalNumerador: number;
  totalSinDatos: number;
  coberturaGlobal: number;
  actividades100Pct: number;
  actividadesMenos50Pct: number;
  mejorCobertura: number;
  peorCobertura: number;
  coberturaPromedio: number;
  tieneNumeradorDenominador: boolean;
}

// ✅ INTERFAZ ACTUALIZADA CON globalStats
interface KeywordStatisticsProps {
  stats: KeywordStat[];
  globalStats?: GlobalStats | null;  // ✅ NUEVA PROP
}

export const KeywordStatistics: React.FC<KeywordStatisticsProps> = memo(({ 
  stats, 
  globalStats 
}) => {
  if (stats.length === 0 && !globalStats) return null;

  return (
    <div style={{ marginBottom: 24 }}>
      {/* ✅ ESTADÍSTICAS GLOBALES NUMERADOR/DENOMINADOR */}
      
    </div>
  );
});

KeywordStatistics.displayName = 'KeywordStatistics';
