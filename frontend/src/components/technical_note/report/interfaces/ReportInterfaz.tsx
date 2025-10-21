// components/technical-note/report/interfaces/ReportInterfaz.ts - ✅ ACTUALIZAR

import type { GeographicFilters } from '../../../../services/TechnicalNoteService';

export interface TemporalReportProps {
  keywordReport: any;
  loadingReport: boolean;
  showReport: boolean;
  hasReport: boolean;
  reportTotalRecords: number;
  selectedFile: string | null;
  reportKeywords: string[];
  reportMinCount: number;
  showTemporalData: boolean;
  geographicFilters: GeographicFilters;
  departamentosOptions: string[];
  municipiosOptions: string[];
  ipsOptions: string[];
  loadingGeoFilters: any;
  
  onToggleReportVisibility: () => void;
  onSetReportKeywords: (keywords: string[]) => void;
  onSetShowTemporalData: (show: boolean) => void;
  
  // ✅ FIRMA ACTUALIZADA CON 6 PARÁMETROS
  onLoadKeywordAgeReport: (
    filename: string,           // 1. filename
    cutoffDate: string,         // 2. cutoffDate (OBLIGATORIO)
    keywords?: string[],        // 3. keywords (OPCIONAL)
    minCount?: number,          // 4. minCount (OPCIONAL)
    includeTemporal?: boolean,  // 5. includeTemporal (OPCIONAL)
    geoFilters?: GeographicFilters // 6. geoFilters (OPCIONAL)
  ) => void | Promise<any>;
  
  onDepartamentoChange: (departamento: string | null) => void;
  onMunicipioChange: (municipio: string | null) => void;
  onIpsChange: (ips: string | null) => void;
  resetGeographicFilters: () => void;
}
