import type { GeographicFilters, KeywordAgeReport } from "../../../../services/TechnicalNoteService";


export interface TemporalReportProps {
    keywordReport: KeywordAgeReport | null;
    loadingReport: boolean;
    showReport: boolean;
    hasReport: boolean;
    reportItemsCount: number;
    reportTotalRecords: number;
    selectedFile: string | null;
    reportKeywords: string[];
    reportMinCount: number;
    showTemporalData: boolean;
    // Filtros geográficos
    geographicFilters: GeographicFilters;
    departamentosOptions: string[];
    municipiosOptions: string[];
    ipsOptions: string[];
    loadingGeoFilters: {
        departamentos: boolean;
        municipios: boolean;
        ips: boolean;
    };
    // Handlers existentes
    onToggleReportVisibility: () => void;
    onRegenerateReport: () => void;
    onSetReportKeywords: (keywords: string[]) => void;
    onSetReportMinCount: (count: number) => void;
    onSetShowTemporalData: (show: boolean) => void;
    onLoadKeywordAgeReport: (
        filename: string, 
        keywords?: string[], 
        minCount?: number, 
        includeTemporal?: boolean,
        geographicFilters?: GeographicFilters
    ) => void;
    onAddKeyword: (keyword: string) => void;
    onRemoveKeyword: (keyword: string) => void;
    // Handlers geográficos
    onDepartamentoChange: (value: string | null) => void;
    onMunicipioChange: (value: string | null) => void;
    onIpsChange: (value: string | null) => void;
    resetGeographicFilters: () => void;
}