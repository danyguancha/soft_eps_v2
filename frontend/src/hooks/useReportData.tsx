// hooks/useReportData.ts
import { useMemo } from 'react';
import { getKeywordConfig, getKeywordLabel } from '../config/reportKeywords.config';


interface KeywordReport {
  totals_by_keyword: Record<string, number>;
  items: Array<{ keyword: string; [key: string]: any }>;
}

export const useReportData = (keywordReport: KeywordReport | null, reportKeywords: string[]) => {
  const keywordStats = useMemo(() => {
    if (!keywordReport?.totals_by_keyword) return [];

    return Object.entries(keywordReport.totals_by_keyword).map(([keyword, total]) => ({
      keyword,
      total,
      itemsCount: keywordReport.items.filter((item: any) => item.keyword === keyword).length,
      config: getKeywordConfig(keyword)
    }));
  }, [keywordReport]);

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

    return baseTitle;
  }, [reportKeywords]);

  return { keywordStats, reportTitle };
};
