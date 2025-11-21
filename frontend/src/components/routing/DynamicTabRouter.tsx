// src/components/routing/DynamicTabRouter.tsx
import React from 'react';
import { useParams } from 'react-router-dom';
import { TabRenderer } from '../tabs/TabRenderer';
import { getTabConfig } from '../tabs/TabRegistry';
import { useFileOperations } from '../../hooks/useFileOperations';
import { useCrossDataContext } from '../../contexts/CrossDataContext';
import { Grid } from 'antd';
import type { TabKey } from '../../types/api.types';

const { useBreakpoint } = Grid;

interface Props {
  tabKey: TabKey;
  onOpenCrossModal?: () => void;
  crossResult?: any;
  processedCrossData?: any[];
  crossDataTotal?: number;
  onExportCrossResult?: (format: 'csv' | 'xlsx') => Promise<void>;
  onClearCrossResult?: () => Promise<void>;
}

export const DynamicTabRouter: React.FC<Props> = ({ 
  tabKey, 
  onOpenCrossModal,
  crossResult,
  processedCrossData,
  crossDataTotal,
  onExportCrossResult,
  onClearCrossResult
}) => {
  const screens = useBreakpoint();
  const params = useParams();
  const fileOperations = useFileOperations();
  const crossData = useCrossDataContext();
  
  const isMobile = !(screens.md ?? false);
  const isTablet = !!(screens.md && !screens.lg);

  // Obtiene la configuración del tab desde el registro
  const tabConfig = getTabConfig(tabKey);

  if (!tabConfig) {
    return <div>Tab no encontrado: {tabKey}</div>;
  }

  // Para technical_note, si hay un parámetro ageGroup, lo manejamos
  const enhancedTabKey = tabKey === 'technical_note' && params.ageGroup 
    ? `${tabKey}_${params.ageGroup}` 
    : tabKey;

  // Usar props recibidas si existen, sino usar del hook local
  const finalCrossResult = crossResult ?? crossData.crossResult;
  const finalProcessedCrossData = processedCrossData ?? crossData.processedCrossData;
  const finalCrossDataTotal = crossDataTotal ?? crossData.crossDataTotal;

  console.log('🔍 DynamicTabRouter - Cross data:', {
    tabKey,
    hasCrossResult: !!finalCrossResult,
    processedDataLength: finalProcessedCrossData?.length,
    total: finalCrossDataTotal
  });

  return (
    <TabRenderer
      activeTab={enhancedTabKey as TabKey}
      fileData={fileOperations.currentFile}
      isMobile={isMobile}
      isTablet={isTablet}
      onTabChange={() => {}}
      onOpenCrossModal={onOpenCrossModal || (() => {})}
      crossResult={finalCrossResult}
      crossTableState={crossData.crossTableState}
      processedCrossData={finalProcessedCrossData}
      crossDataTotal={finalCrossDataTotal}
      onCrossPaginationChange={crossData.handleCrossPaginationChange}
      onCrossFiltersChange={crossData.handleCrossFiltersChange}
      onCrossSortChange={crossData.handleCrossSortChange}
      onCrossSearch={crossData.handleCrossSearch}
      onExportCrossResult={onExportCrossResult || crossData.handleExportCrossResult}
      onClearCrossResult={onClearCrossResult || crossData.handleClearCrossResult}
    />
  );
};
