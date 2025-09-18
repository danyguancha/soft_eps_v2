// src/components/routing/DynamicTabRouter.tsx
import React from 'react';
import { useParams } from 'react-router-dom';
import { TabRenderer } from '../tabs/TabRenderer';
import { getTabConfig } from '../tabs/TabRegistry';
import { useFileOperations } from '../../hooks/useFileOperations';
import { useCrossData } from '../../hooks/useCrossData';
import { Grid } from 'antd';
import type { TabKey } from '../../types/api.types';

const { useBreakpoint } = Grid;

interface Props {
  tabKey: TabKey;
}

export const DynamicTabRouter: React.FC<Props> = ({ tabKey }) => {
  const screens = useBreakpoint();
  const params = useParams();
  const fileOperations = useFileOperations();
  const crossData = useCrossData();
  
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

  return (
    <TabRenderer
      activeTab={enhancedTabKey as TabKey}
      fileData={fileOperations.currentFile}
      isMobile={isMobile}
      isTablet={isTablet}
      onTabChange={() => {}} // No necesario ya que el routing lo maneja React Router
      onOpenCrossModal={() => {}} // Puedes implementar esto si es necesario
      // Props específicos para CrossTab
      crossResult={crossData.crossResult}
      crossTableState={crossData.crossTableState}
      processedCrossData={crossData.processedCrossData}
      crossDataTotal={crossData.crossDataTotal}
      onCrossPaginationChange={crossData.handleCrossPaginationChange}
      onCrossFiltersChange={crossData.handleCrossFiltersChange}
      onCrossSortChange={crossData.handleCrossSortChange}
      onCrossSearch={crossData.handleCrossSearch}
      onExportCrossResult={crossData.handleExportCrossResult}
      onClearCrossResult={crossData.handleClearCrossResult}
    />
  );
};
