// src/components/layout/AppHeader.tsx
import React from 'react';
import { Button, Badge, Typography } from 'antd';
import {
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  HomeOutlined,
  EyeOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useCrossDataContext } from '../../contexts/CrossDataContext';

const { Title } = Typography;

interface Props {
  isMobile: boolean;
  isTablet: boolean | undefined;
  collapsed: boolean;
  currentFile: { original_name: string; total_rows: number } | null;
  onToggleSidebar: () => void;
}

export const AppHeader: React.FC<Props> = ({
  isMobile,
  isTablet,
  collapsed,
  currentFile,
  onToggleSidebar,
}) => {
  const navigate = useNavigate();
  const crossData = useCrossDataContext();
  
  // ✅ Verificar si hay resultados de cruce disponibles
  const hasCrossResults = crossData.crossResult && crossData.processedCrossData.length > 0;

  return (
    <div className="header-content" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
      <div className="header-left">
        <Button
          type="text"
          icon={collapsed || isMobile ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
          onClick={onToggleSidebar}
          className="sidebar-toggle"
        />

        <HomeOutlined className="home-icon" />

        <Title level={isMobile ? 4 : 3} className="app-title">
          {isMobile
            ? 'Procesador'
            : isTablet
            ? 'Procesador Archivos'
            : 'Evaluación de Nota Técnica'}
        </Title>
        
        {currentFile && (
          <Badge>
            <Button type="primary" ghost size={isMobile ? 'small' : 'middle'}>
              {isMobile && currentFile.original_name.length > 10
                ? `${currentFile.original_name.slice(0, 10)}…`
                : currentFile.original_name}
            </Button>
          </Badge>
        )}
      </div>

      {/* ✅ Botón para ver resultados del cruce - solo visible cuando hay resultados */}
      {hasCrossResults && (
        <div className="header-right" style={{ marginLeft: 'auto' }}>
          <Badge count={crossData.crossDataTotal} overflowCount={9999} showZero={false}>
            <Button
              type="primary"
              icon={<EyeOutlined />}
              onClick={() => navigate('/cross')}
              size={isMobile ? 'small' : 'middle'}
            >
              {!isMobile && 'Ver Cruce'}
            </Button>
          </Badge>
        </div>
      )}
    </div>
  );
};
