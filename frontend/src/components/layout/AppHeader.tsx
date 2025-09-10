import React from 'react';
import { Button, Badge, Typography } from 'antd';
import {
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  HomeOutlined,
} from '@ant-design/icons';

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
}) => (
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
      <Badge
      >
        <Button type="primary" ghost size={isMobile ? 'small' : 'middle'}>
          {isMobile && currentFile.original_name.length > 10
            ? `${currentFile.original_name.slice(0, 10)}…`
            : currentFile.original_name}
        </Button>
      </Badge>
    )}
  </div>
);
