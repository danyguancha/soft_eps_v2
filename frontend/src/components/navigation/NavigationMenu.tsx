import React, { useMemo } from 'react';
import { Menu } from 'antd';
import {
  UploadOutlined,
  FormOutlined,
  DownloadOutlined,
  RobotOutlined,
} from '@ant-design/icons';

type LayoutMode = 'inline' | 'vertical';

interface Props {
  layout: LayoutMode;
  isMobile: boolean;
  activeKey: string;
  currentFile: any;               // usar tu tipo FileData si está exportado
  onSelect: (key: string) => void;
}

export const NavigationMenu: React.FC<Props> = ({
  layout,
  isMobile,
  activeKey,
  currentFile,
  onSelect,
}) => {
  /* items generados según breakpoint y disponibilidad de archivo */
  const items = useMemo(
    () => [
      {
        key: 'upload',
        icon: <UploadOutlined />,
        label: isMobile ? 'Cargar' : 'Cargar Archivo',
      },
      {
        key: 'transform',
        icon: <FormOutlined />,
        label: isMobile ? 'Transform' : 'Transformar',
      },
      {
        key: 'export',
        icon: <DownloadOutlined />,
        label: isMobile ? 'Export' : 'Exportar',
     
      },
      {
        key: 'chat',
        icon: <RobotOutlined />,
        label: isMobile ? 'IA' : 'Asistente IA',
       
      },
    ],
    [currentFile, isMobile],
  );

  return (
    <Menu
      mode={layout}
      selectedKeys={[activeKey]}
      items={items}
      onSelect={({ key }) => onSelect(key)}
      className={layout === 'inline' ? 'sidebar-menu' : 'mobile-sidebar-menu'}
    />
  );
};
