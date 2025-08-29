import React, { useMemo } from 'react';
import { Menu } from 'antd';
import {
  FormOutlined,
  RobotOutlined,
} from '@ant-design/icons';

type LayoutMode = 'inline' | 'vertical';

interface Props {
  layout: LayoutMode;
  isMobile: boolean;
  activeKey: string;
  currentFile: any;     
  onSelect: (key: string) => void;
}

export const NavigationMenu: React.FC<Props> = ({
  layout,
  isMobile,
  activeKey,
  currentFile,
  onSelect,
}) => {
  const items = useMemo(
    () => [
      {
        key: 'transform',
        icon: <FormOutlined />,
        label: isMobile ? 'Transform' : 'Transformar',
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
