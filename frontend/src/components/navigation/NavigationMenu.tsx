// src/components/navigation/NavigationMenu.tsx
import React, { useMemo } from 'react';
import { Menu } from 'antd';
import { useNavigate } from 'react-router-dom';
import { getAvailableTabs } from '../tabs/TabRegistry';

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
  onSelect,
}) => {
  const navigate = useNavigate();

  // Genera items dinámicamente desde el TAB_REGISTRY
  const items = useMemo(() => {
    const availableTabs = getAvailableTabs();
    
    return availableTabs
      .filter(tab => {
        // Solo muestra los tabs principales en el menú lateral
        const mainTabs = ['welcome', 'transform', 'technical_note', 'upload', 'chat'];
        return mainTabs.includes(tab.key);
      })
      .map(tab => ({
        key: tab.key,
        icon: tab.icon,
        label: isMobile ? tab.label : tab.label,
        onClick: () => {
          navigate(`/${tab.key}`);
          onSelect(tab.key);
        }
      }));
  }, [isMobile, navigate, onSelect]);

  return (
    <Menu
      mode={layout}
      selectedKeys={[activeKey]}
      items={items}
      className={layout === 'inline' ? 'sidebar-menu' : 'mobile-sidebar-menu'}
    />
  );
};

export default NavigationMenu;