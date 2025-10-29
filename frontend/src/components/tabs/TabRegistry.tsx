// src/components/tabs/TabRegistry.tsx
import { DownloadOutlined, SwapOutlined, HomeOutlined, FileTextOutlined } from '@ant-design/icons';
import type { TabConfig } from '../../types/api.types';

import { TransformTab } from './transformTab/TransformTab';
import { ExportTab } from './exportTab/ExportTab';
// EvalNoteBotELIMINAR: import { ChatTab } from './chartTab/ChatTab';
import { CrossTab } from './crossTab/CrossTab';
import { TransformOutlined } from '@mui/icons-material';
import { WelcomeTab } from './welcomeTab/WelcomeTab';
import TechnicalNoteViewer from '../technical_note/TechnicalNoteViewer';

export const TAB_REGISTRY: Record<string, TabConfig> = {
  welcome: {  
    key: 'welcome',
    label: 'Inicio',
    icon: <HomeOutlined />,
    component: WelcomeTab,
    requiresFile: false,
  },
  transform: {
    key: 'transform',
    label: 'Transformar',
    icon: <TransformOutlined />,
    component: TransformTab,
    requiresFile: false,
  },
  export: {
    key: 'export',
    label: 'Exportar',
    icon: <DownloadOutlined />,
    component: ExportTab,
    requiresFile: true,
  },
  cross: {
    key: 'cross',
    label: 'Cruce',
    icon: <SwapOutlined />,
    component: CrossTab,
    requiresFile: false,
  },
  technical_note: {
    key: 'technical_note',
    label: 'Nota técnica',
    icon: <FileTextOutlined />,
    component: TechnicalNoteViewer,
    requiresFile: false,
  }
};

export const getAvailableTabs = (): TabConfig[] => {
  return Object.values(TAB_REGISTRY);
};

export const getTabConfig = (key: string): TabConfig | undefined => {
  return TAB_REGISTRY[key];
};
