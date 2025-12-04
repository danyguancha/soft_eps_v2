// config/reportKeywords.config.ts
import { 
  MedicineBoxOutlined,
  HeartOutlined,
  SmileOutlined,
  HeartFilled,
} from '@ant-design/icons';
import type { ReactNode } from 'react';

export interface KeywordConfig {
  key: string;
  label: string;
  color: string;
  icon?: ReactNode;
  synonyms?: string[];
  searchTerms?: string[];
}

//ICONOS PRE-RENDERIZADOS - CRÍTICO PARA PERFORMANCE
const MEDICINE_ICON = <MedicineBoxOutlined />;
const HEART_ICON = <HeartOutlined />;
const SMILE_ICON = <SmileOutlined />;
const HEART_FILLED_ICON = <HeartFilled />;

//CONFIGURACIÓN OPTIMIZADA CON TUS PALABRAS CLAVE EXACTAS
export const REPORT_KEYWORDS: KeywordConfig[] = [
  {
    key: 'medicina',
    label: 'Medicina',
    color: '#1890ff',
    icon: MEDICINE_ICON,
    synonyms: ['medicina', 'médica', 'médico', 'medical'],
    searchTerms: ['medicina', 'medica', 'medico', 'medical']
  },
  {
    key: 'enfermería',
    label: 'Enfermería', 
    color: '#52c41a',
    icon: HEART_ICON,
    synonyms: ['enfermería', 'enfermería', 'enfermero', 'nurse'],
    searchTerms: ['enfermería', 'enfermería', 'enfermero', 'nurse']
  },
  {
    key: 'odontológica',
    label: 'Odontología',
    color: '#722ed1',
    icon: SMILE_ICON,
    synonyms: ['odontologia', 'odontológica', 'dental', 'dentista', 'odontolígica'],
    searchTerms: ['odontologia', 'odontológica', 'dental', 'dentista', 'dientes']
  },
  {
    key: 'flúor',
    label: 'Flúor',
    color: '#22c0f5ff',
    icon: HEART_FILLED_ICON,
    synonyms: ['fluor', 'flúor', 'barniz'],
    searchTerms: ['fluor', 'flúor', 'barniz']
  },
  {
    key: 'placa',
    label: 'Placa',
    color: '#fa541cff',
    icon: SMILE_ICON,
    synonyms: ['placa', 'profilaxis', 'limpieza', 'bacteriana'],
    searchTerms: ['placa', 'profilaxis', 'limpieza', 'bacteriana']
  },
  {
    key: 'detartraje',
    label: 'Detartraje',
    color: '#fa541cff',
    icon: SMILE_ICON, 
    synonyms: ['detartraje', 'profilaxis', 'limpieza'],
    searchTerms: ['detartraje', 'profilaxis', 'limpieza' ]
  },
  {
    key: 'sellantes',
    label: 'Sellantes',
    color: '#b0c604ff',
    icon: SMILE_ICON, 
    synonyms: ['sellantes', 'selladores', 'Sellantes'],
    searchTerms: ['sellante', 'selladores', 'Sellantes']
  },
  {
    key: 'micronutrientes',
    label: 'Micronutrientes',
    color: '#c67204ff',
    icon: SMILE_ICON,
    synonyms: ['micronutrientes', 'micro', 'Micronutrientes'],
    searchTerms: ['micronutrientes', 'micro', 'Micronutrientes', 'micronutriente']
  },
  {
    key: 'vitamina_a',
    label: 'Vitamina A',
    color: '#14c604ff',
    icon: SMILE_ICON,
    synonyms: ['Vitamina A', 'vitamina a', 'vit A', 'Vit A', 'vit. a'],
    searchTerms: ['Vitamina A', 'vitamina a', 'vit A', 'Vit A', 'vit. a']
  },
  {
    key: 'sulfato_ferroso',
    label: 'Sulfato Ferroso',
    color: '#595b59ff',
    icon: SMILE_ICON,
    synonyms: ['Sulfato Ferroso', 'Sulfato ferroso', 'sulf ferroso', 'hierro', 'sulfato Ferroso'],
    searchTerms: ['Sulfato Ferroso', 'Sulfato ferroso', 'sulf ferroso', 'hierro', 'sulfato Ferroso']
  },
  {
    key: 'diu',
    label: 'Dispositivo intrauterino',
    color: '#595b59ff',
    icon: SMILE_ICON,
    synonyms: ['diu', 'DIU', 'intrauterino', 'Dispositivo intrauterino', 'Dispositivo Intrauterino'],
    searchTerms: ['diu', 'DIU', 'intrauterino', 'Dispositivo intrauterino', 'Dispositivo Intrauterino']
  },
  {
    key: 'subdermico',
    label: 'Implante subdermico',
    color: '#595b59ff',
    icon: SMILE_ICON,
    synonyms: ['subdermico', 'Subdermico', 'Implante subdermico', 'implante Subdérmico', 'Implante Subdérmico'],
    searchTerms: ['subdermico', 'Subdermico', 'Implante subdermico', 'implante Subdérmico', 'Implante Subdérmico']
  }
];

export const SELECT_OPTIONS = REPORT_KEYWORDS.map(keyword => ({
  key: keyword.key,
  value: keyword.key,
  label: keyword.label,
}));

export const getKeywordConfig = (key: string): KeywordConfig | undefined => {
  return REPORT_KEYWORDS.find(kw => kw.key === key);
};

export const getAllKeywordKeys = (): string[] => {
  return REPORT_KEYWORDS.map(kw => kw.key);
};

export const getAllKeywordLabels = (): string[] => {
  return REPORT_KEYWORDS.map(kw => kw.label);
};

export const getKeywordColor = (key: string): string => {
  const config = getKeywordConfig(key);
  return config?.color || '#595959';
};

export const getKeywordLabel = (key: string): string => {
  const config = getKeywordConfig(key);
  return config?.label || key.charAt(0).toUpperCase() + key.slice(1);
};

export const getKeywordIcon = (key: string): ReactNode | undefined => {
  const config = getKeywordConfig(key);
  return config?.icon;
};

export const DEFAULT_KEYWORDS = ['medicina'];
