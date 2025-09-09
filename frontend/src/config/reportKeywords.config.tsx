// config/reportKeywords.config.ts - ✅ OPTIMIZADA PARA PERFORMANCE
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

// ✅ ICONOS PRE-RENDERIZADOS - CRÍTICO PARA PERFORMANCE
const MEDICINE_ICON = <MedicineBoxOutlined />;
const HEART_ICON = <HeartOutlined />;
const SMILE_ICON = <SmileOutlined />;
const HEART_FILLED_ICON = <HeartFilled />;

// ✅ CONFIGURACIÓN OPTIMIZADA CON TUS PALABRAS CLAVE EXACTAS
export const REPORT_KEYWORDS: KeywordConfig[] = [
  {
    key: 'medicina',
    label: 'Medicina',
    color: '#1890ff',
    icon: MEDICINE_ICON, // ✅ Icono estático pre-renderizado
    synonyms: ['medicina', 'médica', 'médico', 'medical'],
    searchTerms: ['medicina', 'medica', 'medico', 'medical']
  },
  {
    key: 'enfermería',
    label: 'Enfermería', 
    color: '#52c41a',
    icon: HEART_ICON, // ✅ Icono estático pre-renderizado
    synonyms: ['enfermería', 'enfermería', 'enfermero', 'nurse'],
    searchTerms: ['enfermería', 'enfermería', 'enfermero', 'nurse']
  },
  {
    key: 'odontológica',
    label: 'Odontología',
    color: '#722ed1',
    icon: SMILE_ICON, // ✅ Icono estático pre-renderizado
    synonyms: ['odontologia', 'odontológica', 'dental', 'dentista', 'odontolígica'],
    searchTerms: ['odontologia', 'odontológica', 'dental', 'dentista', 'dientes']
  },
  {
    key: 'flúor',
    label: 'Flúor',
    color: '#22c0f5ff',
    icon: HEART_FILLED_ICON, // ✅ Icono estático pre-renderizado
    synonyms: ['fluor', 'flúor', 'barniz'],
    searchTerms: ['fluor', 'flúor', 'barniz']
  },
  {
    key: 'placa',
    label: 'Placa',
    color: '#fa541cff',
    icon: SMILE_ICON, // ✅ Icono estático pre-renderizado
    synonyms: ['placa', 'profilaxis', 'limpieza', 'bacteriana'],
    searchTerms: ['placa', 'profilaxis', 'limpieza', 'bacteriana']
  },
  {
    key: 'detartraje',
    label: 'Detartraje',
    color: '#fa541cff',
    icon: SMILE_ICON, // ✅ Icono estático pre-renderizado
    synonyms: ['detartraje', 'profilaxis', 'limpieza'],
    searchTerms: ['detartraje', 'profilaxis', 'limpieza' ]
  }
];

// ✅ OPCIONES PRE-MEMOIZADAS PARA SELECT - MUY IMPORTANTE PARA PERFORMANCE
export const SELECT_OPTIONS = REPORT_KEYWORDS.map(keyword => ({
  key: keyword.key,
  value: keyword.key,
  label: keyword.label,
}));

// ✅ UTILIDADES PARA ACCESO A LA CONFIGURACIÓN
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

// ✅ CONFIGURACIÓN POR DEFECTO
export const DEFAULT_KEYWORDS = ['medicina'];
