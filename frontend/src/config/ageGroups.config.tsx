// config/ageGroups.config.tsx

import type { AgeGroupIcon, CustomUploadedFile } from '../types/FileTypes';

import primInfanciaIcon from '../assets/icons/prim-inf.png';
import infanciaIcon from '../assets/icons/infancia.png';
import adolescenciaIcon from '../assets/icons/adolescencia.png';
import juventudIcon from '../assets/icons/juventud.png';
import adultezIcon from '../assets/icons/adulto.png';
import vejezIcon from '../assets/icons/vejez.png';
import archivoNuevoIcon from '../assets/icons/archivo_nuevo.png';

const createIcon = (src: string, alt: string) => (
  <img
    src={src}
    alt={alt}
    style={{
      width: '32px',
      height: '32px',
      objectFit: 'contain',
      filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))'
    }}
  />
);

// ============================================
// MAPEO DE ARCHIVOS PREDEFINIDOS
// ============================================

/**
 * Mapeo de nombres de archivo predefinidos a sus claves de grupo
 */
export const PREDEFINED_FILE_MAPPING: Record<string, string> = {
  'PrimeraInfanciaNueva.csv': 'primera-infancia',
  'InfanciaNueva.csv': 'infancia',
  'AdolescenciaNueva.csv': 'adolescencia',
  'JuventudNueva.csv': 'juventud',
  'AdultezNueva.csv': 'adulto',
  'VejezNueva.csv': 'vejez'
};

/**
 * Array de nombres de archivos predefinidos
 */
export const PREDEFINED_FILENAMES = Object.keys(PREDEFINED_FILE_MAPPING);

// ============================================
// GRUPOS ETARIOS BASE
// ============================================

export const BASE_AGE_GROUPS: AgeGroupIcon[] = [
  {
    key: 'primera-infancia',
    displayName: 'Primera Infancia',
    icon: createIcon(primInfanciaIcon, 'Primera Infancia'),
    color: '#ff7a45',
    description: 'Datos de primera infancia (0-5 años)',
    filename: 'PrimeraInfanciaNueva.csv',
    isCustomFile: false
  },
  {
    key: 'infancia',
    displayName: 'Infancia',
    icon: createIcon(infanciaIcon, 'Infancia'),
    color: '#40a9ff',
    description: 'Datos de población infantil (6-11 años)',
    filename: 'InfanciaNueva.csv',
    isCustomFile: false
  },
  {
    key: 'adolescencia',
    displayName: 'Adolescencia',
    icon: createIcon(adolescenciaIcon, 'Adolescencia'),
    color: '#73d13d',
    description: 'Datos de población adolescente (12-17 años)',
    filename: 'AdolescenciaNueva.csv',
    isCustomFile: false
  },
  {
    key: 'juventud',
    displayName: 'Juventud',
    icon: createIcon(juventudIcon, 'Juventud'),
    color: '#ffc53d',
    description: 'Datos de población joven (18-28 años)',
    filename: 'JuventudNueva.csv',
    isCustomFile: false
  },
  {
    key: 'adulto',
    displayName: 'Adultez',
    icon: createIcon(adultezIcon, 'Adultez'),
    color: '#9254de',
    description: 'Datos de población adulta (29-59 años)',
    filename: 'AdultezNueva.csv',
    isCustomFile: false
  },
  {
    key: 'vejez',
    displayName: 'Vejez',
    icon: createIcon(vejezIcon, 'Vejez'),
    color: '#f759ab',
    description: 'Datos de población adulto mayor (60+ años)',
    filename: 'VejezNueva.csv',
    isCustomFile: false
  }
];

export const CUSTOM_FILE_COLOR = '#722ed1';

export const createCustomFileIcon = () => createIcon(archivoNuevoIcon, 'Archivo Personalizado');

// ============================================
// FUNCIONES AUXILIARES
// ============================================

/**
 * Verifica si un archivo es predefinido del sistema
 */
export const isPredefinedFile = (filename: string): boolean => {
  return PREDEFINED_FILENAMES.includes(filename);
};

/**
 * Obtiene la clave del grupo predefinido para un archivo
 * @returns La clave del grupo o null si no es predefinido
 */
export const getPredefinedGroupKey = (filename: string): string | null => {
  return PREDEFINED_FILE_MAPPING[filename] || null;
};

/**
 * Convierte archivos subidos a grupos de edad personalizados
 * SOLO convierte archivos que NO son predefinidos del sistema
 */
export const convertUploadedFilesToGroups = (files: CustomUploadedFile[]): AgeGroupIcon[] => {
  return files
    .filter(file => !isPredefinedFile(file.filename)) // ✅ Excluir archivos predefinidos
    .map(file => ({
      key: `custom-${file.uid}`,
      displayName: file.name.replace(/\.(csv|xlsx|xls)$/i, ''),
      icon: createCustomFileIcon(),
      color: CUSTOM_FILE_COLOR,
      description: `Archivo personalizado: ${file.name} (${(file.size / 1024 / 1024).toFixed(2)} MB)`,
      filename: file.filename,
      isCustomFile: true,
      fileSize: file.size,
      uploadedAt: file.uploadedAt
    }));
};

/**
 * Obtiene los grupos visibles basándose en archivos disponibles
 * SOLO muestra grids predefinidos si hay archivos cargados con esos nombres
 * @param availableFiles - Archivos disponibles del backend
 * @param uploadedFiles - Archivos subidos por el usuario
 * @returns Array de grupos visibles
 */
export const getVisibleGroups = (
  availableFiles: any[],
  uploadedFiles: CustomUploadedFile[]
): AgeGroupIcon[] => {
  console.log('🔍 getVisibleGroups - Calculando grupos visibles...');
  
  // Obtener todos los nombres de archivo disponibles
  const availableFilenames = availableFiles.map(f => f.filename);
  const uploadedFilenames = uploadedFiles.map(f => f.filename);
  const allAvailableFilenames = [...availableFilenames, ...uploadedFilenames];
  
  console.log('📂 Archivos disponibles totales:', allAvailableFilenames);
  
  // Filtrar grupos predefinidos que tienen archivos disponibles
  const visiblePredefinedGroups = BASE_AGE_GROUPS.filter(group => {
    const isAvailable = allAvailableFilenames.includes(group.filename!);
    if (isAvailable) {
      console.log(`✅ Grupo predefinido visible: ${group.displayName} (${group.filename})`);
    }
    return isAvailable;
  });
  
  // Obtener grupos personalizados (archivos que no son predefinidos)
  const customGroups = convertUploadedFilesToGroups(uploadedFiles);
  
  console.log(`📊 Grupos visibles: ${visiblePredefinedGroups.length} predefinidos + ${customGroups.length} personalizados`);
  
  return [...visiblePredefinedGroups, ...customGroups];
};

/**
 * Verifica si un archivo subido debe reemplazar un grid predefinido
 * @param filename - Nombre del archivo subido
 * @returns true si debe actualizar un grid predefinido existente
 */
export const shouldUpdatePredefinedGrid = (filename: string): boolean => {
  return isPredefinedFile(filename);
};
