// config/ageGroups.config.tsx

import type { AgeGroupIcon } from '../types/FileTypes'

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

export const convertUploadedFilesToGroups = (files: any[]): AgeGroupIcon[] => {
  return files.map(file => ({
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
