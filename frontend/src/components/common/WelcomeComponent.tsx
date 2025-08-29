// src/components/common/WelcomeComponent.tsx
import React from 'react';
import type { WelcomeComponentProps } from '../../types/api.types';

export const WelcomeComponent: React.FC<WelcomeComponentProps> = ({ isMobile }) => {
  return (
    <div className="welcome-container">
      <div className="welcome-content">
        <div className="welcome-icon">📊</div>
        <h2 className="welcome-title">
          {isMobile ? '¡Bienvenido!' : '¡Bienvenido al Procesador de Archivos!'}
        </h2>
        <p className="welcome-description">
          {isMobile
            ? 'Usa el menú para comenzar.'
            : 'Selecciona una herramienta del menú lateral para manipular tus archivos.'}
        </p>
      </div>
    </div>
  );
};
