// src/contexts/CrossDataContext.tsx
import React, { createContext, useContext, type ReactNode } from 'react';
import { useCrossData as useCrossDataHook } from '../hooks/useCrossData';
import type { UseCrossDataReturn } from '../types/api.types';

const CrossDataContext = createContext<UseCrossDataReturn | undefined>(undefined);

export const CrossDataProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const crossDataValue = useCrossDataHook();

  return (
    <CrossDataContext.Provider value={crossDataValue}>
      {children}
    </CrossDataContext.Provider>
  );
};

export const useCrossDataContext = () => {
  const context = useContext(CrossDataContext);
  if (!context) {
    throw new Error('useCrossDataContext must be used within CrossDataProvider');
  }
  return context;
};
