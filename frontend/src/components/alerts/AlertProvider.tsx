// src/components/AlertProvider.tsx
import React, { createContext, useContext, useState, useCallback } from 'react';
import { type AlertOptions, AlertModal } from './AlertModal';

interface AlertContextType {
  showAlert: (options: AlertOptions) => Promise<void>;
}

const AlertContext = createContext<AlertContextType | undefined>(undefined);

export const useAlert = () => {
  const ctx = useContext(AlertContext);
  if (!ctx) throw new Error('useAlert debe usarse dentro de AlertProvider');
  return ctx;
};

// src/components/AlertProvider.tsx
export const AlertProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [alert, setAlert] = useState<{ options: AlertOptions; resolve: (() => void) | null } | null>(null);

  const showAlert = useCallback((options: AlertOptions) => {
    return new Promise<void>(resolve => {
      setAlert({ options, resolve });
    });
  }, []);

  const handleClose = useCallback(() => {
    if (alert?.resolve) alert.resolve();
    setAlert(null);
  }, [alert]);

  return (
    <AlertContext.Provider value={{ showAlert }}>
      {children}
      <AlertModal
        open={!!alert}
        title={alert?.options?.title || ''}
        message={alert?.options?.message || ''}
        variant={alert?.options?.variant || 'info'}
        actions={alert?.options?.actions || []}
        onClose={handleClose}
      />
    </AlertContext.Provider>
  );
};

