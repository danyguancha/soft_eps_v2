// src/components/AlertModal.tsx
import React from 'react';
import './AlertModal.css';

export type AlertVariant = 'info' | 'success' | 'error' | 'warning';

export interface AlertOptions {
  title: string;
  message: string;
  variant?: AlertVariant;
  actions?: { label: string; onClick: () => void; type?: 'primary' | 'secondary' }[];
}

export interface AlertModalProps extends AlertOptions {
  open: boolean;
  onClose: () => void;
}

const VARIANT_STYLES: Record<AlertVariant, string> = {
  info: 'alert-info',
  success: 'alert-success',
  error: 'alert-error',
  warning: 'alert-warning',
};

export const AlertModal: React.FC<AlertModalProps> = ({
  open, title, message, variant = 'info', actions = [], onClose,
}) => {
  if (!open) return null;
  
  return (
    <div className="alert-modal-backdrop" onClick={onClose}>
      <div className={`alert-modal ${VARIANT_STYLES[variant]}`} onClick={e => e.stopPropagation()}>
        <div className="alert-modal-header">
          <span className="alert-modal-title">{title}</span>
          <button className="alert-modal-close" onClick={onClose} aria-label="Close">×</button>
        </div>
        <div className="alert-modal-body">{message}</div>
        <div className="alert-modal-footer">
          {actions.length > 0
            ? actions.map((action, idx) => (
                <button
                  key={idx}
                  className={
                    'alert-modal-action ' + (action.type === 'primary' ? 'primary' : 'secondary')
                  }
                  onClick={() => {
                    action.onClick();
                    onClose();
                  }}
                >
                  {action.label}
                </button>
              ))
            : (<button className="alert-modal-action primary" onClick={onClose}>Cerrar</button>)}
        </div>
      </div>
    </div>
  );
};
