// src/components/alerts/AlertModal.tsx
import React, { useEffect } from 'react';
import './AlertModal.css';

export type AlertVariant = 'info' | 'success' | 'error' | 'warning';

export interface AlertOptions {
  title: string;
  message: string;
  variant?: AlertVariant;
  footer?: string; // HTML string, igual que SweetAlert2 footer
  actions?: { label: string; onClick: () => void; type?: 'primary' | 'secondary' }[];
}

export interface AlertModalProps extends AlertOptions {
  open: boolean;
  onClose: () => void;
}

// ─── Animated SVG Icons ──────────────────────────────────────────────────────

const SuccessIcon: React.FC = () => (
  <svg className="swal-icon swal-icon--success" viewBox="0 0 80 80" fill="none">
    <circle
      className="swal-icon__ring swal-icon__ring--success"
      cx="40" cy="40" r="36"
      strokeWidth="4"
    />
    <polyline
      className="swal-icon__check"
      points="20,43 33,56 60,25"
      strokeWidth="5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

const ErrorIcon: React.FC = () => (
  <svg className="swal-icon swal-icon--error" viewBox="0 0 80 80" fill="none">
    <circle
      className="swal-icon__ring swal-icon__ring--error"
      cx="40" cy="40" r="36"
      strokeWidth="4"
    />
    <line
      className="swal-icon__x swal-icon__x--first"
      x1="26" y1="26" x2="54" y2="54"
      strokeWidth="5"
      strokeLinecap="round"
    />
    <line
      className="swal-icon__x swal-icon__x--second"
      x1="54" y1="26" x2="26" y2="54"
      strokeWidth="5"
      strokeLinecap="round"
    />
  </svg>
);

const WarningIcon: React.FC = () => (
  <svg className="swal-icon swal-icon--warning" viewBox="0 0 80 80" fill="none">
    <circle
      className="swal-icon__ring swal-icon__ring--warning"
      cx="40" cy="40" r="36"
      strokeWidth="4"
    />
    <line
      className="swal-icon__exclaim-body"
      x1="40" y1="22" x2="40" y2="47"
      strokeWidth="5"
      strokeLinecap="round"
    />
    <circle
      className="swal-icon__exclaim-dot"
      cx="40" cy="57" r="3.5"
    />
  </svg>
);

const InfoIcon: React.FC = () => (
  <svg className="swal-icon swal-icon--info" viewBox="0 0 80 80" fill="none">
    <circle
      className="swal-icon__ring swal-icon__ring--info"
      cx="40" cy="40" r="36"
      strokeWidth="4"
    />
    <circle
      className="swal-icon__info-dot"
      cx="40" cy="23" r="3.5"
    />
    <line
      className="swal-icon__info-body"
      x1="40" y1="34" x2="40" y2="57"
      strokeWidth="5"
      strokeLinecap="round"
    />
  </svg>
);

const ICONS: Record<AlertVariant, React.ReactNode> = {
  success: <SuccessIcon />,
  error:   <ErrorIcon />,
  warning: <WarningIcon />,
  info:    <InfoIcon />,
};

// ─── Component ───────────────────────────────────────────────────────────────

export const AlertModal: React.FC<AlertModalProps> = ({
  open,
  title,
  message,
  variant = 'info',
  actions = [],
  footer,
  onClose,
}) => {
  // Bloquear scroll del body mientras el modal esté abierto
  useEffect(() => {
    if (!open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => { document.body.style.overflow = prev; };
  }, [open]);

  if (!open) return null;

  return (
    <div className="swal-backdrop" onClick={onClose} role="dialog" aria-modal="true">
      <div
        className={`swal-popup swal-popup--${variant}`}
        onClick={e => e.stopPropagation()}
      >
        {/* Botón cerrar */}
        <button className="swal-close-btn" onClick={onClose} aria-label="Cerrar">
          ×
        </button>

        {/* Ícono animado */}
        <div className="swal-icon-wrapper">
          {ICONS[variant]}
        </div>

        {/* Título */}
        <h2 className="swal-title">{title}</h2>

        {/* Mensaje */}
        <p className="swal-html-container">{message}</p>

        {/* Botones */}
        <div className="swal-actions">
          {actions.length > 0 ? (
            actions.map((action, idx) => (
              <button
                key={idx}
                className={`swal-btn ${
                  action.type === 'secondary'
                    ? 'swal-btn--deny'
                    : `swal-btn--confirm swal-btn--confirm-${variant}`
                }`}
                onClick={() => { action.onClick(); onClose(); }}
              >
                {action.label}
              </button>
            ))
          ) : (
            <button
              className={`swal-btn swal-btn--confirm swal-btn--confirm-${variant}`}
              onClick={onClose}
            >
              OK
            </button>
          )}
        </div>

        {/* Footer opcional (acepta HTML como SweetAlert2) */}
        {footer && (
          <div
            className="swal-footer"
            dangerouslySetInnerHTML={{ __html: footer }}
          />
        )}
      </div>
    </div>
  );
};
