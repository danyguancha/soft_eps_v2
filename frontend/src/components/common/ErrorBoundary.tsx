// src/components/common/ErrorBoundary.tsx
import React from 'react';
import { Alert, Button } from 'antd';
import type { ErrorBoundaryState } from '../../types/api.types';

export class ErrorBoundary extends React.Component<
  { children: React.ReactNode },
  ErrorBoundaryState
> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(err: Error): ErrorBoundaryState {
    return { hasError: true, error: err };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('Error capturado por ErrorBoundary:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="error-boundary">
          <Alert
            type="error"
            showIcon
            message="Algo salió mal"
            description={this.state.error?.message}
            action={
              <Button
                size="small"
                danger
                onClick={() => this.setState({ hasError: false, error: null })}
              >
                Reintentar
              </Button>
            }
          />
        </div>
      );
    }
    return this.props.children;
  }
}
