// src/App.tsx
import '@ant-design/v5-patch-for-react-19';
import React, { useState } from 'react';
import { BrowserRouter, Routes, Route, useLocation, useNavigate } from 'react-router-dom';
import { Layout, Alert, Spin, Drawer, Modal } from 'antd';
import { Grid } from 'antd';

import { AppHeader } from './components/layout/AppHeader';
import { NavigationMenu } from './components/navigation/NavigationMenu';
import FileCrossManager from './components/cross/FileCrossManager';
import { ErrorBoundary } from './components/common/ErrorBoundary';

// Importa el nuevo componente de routing dinámico
import { DynamicTabRouter } from './components/routing/DynamicTabRouter';

import { useFileOperations } from './hooks/useFileOperations';
import { CrossDataProvider, useCrossDataContext } from './contexts/CrossDataContext';
import type { TabKey } from './types/api.types';
import 'antd/dist/reset.css';
import './App.css';

const { Header, Content, Sider, Footer } = Layout;
const { useBreakpoint } = Grid;

interface UIState {
  collapsed: boolean;
  mobileMenuVisible: boolean;
  crossModalVisible: boolean;
}

// Componente principal de la aplicación
const AppContent: React.FC = () => {
  const screens = useBreakpoint();
  const location = useLocation();
  const navigate = useNavigate();
  
  const isMobile = !(screens.md ?? false);
  const isTablet = !!(screens.md && !screens.lg);

  const fileOperations = useFileOperations();
  const crossData = useCrossDataContext();

  const [ui, setUI] = useState<UIState>({
    collapsed: isMobile,
    mobileMenuVisible: false,
    crossModalVisible: false,
  });

  // Determina la clave activa basada en la ruta actual
  const getActiveKey = (): TabKey => {
    const path = location.pathname.slice(1) || 'welcome'; // Remueve '/' inicial
    return path as TabKey;
  };

  const handleMenuSelect = (key: string) => {
    setUI((p) => ({ ...p, mobileMenuVisible: false }));
    navigate(`/${key}`);
  };

  const handleToggleSidebar = () =>
    setUI((p) =>
      isMobile 
        ? { ...p, mobileMenuVisible: !p.mobileMenuVisible } 
        : { ...p, collapsed: !p.collapsed }
    );

  return (
    <ErrorBoundary>
      <div className={`app-container ${isMobile ? 'mobile' : isTablet ? 'tablet' : 'desktop'}`}>
        <Layout className="main-layout">
          <Header className="app-header">
            <AppHeader
              isMobile={isMobile}
              isTablet={isTablet}
              collapsed={ui.collapsed}
              currentFile={fileOperations.currentFile}
              onToggleSidebar={handleToggleSidebar}
            />
          </Header>

          <Layout className="inner-layout">
            {!isMobile && (
              <Sider
                width={200}
                className="app-sider"
                collapsedWidth={80}
                collapsed={ui.collapsed}
              >
                <NavigationMenu
                  layout="inline"
                  isMobile={false}
                  activeKey={getActiveKey()}
                  currentFile={null}
                  onSelect={handleMenuSelect}
                />
              </Sider>
            )}

            <Layout className="content-layout">
              <Content className="app-content">
                {fileOperations.error && (
                  <Alert
                    type="error"
                    message="Error"
                    description={fileOperations.error}
                    closable
                    className="error-alert"
                    onClose={() => fileOperations.setError(null)}
                  />
                )}

                <div className="main-content">
                  {fileOperations.loading && !fileOperations.currentData && getActiveKey() === 'upload' ? (
                    <div className="loading-container">
                      <Spin size={isMobile ? 'default' : 'large'} />
                    </div>
                  ) : (
                    <Routes>
                      {/* Ruta por defecto */}
                      <Route path="/" element={<DynamicTabRouter tabKey="welcome" />} />
                      
                      {/* Rutas dinámicas para cada tab del registro */}
                      <Route path="/welcome" element={<DynamicTabRouter tabKey="welcome" />} />
                      <Route path="/upload" element={<DynamicTabRouter tabKey="upload" />} />
                      <Route 
                        path="/transform" 
                        element={
                          <DynamicTabRouter 
                            tabKey="transform" 
                            onOpenCrossModal={() => setUI(p => ({ ...p, crossModalVisible: true }))}
                          />
                        } 
                      />
                      <Route path="/export" element={<DynamicTabRouter tabKey="export" />} />
                      <Route path="/chat" element={<DynamicTabRouter tabKey="chat" />} />
                      
                      {/* ✅ ÚNICA RUTA QUE NECESITA LAS PROPS DE CROSS */}
                      <Route 
                        path="/cross" 
                        element={
                          <DynamicTabRouter 
                            tabKey="cross"
                            crossResult={crossData.crossResult}
                            processedCrossData={crossData.processedCrossData}
                            crossDataTotal={crossData.crossDataTotal}
                            onExportCrossResult={crossData.handleExportCrossResult}
                            onClearCrossResult={crossData.handleClearCrossResult}
                          />
                        } 
                      />
                      
                      <Route path="/technical_note" element={<DynamicTabRouter tabKey="technical_note" />} />
                      
                      {/* Rutas anidadas para nota técnica con grupos etarios */}
                      <Route path="/technical_note/:ageGroup" element={<DynamicTabRouter tabKey="technical_note" />} />
                      
                      {/* Ruta 404 */}
                      <Route path="*" element={<div>Página no encontrada</div>} />
                    </Routes>
                  )}
                </div>
              </Content>
              
              <Footer className="app-footer">
                {isMobile ? 'Procesador ©2025' : 'Evaluación de nota técnica ©2025'}
              </Footer>
            </Layout>
          </Layout>

          {/* Drawer móvil */}
          <Drawer
            title="Menú"
            placement="left"
            width={280}
            open={isMobile && ui.mobileMenuVisible}
            onClose={() => setUI((p) => ({ ...p, mobileMenuVisible: false }))}
            className="mobile-menu-drawer"
          >
            <NavigationMenu
              layout="vertical"
              isMobile
              activeKey={getActiveKey()}
              currentFile={null}
              onSelect={handleMenuSelect}
            />
          </Drawer>

          {/* Modal para cruce de archivos */}
          <Modal
            title="🔄 Cruzar Archivos - VLOOKUP"
            open={ui.crossModalVisible}
            onCancel={() => setUI((p) => ({ ...p, crossModalVisible: false }))}
            footer={null}
            width="85%"
            style={{ top: 20 }}
            className="cross-modal"
          >
            <FileCrossManager
              availableFiles={fileOperations.files || []}
              onRefreshFiles={fileOperations.loadFiles}
              onCrossComplete={(result) => {
                console.log('🎯 Cruce completado, resultado:', result);
                
                // Actualizar estado
                crossData.handleCrossComplete(result);
                
                // Cerrar modal
                setUI(p => ({ ...p, crossModalVisible: false }));
                
                // Solo navegar si NO fue descarga automática
                if (!result.download_completed) {
                  setTimeout(() => {
                    navigate('/cross');
                  }, 300);
                }
              }}
              onComplete={() => setUI((p) => ({ ...p, crossModalVisible: false }))}
            />
          </Modal>
        </Layout>
      </div>
    </ErrorBoundary>
  );
};

// Componente App principal con BrowserRouter
const App: React.FC = () => {
  return (
    <BrowserRouter>
      <CrossDataProvider>
      <AppContent />
      </CrossDataProvider>
    </BrowserRouter>
  );
};

export default App;
