// src/App.tsx
import '@ant-design/v5-patch-for-react-19';
import React, { useState, useEffect } from 'react';
import { BrowserRouter, Routes, Route, useLocation, useNavigate } from 'react-router-dom';
import { Layout, Alert, Spin, Drawer, Modal, message, Grid } from 'antd';

import { AppHeader } from './components/layout/AppHeader';
import { NavigationMenu } from './components/navigation/NavigationMenu';
import FileCrossManager from './components/cross/FileCrossManager';
import { ErrorBoundary } from './components/common/ErrorBoundary';
import { DynamicTabRouter } from './components/routing/DynamicTabRouter';
import { ChatBot } from './components/chatbot/ChatBot';

import { useFileOperations } from './hooks/useFileOperations';
import { CrossDataProvider, useCrossDataContext } from './contexts/CrossDataContext';
import { healthMonitor } from './services/HealthMonitor';
import { TechnicalNoteService } from './services/TechnicalNoteService';
import type { TabKey } from './types/api.types';

import 'antd/dist/reset.css';
import './App.css';

const { Header, Content, Sider, Footer } = Layout;
const { useBreakpoint } = Grid;

interface UIState {
  collapsed: boolean;
  mobileMenuVisible: boolean;
  crossModalVisible: boolean;
  serverPort: number;
  serverStatus: 'online' | 'offline' | 'reconnecting';
}

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
    serverPort: 8000,
    serverStatus: 'online'
  });

  // ========== LIMPIEZA DE CACHE AL INICIAR LA APLICACIÓN ==========
  useEffect(() => {
    const initializeCache = async () => {
      try {
        console.log('Limpiando cache del backend al iniciar...');
        
        const cleanupResult = await TechnicalNoteService.cleanupAllCache();
        
        if (cleanupResult.success) {
          console.log('Cache limpiado exitosamente:');
          console.log(`Directorios limpiados: ${cleanupResult.cleaned_directories.join(', ')}`);
          console.log(`Tablas eliminadas: ${cleanupResult.tables_cleared}`);
          console.log(`Archivos técnicos eliminados: ${cleanupResult.technical_files_cleared}`);
          
          // Mensaje discreto de confirmación
          message.success('Sistema inicializado correctamente', 1.5);
        } else if (cleanupResult.errors && cleanupResult.errors.length > 0) {
          console.warn('Limpieza completada con algunos errores:', cleanupResult.errors);
          message.warning('Cache limpiado', 2);
        }
      } catch (error) {
        console.error('Error limpiando cache:', error);
        // No bloqueamos la aplicación si falla la limpieza
        message.warning('No se pudo limpiar el cache, continuando...', 2);
      }
    };

    // Ejecutar limpieza al montar el componente
    initializeCache();
  }, []); // Solo ejecutar una vez al montar

  // ========== HEALTH MONITOR SETUP ==========
  useEffect(() => {
    console.log('🚀 Iniciando monitoreo de servidor...');
    
    healthMonitor.start({
      onPortChange: (newPort, oldPort) => {
        console.log(`🔄 Puerto actualizado: ${oldPort} → ${newPort}`);
        
        setUI(prev => ({ 
          ...prev, 
          serverPort: newPort,
          serverStatus: 'online'
        }));
        
        // Notificación discreta
        message.info(`Servidor actualizado al puerto ${newPort}`, 2);
      },
      
      onServerDown: () => {
        console.log('Servidor no disponible, buscando...');
        
        setUI(prev => ({ 
          ...prev, 
          serverStatus: 'reconnecting'
        }));
      },
      
      onServerUp: (port) => {
        console.log(`Servidor reconectado en puerto ${port}`);
        
        setUI(prev => ({ 
          ...prev, 
          serverPort: port,
          serverStatus: 'online'
        }));
        
        message.success(`Conectado al servidor en puerto ${port}`, 2);
      }
    }, 10000); // Check cada 10 segundos
    
    // Cleanup al desmontar
    return () => {
      console.log('🛑 Deteniendo monitoreo de servidor...');
      healthMonitor.stop();
    };
  }, []);

  const getActiveKey = (): TabKey => {
    const path = location.pathname.slice(1) || 'welcome';
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
                {/* Alerta de estado del servidor (solo si está reconectando) */}
                {ui.serverStatus === 'reconnecting' && (
                  <Alert
                    type="warning"
                    message="Reconectando..."
                    description="Buscando servidor en puertos disponibles..."
                    showIcon
                    closable={false}
                    style={{ marginBottom: 16 }}
                  />
                )}

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
                      
                      {/* Rutas principales */}
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
                      
                      {/* Ruta de cruce */}
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
                      
                      {/* Rutas de nota técnica */}
                      <Route path="/technical_note" element={<DynamicTabRouter tabKey="technical_note" />} />
                      <Route path="/technical_note/:ageGroup" element={<DynamicTabRouter tabKey="technical_note" />} />
                      
                      {/* Ruta 404 */}
                      <Route path="*" element={<div>Página no encontrada</div>} />
                    </Routes>
                  )}
                </div>
              </Content>
              
              <Footer className="app-footer">
                <span>
                  {isMobile ? 'Procesador ©2025' : 'Evaluación de nota técnica ©2025'}
                </span>
                
                {/* Indicador discreto de puerto (opcional) */}
                <span style={{ 
                  marginLeft: '10px', 
                  fontSize: '11px', 
                  opacity: 0.5,
                  display: isMobile ? 'none' : 'inline'
                }}>
                  Puerto: {ui.serverPort}
                </span>
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
                crossData.handleCrossComplete(result);
                setUI(p => ({ ...p, crossModalVisible: false }));
                
                if (!result.download_completed) {
                  setTimeout(() => {
                    navigate('/cross');
                  }, 300);
                }
              }}
              onComplete={() => setUI((p) => ({ ...p, crossModalVisible: false }))}
            />
          </Modal>

          {/* CHATBOT FLOTANTE - DISPONIBLE EN TODAS LAS PÁGINAS */}
          <ChatBot fileContext={fileOperations.currentFile?.file_id} />
        </Layout>
      </div>
    </ErrorBoundary>
  );
};

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
