// src/App.tsx
import '@ant-design/v5-patch-for-react-19';
import React, { useState } from 'react';
import { Layout, Alert, Spin, Drawer, Modal } from 'antd';
import { Grid } from 'antd';

import { AppHeader } from './components/layout/AppHeader';
import { NavigationMenu } from './components/navigation/NavigationMenu';
import { TabRenderer } from './components/tabs/TabRenderer';
import FileCrossManager from './components/cross/FileCrossManager';
import { ErrorBoundary } from './components/common/ErrorBoundary';

import { useFileOperations } from './hooks/useFileOperations';
import { useCrossData } from './hooks/useCrossData';
import type { TabKey } from './types/api.types';
import 'antd/dist/reset.css';
import './App.css';

const { Header, Content, Sider, Footer } = Layout;
const { useBreakpoint } = Grid;

interface UIState {
  activeTab: TabKey;
  collapsed: boolean;
  mobileMenuVisible: boolean;
  crossModalVisible: boolean;
}

const App: React.FC = () => {
  const screens = useBreakpoint();
  
  const isMobile = !(screens.md ?? false);
  const isTablet = !!(screens.md && !screens.lg);

  const fileOperations = useFileOperations();
  const crossData = useCrossData();

  const [ui, setUI] = useState<UIState>({
    activeTab: 'welcome',
    collapsed: isMobile,
    mobileMenuVisible: false,
    crossModalVisible: false,
  });

  const handleTabChange = (key: string) =>
    setUI((p) => ({
      ...p,
      activeTab: key as TabKey,
      mobileMenuVisible: false,
    }));

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
              >
                <NavigationMenu
                  layout="inline"
                  isMobile={false}
                  activeKey={ui.activeTab}
                  currentFile={null}
                  onSelect={handleTabChange}
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
                  {fileOperations.loading && !fileOperations.currentData && ui.activeTab === 'upload' ? (
                    <div className="loading-container">
                      <Spin size={isMobile ? 'default' : 'large'} />
                    </div>
                  ) : (
                    <TabRenderer
                      activeTab={ui.activeTab}
                      fileData={fileOperations.currentFile}
                      isMobile={isMobile}
                      isTablet={isTablet}
                      onTabChange={handleTabChange}
                      onOpenCrossModal={() => setUI(p => ({ ...p, crossModalVisible: true }))}
                      // Props específicos para CrossTab
                      crossResult={crossData.crossResult}
                      crossTableState={crossData.crossTableState}
                      processedCrossData={crossData.processedCrossData}
                      crossDataTotal={crossData.crossDataTotal}
                      onCrossPaginationChange={crossData.handleCrossPaginationChange}
                      onCrossFiltersChange={crossData.handleCrossFiltersChange}
                      onCrossSortChange={crossData.handleCrossSortChange}
                      onCrossSearch={crossData.handleCrossSearch}
                      onExportCrossResult={crossData.handleExportCrossResult}
                      onClearCrossResult={crossData.handleClearCrossResult}
                    />
                  )}
                </div>
              </Content>
              
              <Footer className="app-footer">
                {isMobile ? 'Procesador ©2025' : 'Procesador de Archivos ©2025'}
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
              activeKey={ui.activeTab}
              currentFile={null}
              onSelect={handleTabChange}
            />
          </Drawer>

          {/* Modal para cruce de archivos */}
          <Modal
            title="🔄 Cruzar Archivos - VLOOKUP"
            open={ui.crossModalVisible}
            onCancel={() => setUI((p) => ({ ...p, crossModalVisible: false }))}
            footer={null}
            width="95%"
            style={{ top: 20 }}
            className="cross-modal"
          >
            <FileCrossManager
              availableFiles={fileOperations.files || []}
              onRefreshFiles={fileOperations.loadFiles}
              onCrossComplete={(result) => {
                crossData.handleCrossComplete(result);
                setUI(p => ({ ...p, crossModalVisible: false, activeTab: 'cross' }));
              }}
              onComplete={() => setUI((p) => ({ ...p, crossModalVisible: false }))}
            />
          </Modal>
        </Layout>
      </div>
    </ErrorBoundary>
  );
};

export default App;
