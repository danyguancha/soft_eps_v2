// App.tsx - SOLUCIÓN DEFINITIVA PARA EL PROBLEMA DE SUPERPOSICIÓN
import React, { useEffect, useState, useCallback, useMemo } from 'react';
import {
  Layout,
  Menu,
  Card,
  Row,
  Col,
  Spin,
  Alert,
  Typography,
  Badge,
  Button,
  Space,
  message,
  Modal,
  Select,
  Drawer
} from 'antd';
import {
  FileOutlined,
  RobotOutlined,
  DownloadOutlined,
  UploadOutlined,
  DeleteOutlined,
  FormOutlined,
  EyeOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  HomeOutlined
} from '@ant-design/icons';

// Importar componentes
import { FileUploader } from './components/fileUploader/FileUploader';
import { DataTable } from './components/dataTable/DataTable';
import { ChatBot } from './components/chatbot/ChatBot';

// Importar hooks y servicios
import { useFileManager } from './hooks/useFileManager';
import type { DataRequest, FilterCondition, SortCondition, TransformRequest } from './types/api.types';
import { TransformService } from './services/TransformService';
import { ExportService } from './services/ExportService';
import { DeleteService } from './services/DeleteService';

// Importar estilos
import 'antd/dist/reset.css';
import './App.css';

const { Header, Content, Sider, Footer } = Layout;
const { Title, Text } = Typography;
const { Option } = Select;

// ===== TYPES =====
type TabKey = 'upload' | 'transform' | 'chat' | 'export' | 'settings';

interface FileData {
  file_id: string;
  original_name: string;
  columns: string[];
  sheets?: string[];
  total_rows: number;
}

interface UIState {
  activeTab: TabKey;
  collapsed: boolean;
  currentPage: number;
  pageSize: number;
  filters: FilterCondition[];
  sorting: SortCondition[];
  searchTerm: string;
  chatDrawerVisible: boolean;
  transformModalVisible: boolean;
  selectedTransform: string;
}

// ===== TYPE GUARDS =====
function isValidCurrentData(data: any): data is NonNullable<typeof data> {
  return data != null && typeof data === 'object' && Array.isArray(data.data);
}

// ===== LAYOUT STYLES CORREGIDOS - SIN BOX-SHADOW PROBLEMÁTICO =====
const layoutStyles = {
  main: {
    minHeight: '100vh',
    width: '100%',
    overflow: 'visible' as const,
    position: 'relative' as const,
    zIndex: 'auto' as const  // IMPORTANTE: Sin z-index forzado
  },
  header: {
    background: '#001529',
    padding: '0 16px',
    display: 'flex' as const,
    justifyContent: 'space-between' as const,
    alignItems: 'center' as const,
    width: '100%',
    position: 'sticky' as const,
    top: 0,
    zIndex: 1000,
    height: '64px',
    boxShadow: '0 2px 8px rgba(0, 0, 0, 0.15)'
  },
  sider: {
    background: '#fff',
    zIndex: 100,
    height: 'auto',
    boxShadow: '2px 0 8px rgba(0, 0, 0, 0.05)'
  },
  content: {
    background: '#f0f2f5',  // IMPORTANTE: Fondo consistente
    minHeight: 'calc(100vh - 64px)',
    width: '100%',
    padding: 0,
    margin: 0,
    position: 'relative' as const,
    flex: 1,
    zIndex: 'auto' as const  // IMPORTANTE: Sin z-index forzado
  },
  contentContainer: {
    width: '100%',
    maxWidth: '100%',
    padding: '16px',
    boxSizing: 'border-box' as const,
    overflow: 'visible' as const,
    minHeight: '100%',
    position: 'relative' as const,
    zIndex: 'auto' as const  // IMPORTANTE: Sin z-index forzado
  },
  // ===== CARD SIN BOX-SHADOW PROBLEMÁTICO =====
  contentCard: {
    width: '100%',
    maxWidth: '100%',
    marginBottom: '16px',
    overflow: 'visible' as const,
    position: 'relative' as const,
    borderRadius: '8px',  // Reducido
    // SIN BOX-SHADOW QUE CAUSE PROBLEMAS
    border: '1px solid #e8e8e8',  // Borde simple en lugar de sombra
    zIndex: 'auto' as const  // IMPORTANTE: Sin z-index forzado
  },
  footer: {
    textAlign: 'center' as const,
    background: '#fff',
    padding: '16px 24px',
    borderTop: '1px solid #e8e8e8',
    marginTop: '0',
    zIndex: 'auto' as const  // IMPORTANTE: Sin z-index forzado
  }
};

// ===== ERROR BOUNDARY COMPONENT =====
class ErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { hasError: boolean; error: Error | null }
> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('Error caught by boundary:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{ padding: '50px', textAlign: 'center' }}>
          <Alert
            message="Algo salió mal"
            description={this.state.error?.message || "Ha ocurrido un error inesperado"}
            type="error"
            showIcon
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

// ===== MAIN APP COMPONENT =====
const App: React.FC = () => {
  // ===== ESTADOS DEL FILE MANAGER =====
  const {
    files,
    currentFile,
    currentData,
    loading,
    error,
    loadFiles,
    loadFileData,
    deleteFile,
    setCurrentFile,
    setError
  } = useFileManager();

  // ===== ESTADOS DE LA UI AGRUPADOS =====
  const [uiState, setUIState] = useState<UIState>({
    activeTab: 'upload',
    collapsed: false,
    currentPage: 1,
    pageSize: 20,
    filters: [],
    sorting: [],
    searchTerm: '',
    chatDrawerVisible: false,
    transformModalVisible: false,
    selectedTransform: ''
  });

  // ===== ESTADO PARA PRESERVAR DATOS =====
  const [lastProcessedFile, setLastProcessedFile] = useState<FileData | null>(null);
  const [hasDataLoaded, setHasDataLoaded] = useState(false);

  // ===== MEMO PARA ITEMS DEL MENU =====
  const menuItems = useMemo(() => [
    {
      key: 'upload',
      icon: <UploadOutlined />,
      label: 'Cargar Archivo',
    },
    {
      key: 'transform',
      icon: <FormOutlined />,
      label: 'Transformar',
      disabled: !currentFile,
    },
    {
      key: 'export',
      icon: <DownloadOutlined />,
      label: 'Exportar',
      disabled: !currentFile,
    },
    {
      key: 'chat',
      icon: <RobotOutlined />,
      label: 'Asistente IA',
      disabled: !currentFile,
    },
  ], [currentFile]);

  // ===== VERIFICAR SI HAY DATOS PARA MOSTRAR =====
  const showTable = useMemo(() => {
    return !!(currentFile && isValidCurrentData(currentData) && currentData && currentData.data.length > 0);
  }, [currentFile, currentData]);

  // ===== VERIFICAR SI MOSTRAR MENSAJE DE BIENVENIDA =====
  const showWelcome = useMemo(() => {
    return uiState.activeTab === 'upload' && !showTable;
  }, [uiState.activeTab, showTable]);

  // ===== EFECTOS =====
  useEffect(() => {
    loadFiles();
  }, [loadFiles]);

  useEffect(() => {
    if (currentFile) {
      const request: DataRequest = {
        file_id: currentFile.file_id,
        page: uiState.currentPage,
        page_size: uiState.pageSize,
        filters: uiState.filters.length > 0 ? uiState.filters : undefined,
        sort: uiState.sorting.length > 0 ? uiState.sorting : undefined,
        search: uiState.searchTerm || undefined,
      };

      loadFileData(request);
      setHasDataLoaded(true);
      setLastProcessedFile(currentFile);
    }
  }, [currentFile, uiState.currentPage, uiState.pageSize, uiState.filters, uiState.sorting, uiState.searchTerm, loadFileData]);

  // ===== HANDLERS OPTIMIZADOS =====
  const handleUploadSuccess = useCallback((response: any) => {
    const newFile: FileData = {
      file_id: response.file_id,
      original_name: response.original_name || response.filename || 'Archivo cargado',
      columns: response.columns,
      sheets: response.sheets,
      total_rows: response.total_rows,
    };

    setCurrentFile(newFile);
    setLastProcessedFile(newFile);
    message.success('Archivo cargado exitosamente');
  }, [setCurrentFile]);

  const handlePaginationChange = useCallback((page: number, size: number) => {
    setUIState(prev => ({
      ...prev,
      currentPage: page,
      pageSize: size
    }));
  }, []);

  const handleDeleteRows = useCallback(async (indices: number[]) => {
    if (!currentFile) return;

    try {
      await DeleteService.deleteRows({
        file_id: currentFile.file_id,
        row_indices: indices
      });

      const request: DataRequest = {
        file_id: currentFile.file_id,
        page: uiState.currentPage,
        page_size: uiState.pageSize,
        filters: uiState.filters.length > 0 ? uiState.filters : undefined,
        sort: uiState.sorting.length > 0 ? uiState.sorting : undefined,
        search: uiState.searchTerm || undefined,
      };
      
      loadFileData(request);
      message.success(`${indices.length} filas eliminadas exitosamente`);
    } catch (error) {
      message.error('Error al eliminar filas');
    }
  }, [currentFile, uiState, loadFileData]);

  const handleTransform = useCallback(async () => {
    if (!currentFile || !uiState.selectedTransform) return;

    try {
      const transformRequest: TransformRequest = {
        file_id: currentFile.file_id,
        operation: uiState.selectedTransform as any,
        params: {}
      };

      await TransformService.transformData(transformRequest);
      message.success('Transformación aplicada exitosamente');
      
      setUIState(prev => ({ ...prev, transformModalVisible: false }));

      const request: DataRequest = {
        file_id: currentFile.file_id,
        page: uiState.currentPage,
        page_size: uiState.pageSize,
      };
      
      loadFileData(request);
    } catch (error) {
      message.error('Error al aplicar transformación');
    }
  }, [currentFile, uiState.selectedTransform, uiState.currentPage, uiState.pageSize, loadFileData]);

  const handleExport = useCallback(async (format: 'csv' | 'excel' | 'json') => {
    if (!currentFile) return;

    try {
      const exportRequest = {
        file_id: currentFile.file_id,
        format,
        filters: uiState.filters.length > 0 ? uiState.filters : undefined,
        sort: uiState.sorting.length > 0 ? uiState.sorting : undefined,
        search: uiState.searchTerm || undefined,
        include_headers: true
      };

      const response = await ExportService.exportData(exportRequest);
      message.success(`Archivo exportado: ${response.filename}`);
    } catch (error) {
      message.error('Error al exportar archivo');
    }
  }, [currentFile, uiState.filters, uiState.sorting, uiState.searchTerm]);

  const handleSelectExistingFile = useCallback((file: any) => {
    setCurrentFile(file);
    setLastProcessedFile(file);
    message.success(`Archivo "${file.original_name}" cargado`);
  }, [setCurrentFile]);

  const handleTabChange = useCallback((key: string) => {
    setUIState(prev => ({ ...prev, activeTab: key as TabKey }));
  }, []);

  const handleToggleSidebar = useCallback(() => {
    setUIState(prev => ({ ...prev, collapsed: !prev.collapsed }));
  }, []);

  // ===== RENDER FUNCTIONS =====
  const renderWelcomeMessage = () => (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      minHeight: '400px',
      padding: '20px',
      background: 'transparent'  // IMPORTANTE: Sin background que cause problemas
    }}>
      <div style={{ textAlign: 'center', maxWidth: '500px' }}>
        <div style={{
          width: '80px',
          height: '80px',
          background: 'linear-gradient(135deg, #1890ff 0%, #722ed1 100%)',
          borderRadius: '24px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          margin: '0 auto 24px',
          fontSize: '32px'
        }}>
          📊
        </div>
        <h2 style={{
          fontSize: '24px',
          fontWeight: 'bold',
          color: '#262626',
          marginBottom: '16px'
        }}>
          ¡Bienvenido al Procesador de Archivos!
        </h2>
        <p style={{
          fontSize: '16px',
          color: '#595959',
          lineHeight: '1.6'
        }}>
          Selecciona una herramienta del menú lateral para comenzar a procesar tus archivos Excel y CSV.
          Cada herramienta está diseñada para tareas específicas de manipulación de datos.
        </p>
      </div>
    </div>
  );

  const renderUploadTab = () => (
    <div style={layoutStyles.contentContainer}>
      {/* Mostrar mensaje de bienvenida si no hay datos */}
      {showWelcome && renderWelcomeMessage()}

      {/* Card del uploader - SIN BOX-SHADOW PROBLEMÁTICO */}
      <Card 
        title="Cargar Archivo Excel/CSV" 
        style={{
          ...layoutStyles.contentCard,
          background: '#fff',
          position: 'static'  // IMPORTANTE: static en lugar de relative
        }}
      >
        <div style={{ textAlign: 'center', padding: '40px' }}>
          <FileUploader
            onUploadSuccess={handleUploadSuccess}
            loading={loading}
          />
          <Text type="secondary" style={{ display: 'block', marginTop: '16px' }}>
            Formatos soportados: .csv, .xlsx, .xls
          </Text>
        </div>
      </Card>

      {/* Archivos disponibles - SIN BOX-SHADOW PROBLEMÁTICO */}
      {files && Array.isArray(files) && files.length > 0 && (
        <Card 
          title="Archivos Disponibles" 
          style={{
            ...layoutStyles.contentCard,
            background: '#fff',
            position: 'static'  // IMPORTANTE: static en lugar de relative
          }}
        >
          <Row gutter={[16, 16]}>
            {files.map((file) => (
              <Col xs={24} sm={12} lg={8} key={file.file_id}>
                <Card
                  size="small"
                  hoverable
                  style={{
                    border: currentFile?.file_id === file.file_id ? '2px solid #1890ff' : '1px solid #e8e8e8',
                    background: currentFile?.file_id === file.file_id ? '#f0f8ff' : '#fff',
                    borderRadius: '6px',
                    position: 'static'  // IMPORTANTE: static
                  }}
                  actions={[
                    <Button
                      key="select"
                      type={currentFile?.file_id === file.file_id ? "primary" : "link"}
                      icon={<EyeOutlined />}
                      onClick={() => handleSelectExistingFile(file)}
                    >
                      {currentFile?.file_id === file.file_id ? 'Seleccionado' : 'Seleccionar'}
                    </Button>,
                    <Button
                      key="delete"
                      type="link"
                      danger
                      icon={<DeleteOutlined />}
                      onClick={() => deleteFile(file.file_id)}
                    >
                      Eliminar
                    </Button>
                  ]}
                >
                  <Card.Meta
                    title={file.original_name}
                    description={
                      <div>
                        <Text>{file.total_rows?.toLocaleString() || 0} filas</Text>
                        <br />
                        <Text>{file.columns?.length || 0} columnas</Text>
                        {file.sheets && file.sheets.length > 0 && (
                          <>
                            <br />
                            <Text>{file.sheets.length} hojas</Text>
                          </>
                        )}
                      </div>
                    }
                  />
                </Card>
              </Col>
            ))}
          </Row>
        </Card>
      )}

      {/* ===== TABLA CORREGIDA - SIN SUPERPOSICIÓN ===== */}
      {showTable && currentData && (
        <Card
          title={
            <div style={{ 
              display: 'flex', 
              justifyContent: 'space-between', 
              alignItems: 'center',
              padding: '8px 0'
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                <span>📋 Datos: {currentFile?.original_name}</span>
                {lastProcessedFile?.file_id === currentFile?.file_id && (
                  <Badge count="Actual" style={{ backgroundColor: '#52c41a' }} />
                )}
              </div>
              <Space>
                <Badge count={currentData?.total || 0} style={{ backgroundColor: '#52c41a' }}>
                  <Text>Total registros</Text>
                </Badge>
                <Button
                  icon={<RobotOutlined />}
                  onClick={() => setUIState(prev => ({ ...prev, chatDrawerVisible: true }))}
                >
                  Asistente IA
                </Button>
              </Space>
            </div>
          }
          bodyStyle={{ 
            padding: '0',
            background: '#fff'  // IMPORTANTE: Background explícito
          }}
          style={{
            width: '100%',
            maxWidth: '100%',
            marginBottom: '16px',
            overflow: 'visible',
            background: '#fff',
            border: '1px solid #e8e8e8',
            borderRadius: '8px',
            position: 'static',  // IMPORTANTE: static para evitar stacking context
            zIndex: 'auto'       // IMPORTANTE: sin z-index forzado
          }}
        >
          {/* WRAPPER DE LA TABLA SIN PROBLEMAS DE SUPERPOSICIÓN */}
          <div 
            style={{ 
              width: '100%',
              overflow: 'visible',
              position: 'static',  // IMPORTANTE: static
              zIndex: 'auto'       // IMPORTANTE: sin z-index forzado
            }}
          >
            <DataTable
              data={currentData?.data || []}
              columns={currentFile?.columns || []}
              loading={loading}
              pagination={{
                current: currentData?.page || 1,
                pageSize: currentData?.page_size || 20,
                total: currentData?.total || 0,
                showSizeChanger: true,
                showQuickJumper: true,
              }}
              onPaginationChange={handlePaginationChange}
              onFiltersChange={(filters) => setUIState(prev => ({ ...prev, filters }))}
              onSortChange={(sorting) => setUIState(prev => ({ ...prev, sorting }))}
              onDeleteRows={handleDeleteRows}
              onSearch={(searchTerm) => setUIState(prev => ({ ...prev, searchTerm }))}
            />
          </div>
        </Card>
      )}
    </div>
  );

  const renderTransformTab = () => (
    <div style={layoutStyles.contentContainer}>
      <Card title="Transformaciones de Datos" style={{
        ...layoutStyles.contentCard,
        background: '#fff',
        position: 'static'
      }}>
        <Row gutter={[16, 16]}>
          <Col xs={24} md={12}>
            <Card size="small" title="Operaciones Básicas">
              <Space direction="vertical" style={{ width: '100%' }}>
                <Button
                  block
                  onClick={() => setUIState(prev => ({ 
                    ...prev, 
                    selectedTransform: 'concatenate',
                    transformModalVisible: true 
                  }))}
                >
                  Concatenar Columnas
                </Button>
                <Button
                  block
                  onClick={() => setUIState(prev => ({ 
                    ...prev, 
                    selectedTransform: 'split_column',
                    transformModalVisible: true 
                  }))}
                >
                  Dividir Columna
                </Button>
                <Button
                  block
                  onClick={() => setUIState(prev => ({ 
                    ...prev, 
                    selectedTransform: 'replace_values',
                    transformModalVisible: true 
                  }))}
                >
                  Reemplazar Valores
                </Button>
              </Space>
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card size="small" title="Operaciones Avanzadas">
              <Space direction="vertical" style={{ width: '100%' }}>
                <Button
                  block
                  onClick={() => setUIState(prev => ({ 
                    ...prev, 
                    selectedTransform: 'to_uppercase',
                    transformModalVisible: true 
                  }))}
                >
                  Convertir a Mayúsculas
                </Button>
                <Button
                  block
                  onClick={() => setUIState(prev => ({ 
                    ...prev, 
                    selectedTransform: 'fill_null',
                    transformModalVisible: true 
                  }))}
                >
                  Llenar Valores Nulos
                </Button>
                <Button
                  block
                  onClick={() => setUIState(prev => ({ 
                    ...prev, 
                    selectedTransform: 'delete_column',
                    transformModalVisible: true 
                  }))}
                >
                  Eliminar Columna
                </Button>
              </Space>
            </Card>
          </Col>
        </Row>
      </Card>
    </div>
  );

  const renderExportTab = () => (
    <div style={layoutStyles.contentContainer}>
      <Card title="Exportar Datos" style={{
        ...layoutStyles.contentCard,
        background: '#fff',
        position: 'static'
      }}>
        <Row gutter={[16, 16]}>
          <Col xs={24} md={8}>
            <Card 
              size="small" 
              hoverable 
              onClick={() => handleExport('csv')}
              style={{ position: 'static' }}
            >
              <div style={{ textAlign: 'center', padding: '20px' }}>
                <FileOutlined style={{ fontSize: '48px', color: '#1890ff' }} />
                <Title level={4}>Exportar CSV</Title>
                <Text type="secondary">Formato separado por comas</Text>
              </div>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card 
              size="small" 
              hoverable 
              onClick={() => handleExport('excel')}
              style={{ position: 'static' }}
            >
              <div style={{ textAlign: 'center', padding: '20px' }}>
                <FileOutlined style={{ fontSize: '48px', color: '#52c41a' }} />
                <Title level={4}>Exportar Excel</Title>
                <Text type="secondary">Formato Microsoft Excel</Text>
              </div>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card 
              size="small" 
              hoverable 
              onClick={() => handleExport('json')}
              style={{ position: 'static' }}
            >
              <div style={{ textAlign: 'center', padding: '20px' }}>
                <FileOutlined style={{ fontSize: '48px', color: '#faad14' }} />
                <Title level={4}>Exportar JSON</Title>
                <Text type="secondary">Formato JavaScript Object</Text>
              </div>
            </Card>
          </Col>
        </Row>
      </Card>
    </div>
  );

  const renderChatTab = () => (
    <div style={layoutStyles.contentContainer}>
      <ChatBot fileContext={currentFile?.file_id} />
    </div>
  );

  const renderContent = () => {
    switch (uiState.activeTab) {
      case 'upload':
        return renderUploadTab();
      case 'transform':
        return renderTransformTab();
      case 'export':
        return renderExportTab();
      case 'chat':
        return renderChatTab();
      default:
        return renderUploadTab();
    }
  };

  // ===== MAIN RENDER =====
  return (
    <ErrorBoundary>
      {/* ===== CONTENEDOR PRINCIPAL CORREGIDO ===== */}
      <div style={{ 
        width: '100%', 
        minHeight: '100vh', 
        overflow: 'visible',
        position: 'static',  // IMPORTANTE: static en lugar de relative
        backgroundColor: '#f0f2f5'
      }}>
        <Layout style={layoutStyles.main}>
          {/* Header */}
          <Header style={layoutStyles.header}>
            <div style={{ display: 'flex', alignItems: 'center' }}>
              <Button
                type="text"
                icon={uiState.collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
                onClick={handleToggleSidebar}
                style={{ color: 'white', fontSize: '16px' }}
              />
              <HomeOutlined style={{ color: 'white', fontSize: '20px', margin: '0 16px' }} />
              <Title level={3} style={{ color: 'white', margin: 0 }}>
                Procesador de Archivos Excel/CSV
              </Title>
            </div>

            <Space>
              {currentFile && (
                <Badge count={currentFile.total_rows} style={{ backgroundColor: '#52c41a' }}>
                  <Button type="primary" ghost>
                    {currentFile.original_name}
                  </Button>
                </Badge>
              )}
            </Space>
          </Header>

          {/* Layout interno */}
          <Layout style={{ 
            width: '100%', 
            display: 'flex',
            flexDirection: 'row',
            minHeight: 'calc(100vh - 64px)',
            position: 'static'  // IMPORTANTE: static
          }}>
            {/* Sidebar */}
            <Sider
              width={200}
              style={layoutStyles.sider}
              collapsed={uiState.collapsed}
              collapsedWidth={80}
            >
              <Menu
                mode="inline"
                selectedKeys={[uiState.activeTab]}
                onSelect={({ key }) => handleTabChange(key)}
                style={{ 
                  height: '100%', 
                  borderRight: 0,
                  overflow: 'auto'
                }}
                items={menuItems}
              />
            </Sider>

            {/* Content */}
            <Layout style={{ 
              ...layoutStyles.content,
              flex: 1,
              display: 'flex',
              flexDirection: 'column',
              position: 'static'  // IMPORTANTE: static
            }}>
              <Content style={{
                ...layoutStyles.content,
                flex: 1,
                display: 'flex',
                flexDirection: 'column',
                position: 'static'  // IMPORTANTE: static
              }}>
                {/* Alertas de error */}
                {error && (
                  <Alert
                    message="Error"
                    description={error}
                    type="error"
                    closable
                    style={{ 
                      margin: '16px 16px 0 16px',
                      flexShrink: 0,
                      position: 'static'  // IMPORTANTE: static
                    }}
                    onClose={() => setError(null)}
                  />
                )}

                {/* Contenido principal */}
                <div style={{
                  flex: 1,
                  overflow: 'visible',
                  width: '100%',
                  position: 'static'  // IMPORTANTE: static
                }}>
                  {loading && !currentData ? (
                    <div style={{
                      display: 'flex',
                      justifyContent: 'center',
                      alignItems: 'center',
                      height: '50vh',
                      width: '100%',
                      position: 'static'  // IMPORTANTE: static
                    }}>
                      <Spin size="large">
                        <div style={{ padding: '50px' }}>
                          <span>Procesando...</span>
                        </div>
                      </Spin>
                    </div>
                  ) : (
                    renderContent()
                  )}
                </div>
              </Content>

              {/* Footer */}
              <Footer style={{
                ...layoutStyles.footer,
                flexShrink: 0,
                marginTop: 'auto',
                position: 'static'  // IMPORTANTE: static
              }}>
                Procesador de Archivos ©2025 - Desarrollado con React + FastAPI
              </Footer>
            </Layout>
          </Layout>

          {/* Drawer para Chat */}
          <Drawer
            title="Asistente IA"
            placement="right"
            width={400}
            onClose={() => setUIState(prev => ({ ...prev, chatDrawerVisible: false }))}
            open={uiState.chatDrawerVisible}
            style={{ zIndex: 2000 }}
          >
            <ChatBot fileContext={currentFile?.file_id} />
          </Drawer>

          {/* Modal para Transformaciones */}
          <Modal
            title="Aplicar Transformación"
            open={uiState.transformModalVisible}
            onOk={handleTransform}
            onCancel={() => setUIState(prev => ({ ...prev, transformModalVisible: false }))}
            okText="Aplicar"
            cancelText="Cancelar"
            style={{ zIndex: 2000 }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <Text>Operación seleccionada: <Text strong>{uiState.selectedTransform}</Text></Text>
              <Select
                style={{ width: '100%' }}
                placeholder="Seleccionar columna"
                disabled={!currentFile}
              >
                {(currentFile?.columns || []).map(col => (
                  <Option key={col} value={col}>{col}</Option>
                ))}
              </Select>
            </Space>
          </Modal>
        </Layout>
      </div>
    </ErrorBoundary>
  );
};

export default App;
