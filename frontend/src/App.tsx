// src/App.tsx
import React, { useEffect, useState, useMemo, useCallback } from 'react';
import {
  Layout,
  Card,
  Row,
  Col,
  Spin,
  Alert,
  Typography,
  Button,
  Space,
  message,
  Modal,
  Select,
  Drawer,
  Tag
} from 'antd';
import {
  FileOutlined,
  DeleteOutlined,
  EyeOutlined,
  DownloadOutlined,
  SwapOutlined
} from '@ant-design/icons';
import { Grid } from 'antd';

/* ─── Componentes de UI ──────────────────────────────────────────────── */
import { AppHeader } from './components/layout/AppHeader';
import { NavigationMenu } from './components/navigation/NavigationMenu';
import { FileUploader } from './components/fileUploader/FileUploader';
import { DataTable } from './components/dataTable/DataTable';
import { ChatBot } from './components/chatbot/ChatBot';
import { TransformPanel } from './components/transform/TransformPanel';
import FileCrossManager from './components/cross/FileCrossManager'; // ✅ Componente de cruce

/* ─── Hooks / servicios ──────────────────────────────────────────────── */
import { useFileManager } from './hooks/useFileManager';
import type {
  DataRequest,
  FilterCondition,
  SortCondition,
  FileInfo,
} from './types/api.types';
import { TransformService } from './services/TransformService';
import { ExportService } from './services/ExportService';
import { DeleteService } from './services/DeleteService';
import { FileService } from './services/FileService';

/* ─── Estilos globales ──────────────────────────────────────────────── */
import 'antd/dist/reset.css';
import './App.css';

const { Header, Content, Sider, Footer } = Layout;
const { Title, Text } = Typography;
const { Option } = Select;
const { useBreakpoint } = Grid;

/* ─── Tipos de UI ───────────────────────────────────────────────────── */
type TabKey = 'upload' | 'transform' | 'chat' | 'export' | 'cross'; // ✅ Agregado 'cross'

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
  mobileMenuVisible: boolean;
  crossModalVisible: boolean; // ✅ Estado para modal de cruce
}

/* ─── Utilidades ────────────────────────────────────────────────────── */
const isValidCurrentData = (d: any): d is { data: any[] } =>
  d && typeof d === 'object' && Array.isArray(d.data);

/* ─── Error Boundary ────────────────────────────────────────────────── */
class ErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { hasError: boolean; error: Error | null }
> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { hasError: false, error: null };
  }
  static getDerivedStateFromError(err: Error) {
    return { hasError: true, error: err };
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

/* ─── Componente principal ─────────────────────────────────────────── */
const App: React.FC = () => {
  /* breakpoints */
  const screens = useBreakpoint();
  const isMobile = !screens.md;
  const isTablet = screens.md && !screens.lg;

  /* File-manager state */
  const {
    files,
    currentFile,
    currentData,
    loading,
    error,
    loadFileData,
    deleteFile,
    setCurrentFile,
    setError,
    loadFiles,
  } = useFileManager();

  /* UI state */
  const [ui, setUI] = useState<UIState>({
    activeTab: 'upload',
    collapsed: isMobile,
    currentPage: 1,
    pageSize: 20,
    filters: [],
    sorting: [],
    searchTerm: '',
    chatDrawerVisible: false,
    transformModalVisible: false,
    selectedTransform: '',
    mobileMenuVisible: false,
    crossModalVisible: false, // ✅ Inicializado
  });

  const [lastProcessedFile, setLastProcessedFile] = useState<FileData | null>(null);

  // ✅ ESTADO PARA RESULTADO DEL CRUCE
  const [crossResult, setCrossResult] = useState<any>(null);

  // Función para refrescar archivos desde TransformPanel
  const handleRefreshFromTransform = useCallback(async () => {
    console.log('🔄 Refrescando archivos desde TransformPanel...');
    try {
      await loadFiles();
      console.log('✅ Archivos refrescados exitosamente');
    } catch (error) {
      console.error('❌ Error refrescando archivos:', error);
      message.error('Error al refrescar la lista de archivos');
    }
  }, [loadFiles]);

  // Callback cuando se sube un archivo desde TransformPanel
  const handleFileUploadedFromTransform = useCallback((fileInfo: FileInfo) => {
    console.log('📤 Archivo subido desde TransformPanel:', fileInfo);

    // Convertir FileInfo a FileData
    const newFileData: FileData = {
      file_id: fileInfo.file_id,
      original_name: fileInfo.original_name,
      columns: fileInfo.columns,
      sheets: fileInfo.sheets,
      total_rows: fileInfo.total_rows,
    };

    // Si no hay archivo actual, establecer este como actual
    if (!currentFile) {
      setCurrentFile(newFileData);
      setLastProcessedFile(newFileData);
      message.success(`Archivo "${fileInfo.original_name}" seleccionado automáticamente`);
    }

    // Notificar éxito
    message.success(`Archivo "${fileInfo.original_name}" disponible para cruce`);
  }, [currentFile, setCurrentFile]);

  // ✅ FUNCIÓN PARA MANEJAR RESULTADO DEL CRUCE
  const handleCrossComplete = useCallback((result: any) => {
    console.log('📊 Cruce completado:', result);
    setCrossResult(result);
    setUI(prev => ({ ...prev, crossModalVisible: false, activeTab: 'cross' })); // Cambiar a tab de cruce
    message.success(`Cruce completado: ${result.total_rows?.toLocaleString()} registros procesados`);
  }, []);

  // ✅ FUNCIÓN PARA EXPORTAR RESULTADO
  // En App.tsx - función handleExportCrossResult
  const handleExportCrossResult = useCallback(async (format: 'csv' | 'xlsx' = 'csv') => {
    if (!crossResult) {
      message.warning('No hay resultado de cruce para exportar');
      return;
    }

    try {
      const exportData = crossResult.data || [];

      if (format === 'csv') {
        // ✅ EXPORTAR CON PUNTO Y COMA Y CODIFICACIÓN CORRECTA
        const headers = crossResult.columns.join(';'); // ✅ Punto y coma en headers
        const rows = exportData.map((row: any) =>
          crossResult.columns.map((col: string) => {
            const value = row[col];
            // Manejar valores que contienen punto y coma o comillas
            if (typeof value === 'string' && (value.includes(';') || value.includes('"'))) {
              return `"${value.replace(/"/g, '""')}"`;
            }
            return value || '';
          }).join(';') // ✅ Punto y coma como separador
        );

        const csvContent = [headers, ...rows].join('\n');

        // ✅ CREAR BLOB CON UTF-8-BOM PARA CARACTERES ESPECIALES
        const BOM = '\uFEFF'; // Byte Order Mark para UTF-8
        const blob = new Blob([BOM + csvContent], {
          type: 'text/csv;charset=utf-8-sig;' 
        });

        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `cruce_resultado_${new Date().toISOString().slice(0, 10)}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);

        message.success('Archivo CSV exportado exitosamente con punto y coma');
      }
    } catch (error) {
      console.error('Error exportando archivo:', error);
      message.error('Error al exportar archivo');
    }
  }, [crossResult]);


  // ✅ FUNCIÓN PARA LIMPIAR RESULTADO DEL CRUCE
  const handleClearCrossResult = useCallback(() => {
    setCrossResult(null);
    message.info('Resultado del cruce eliminado');
  }, []);

  /* --- Data loading -------------------------------------------------- */
  useEffect(() => {
    if (!currentFile) return;
    const req: DataRequest = {
      file_id: currentFile.file_id,
      page: ui.currentPage,
      page_size: ui.pageSize,
      filters: ui.filters.length ? ui.filters : undefined,
      sort: ui.sorting.length ? ui.sorting : undefined,
      search: ui.searchTerm || undefined,
    };
    loadFileData(req);
    setLastProcessedFile(currentFile);
  }, [
    currentFile,
    ui.currentPage,
    ui.pageSize,
    ui.filters,
    ui.sorting,
    ui.searchTerm,
    loadFileData,
  ]);

  /* --- Helpers ------------------------------------------------------- */
  const showTable = !!(
    currentFile &&
    isValidCurrentData(currentData) &&
    currentData.data.length
  );

  /* --- Handlers ------------------------------------------------------ */
  const handleTabChange = (key: string) =>
    setUI((p) => ({
      ...p,
      activeTab: key as TabKey,
      mobileMenuVisible: false,
    }));

  const handleToggleSidebar = () =>
    setUI((p) =>
      isMobile ? { ...p, mobileMenuVisible: !p.mobileMenuVisible } : { ...p, collapsed: !p.collapsed },
    );

  const handleUploadSuccess = (res: any) => {
    const newFile: FileData = {
      file_id: res.file_id,
      original_name: res.original_name ?? res.filename ?? 'Archivo',
      columns: res.columns,
      sheets: res.sheets,
      total_rows: res.total_rows,
    };
    setCurrentFile(newFile);
    setLastProcessedFile(newFile);
    message.success('Archivo cargado');
  };

  const handlePaginationChange = (page: number, size: number) =>
    setUI((p) => ({ ...p, currentPage: page, pageSize: size }));

  const handleDeleteRows = async (idx: number[]) => {
    if (!currentFile) return;
    await DeleteService.deleteRows({
      file_id: currentFile.file_id,
      row_indices: idx,
    });
    message.success(`${idx.length} filas eliminadas`);
    loadFileData({
      file_id: currentFile.file_id,
      page: ui.currentPage,
      page_size: ui.pageSize,
    });
  };

  const handleTransform = async () => {
    if (!currentFile || !ui.selectedTransform) return;
    await TransformService.transformData({
      file_id: currentFile.file_id,
      operation: ui.selectedTransform as any,
      params: {},
    });
    message.success('Transformación aplicada');
    setUI((p) => ({ ...p, transformModalVisible: false }));
    loadFileData({
      file_id: currentFile.file_id,
      page: ui.currentPage,
      page_size: ui.pageSize,
    });
  };

  const handleExport = async (format: 'csv' | 'excel' | 'json') => {
    if (!currentFile) {
      message.warning('Selecciona un archivo para exportar');
      return;
    }

    try {
      const res = await ExportService.exportData({
        file_id: currentFile.file_id,
        format,
        include_headers: true,
      });
      message.success(`Descargado: ${res.filename}`);
    } catch (error) {
      message.error('Error al exportar archivo');
    }
  };

  /* --- Render helpers ------------------------------------------------ */
  const renderWelcome = () => (
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

  const renderUploadTab = () => (
    <div className="content-container">
      {!showTable && renderWelcome()}
      {/* uploader */}
      <Card title="Cargar Archivo Excel/CSV" className="upload-card">
        <div className="uploader-content">
          <FileUploader loading={loading} onUploadSuccess={handleUploadSuccess} />
          <Text type="secondary" className="upload-hint">
            {isMobile ? 'CSV, XLSX, XLS' : 'Formatos soportados: .csv, .xlsx, .xls'}
          </Text>
        </div>
      </Card>

      {/* archivos */}
      {files?.length ? (
        <Card title="Archivos Disponibles" className="files-card">
          <Row gutter={[16, 16]}>
            {files.map((f) => (
              <Col xs={24} sm={12} md={isTablet ? 12 : 8} lg={8} xl={6} key={f.file_id}>
                <Card
                  size="small"
                  hoverable
                  className={`file-item ${currentFile?.file_id === f.file_id ? 'selected' : ''}`}
                  actions={[
                    <Button
                      key="sel"
                      type={currentFile?.file_id === f.file_id ? 'primary' : 'link'}
                      icon={<EyeOutlined />}
                      size={isMobile ? 'small' : 'middle'}
                      onClick={() => setCurrentFile(f)}
                    >
                      {currentFile?.file_id === f.file_id ? 'Actual' : isMobile ? 'Ver' : 'Seleccionar'}
                    </Button>,
                    <Button
                      key="del"
                      type="link"
                      danger
                      icon={<DeleteOutlined />}
                      size={isMobile ? 'small' : 'middle'}
                      onClick={() => deleteFile(f.file_id)}
                    >
                      {isMobile ? 'Del' : 'Eliminar'}
                    </Button>,
                  ]}
                >
                  <Card.Meta
                    title={
                      isMobile && f.original_name.length > 18
                        ? `${f.original_name.slice(0, 18)}…`
                        : f.original_name
                    }
                    description={
                      <>
                        <Text>{f.total_rows?.toLocaleString()} filas</Text>
                        <br />
                        <Text>{f.columns?.length} columnas</Text>
                      </>
                    }
                  />
                </Card>
              </Col>
            ))}
          </Row>
        </Card>
      ) : null}

      {/* tabla */}
      {showTable && (
        <Card
          className="data-table-card"
          title={
            <div className="table-header">
              <div className="table-title">
                <span>
                  {isMobile ? '📋' : '📋 Datos: '}
                  {isMobile && currentFile
                    ? currentFile.original_name.slice(0, 15)
                    : currentFile?.original_name}
                </span>
              </div>
            </div>
          }
        >
          <DataTable
            data={currentData?.data ?? []}
            columns={currentFile?.columns ?? []}
            loading={loading}
            pagination={{
              current: currentData?.page ?? 1,
              pageSize: currentData?.page_size ?? 20,
              total: currentData?.total ?? 0,
              showSizeChanger: !isMobile,
              showQuickJumper: !isMobile,
              size: isMobile ? 'small' : 'default',
            }}
            onPaginationChange={handlePaginationChange}
            onFiltersChange={(filters) => setUI((p) => ({ ...p, filters }))}
            onSortChange={(sorting) => setUI((p) => ({ ...p, sorting }))}
            onDeleteRows={handleDeleteRows}
            onSearch={(t) => setUI((p) => ({ ...p, searchTerm: t }))}
          />
        </Card>
      )}
    </div>
  );

  const renderTransformTab = () => (
    <div className="content-container">
      <TransformPanel
        isMobile={isMobile}
        onSelectOp={(op) =>
          setUI((p) => ({ ...p, selectedTransform: op, transformModalVisible: true }))
        }
        availableFiles={files || []}
        onRefreshFiles={handleRefreshFromTransform}
        onFileUploaded={handleFileUploadedFromTransform}
      />

      {/* ✅ BOTÓN PARA ABRIR MODAL DE CRUCE */}
      <Card title="🔄 Cruzar Archivos" style={{ marginTop: 16 }}>
        <Alert
          message="Cruce de Archivos - VLOOKUP"
          description="Combina datos de dos archivos basándose en columnas clave comunes. Perfecto para enriquecer tus datos principales con información adicional."
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />

        <Space>
          <Button
            type="primary"
            icon={<SwapOutlined />}
            size="large"
            onClick={() => setUI(p => ({ ...p, crossModalVisible: true }))}
            disabled={!files || files.length < 2}
          >
            {isMobile ? 'Cruzar' : 'Iniciar Cruce de Archivos'}
          </Button>

          {(!files || files.length < 2) && (
            <Text type="secondary">
              (Necesitas al menos 2 archivos cargados)
            </Text>
          )}
        </Space>
      </Card>
    </div>
  );

  const renderExportTab = () => (
    <div className="content-container">
      <Card title="Exportar Datos" className="export-card">
        {!currentFile && (
          <Alert
            message="Sin archivo seleccionado"
            description="Selecciona un archivo en la sección 'Cargar' para poder exportar datos."
            type="info"
            showIcon
            style={{ marginBottom: 16 }}
            action={
              <Button
                size="small"
                type="primary"
                onClick={() => setUI(p => ({ ...p, activeTab: 'upload' }))}
              >
                Ir a Cargar
              </Button>
            }
          />
        )}

        <Row gutter={[16, 16]}>
          {[
            { key: 'csv', label: 'CSV', color: 'csv-icon' },
            { key: 'excel', label: 'Excel', color: 'excel-icon' },
            { key: 'json', label: 'JSON', color: 'json-icon' },
          ].map((o) => (
            <Col xs={24} sm={8} key={o.key}>
              <Card
                size="small"
                hoverable={!!currentFile}
                onClick={() => handleExport(o.key as any)}
                className={`export-option ${o.key}-option ${!currentFile ? 'disabled' : ''}`}
                style={{
                  opacity: !currentFile ? 0.6 : 1,
                  cursor: !currentFile ? 'not-allowed' : 'pointer'
                }}
              >
                <div className="export-content">
                  <FileOutlined className={`export-icon ${o.color}`} />
                  <Title level={isMobile ? 5 : 4}>{o.label}</Title>
                  <Text type="secondary">
                    {o.key === 'csv'
                      ? isMobile
                        ? 'Separado por comas'
                        : 'Formato separado por comas'
                      : o.key === 'excel'
                        ? isMobile
                          ? 'Microsoft Excel'
                          : 'Formato Microsoft Excel'
                        : isMobile
                          ? 'JavaScript Object'
                          : 'Formato JavaScript Object'}
                  </Text>
                </div>
              </Card>
            </Col>
          ))}
        </Row>

        {currentFile && (
          <Card size="small" style={{ marginTop: 16 }} title="Archivo actual">
            <Space direction="vertical" style={{ width: '100%' }}>
              <Text><strong>Nombre:</strong> {currentFile.original_name}</Text>
              <Text><strong>Filas:</strong> {currentFile.total_rows?.toLocaleString()}</Text>
              <Text><strong>Columnas:</strong> {currentFile.columns?.length}</Text>
              {currentFile.sheets && currentFile.sheets.length > 0 && (
                <Text><strong>Hojas:</strong> {currentFile.sheets.join(', ')}</Text>
              )}
            </Space>
          </Card>
        )}
      </Card>
    </div>
  );

  // ✅ TAB PARA MOSTRAR RESULTADO DEL CRUCE USANDO DATATABLE
  const renderCrossTab = () => (
    <div className="content-container">
      {!crossResult ? (
        <Card title="📊 Resultado del Cruce">
          <Alert
            message="Sin resultado de cruce"
            description="Realiza un cruce de archivos desde la sección 'Transformar' para ver los resultados aquí."
            type="info"
            showIcon
            action={
              <Button
                size="small"
                type="primary"
                onClick={() => setUI(p => ({ ...p, activeTab: 'transform' }))}
              >
                Ir a Transformar
              </Button>
            }
          />
        </Card>
      ) : (
        <>
          {/* ✅ INFORMACIÓN DEL CRUCE */}
          <Alert
            message="🎉 Cruce completado exitosamente"
            description={`${crossResult.total_rows?.toLocaleString()} registros con ${crossResult.columns?.length} columnas`}
            type="success"
            style={{ marginBottom: 16 }}
            action={
              <Space>
                <Button
                  type="primary"
                  icon={<DownloadOutlined />}
                  onClick={() => handleExportCrossResult('csv')}
                >
                  {isMobile ? 'CSV' : 'Exportar CSV'}
                </Button>
                <Button
                  onClick={handleClearCrossResult}
                  danger
                >
                  {isMobile ? 'Limpiar' : 'Limpiar Resultado'}
                </Button>
              </Space>
            }
          />

          {/* ✅ USAR DATATABLE EN LUGAR DE TABLE NATIVA */}
          <Card
            title="📊 Resultado del Cruce de Archivos"
            size="small"
          >
            <DataTable
              data={crossResult.data || []}
              columns={crossResult.columns || []}
              loading={loading}
              pagination={{
                current: 1,
                pageSize: 50,
                total: crossResult.total_rows || 0,
                showSizeChanger: true,
                showQuickJumper: !isMobile,
                size: isMobile ? 'small' : 'default'
              }}
              // ✅ CALLBACKS PARA FUNCIONALIDAD COMPLETA DEL DATATABLE
              onPaginationChange={(page: number, size: number) => {
                console.log(`Navegación: página ${page}, tamaño ${size}`);
                // La paginación se maneja localmente en DataTable
              }}
              onFiltersChange={(filters) => {
                console.log('Filtros aplicados en resultado de cruce:', filters);
                // Los filtros se aplican localmente en DataTable
              }}
              onSortChange={(sort) => {
                console.log('Ordenamiento aplicado en resultado de cruce:', sort);
                // El ordenamiento se aplica localmente en DataTable
              }}
              onSearch={(searchTerm) => {
                console.log('Búsqueda en resultado de cruce:', searchTerm);
                // La búsqueda se aplica localmente en DataTable
              }} onDeleteRows={function (indices: number[]): void {
                throw new Error('Function not implemented.');
              }}              // ✅ NO INCLUIR onDeleteRows PARA PROTEGER EL RESULTADO
            />

            {/* ✅ INFORMACIÓN ADICIONAL SI HAY MUCHOS DATOS */}
            {(crossResult.data?.length || 0) > 100 && (
              <Alert
                message="Optimización de rendimiento"
                description="Para mejor performance, se muestran los primeros registros. Usa filtros, búsqueda o exporta para trabajar con todos los datos."
                type="info"
                style={{ marginTop: 16 }}
              />
            )}
          </Card>
        </>
      )}
    </div>
  );

  const renderChatTab = () => (
    <div className="content-container">
      {!currentFile && (
        <Alert
          message="Recomendación"
          description="Para obtener respuestas más precisas, selecciona un archivo en la sección 'Cargar' para dar contexto al chat."
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          action={
            <Button
              size="small"
              type="primary"
              onClick={() => setUI(p => ({ ...p, activeTab: 'upload' }))}
            >
              Seleccionar Archivo
            </Button>
          }
        />
      )}
      <ChatBot fileContext={currentFile?.file_id} />
    </div>
  );

  const renderContent = () => {
    switch (ui.activeTab) {
      case 'upload':
        return renderUploadTab();
      case 'transform':
        return renderTransformTab();
      case 'export':
        return renderExportTab();
      case 'chat':
        return renderChatTab();
      case 'cross': // ✅ Caso para tab de cruce
        return renderCrossTab();
      default:
        return null;
    }
  };

  /* --- Debug Info (opcional, remover en producción) ----------------- */
  console.log('🎭 App Render:', {
    filesCount: files?.length || 0,
    currentFile: currentFile?.original_name || 'none',
    activeTab: ui.activeTab,
    loading,
    hasCrossResult: !!crossResult
  });

  /* --- Render principal --------------------------------------------- */
  return (
    <ErrorBoundary>
      <div className={`app-container ${isMobile ? 'mobile' : isTablet ? 'tablet' : 'desktop'}`}>
        <Layout className="main-layout">
          {/* Header desacoplado */}
          <Header className="app-header">
            <AppHeader
              isMobile={isMobile}
              isTablet={isTablet}
              collapsed={ui.collapsed}
              currentFile={currentFile}
              onToggleSidebar={handleToggleSidebar}
            />
          </Header>

          <Layout className="inner-layout">
            {/* Sidebar desktop */}
            {!isMobile && (
              <Sider
                width={200}
                className="app-sider"
                collapsed={ui.collapsed}
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

            {/* Contenido */}
            <Layout className="content-layout">
              <Content className="app-content">
                {error && (
                  <Alert
                    type="error"
                    message="Error"
                    description={error}
                    closable
                    className="error-alert"
                    onClose={() => setError(null)}
                  />
                )}

                <div className="main-content">
                  {loading && !currentData && ui.activeTab === 'upload' ? (
                    <div className="loading-container">
                      <Spin size={isMobile ? 'default' : 'large'} />
                    </div>
                  ) : (
                    renderContent()
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

          {/* Modal Transformación */}
          <Modal
            title="Aplicar Transformación"
            open={ui.transformModalVisible}
            onOk={handleTransform}
            onCancel={() => setUI((p) => ({ ...p, transformModalVisible: false }))}
            okText="Aplicar"
            cancelText="Cancelar"
            className="transform-modal"
            width={isMobile ? '90%' : 520}
          >
            <Space direction="vertical" className="transform-modal-content">
              <Text>
                Operación: <Text strong>{ui.selectedTransform}</Text>
              </Text>
              <Select
                className="column-select"
                placeholder="Seleccionar columna"
                disabled={!currentFile}
                size={isMobile ? 'large' : 'middle'}
              >
                {(currentFile?.columns ?? []).map((c) => (
                  <Option value={c} key={c}>
                    {c}
                  </Option>
                ))}
              </Select>
            </Space>
          </Modal>

          {/* ✅ MODAL PARA CRUCE DE ARCHIVOS */}
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
              availableFiles={files || []}
              onRefreshFiles={handleRefreshFromTransform}
              onCrossComplete={handleCrossComplete}
            />
          </Modal>
        </Layout>
      </div>
    </ErrorBoundary>
  );
};

export default App;
