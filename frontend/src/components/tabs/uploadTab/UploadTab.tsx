// src/components/tabs/tabs/UploadTab/UploadTab.tsx
import React, { useEffect, useState } from 'react';
import { Card, Typography, Row, Col, Button } from 'antd';
import { EyeOutlined, DeleteOutlined } from '@ant-design/icons';
import type { TabProps, FilterCondition, SortCondition } from '../../../types/api.types';
import { FileUploader } from '../../fileUploader/FileUploader';
import { DataTable } from '../../dataTable/DataTable';
import { useFileOperations } from '../../../hooks/useFileOperations';
import { WelcomeComponent } from '../../common/WelcomeComponent';

const { Text } = Typography;

export const UploadTab: React.FC<TabProps> = ({ isMobile, isTablet }) => {
  const {
    files,
    currentFile,
    currentData,
    loading,
    setCurrentFile,
    deleteFile,
    handleUploadSuccess,
    handleDeleteRows,
    loadFileData,
  } = useFileOperations();

  const [ui, setUI] = useState({
    currentPage: 1,
    pageSize: 20,
    filters: [] as FilterCondition[],
    sorting: [] as SortCondition[],
    searchTerm: '',
  });

  // Validación mejorada
  const showTable = !!(
    currentFile &&
    currentData &&
    Array.isArray(currentData.data) &&
    currentData.data.length > 0
  );

  // Cargar datos cuando cambie el archivo o la paginación
  useEffect(() => {
    if (!currentFile) return;
    
    const loadData = async () => {
      try {
        await loadFileData({
          file_id: currentFile.file_id,
          page: ui.currentPage,
          page_size: ui.pageSize,
          filters: ui.filters.length ? ui.filters : undefined,
          sort: ui.sorting.length ? ui.sorting : undefined,
          search: ui.searchTerm || undefined,
        });
      } catch (error) {
        console.error('Error cargando datos:', error);
      }
    };
    
    loadData();
  }, [currentFile, ui.currentPage, ui.pageSize, ui.filters, ui.sorting, ui.searchTerm, loadFileData]);

  const handlePaginationChange = (page: number, size: number) => {
    setUI(prev => ({ ...prev, currentPage: page, pageSize: size }));
  };

  return (
    <div className="content-container">
      {!showTable && <WelcomeComponent isMobile={isMobile} />}
      
      {/* Uploader */}
      <Card title="Cargar Archivo Excel/CSV" className="upload-card">
        <div className="uploader-content">
          <FileUploader loading={loading} onUploadSuccess={handleUploadSuccess} />
          <Text type="secondary" className="upload-hint">
            {isMobile ? 'CSV, XLSX, XLS' : 'Formatos soportados: .csv, .xlsx, .xls'}
          </Text>
        </div>
      </Card>

      {/* Archivos disponibles */}
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

      {/* Tabla de datos */}
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
            onDeleteRows={handleDeleteRows}
            onFiltersChange={(filters: any) => setUI(prev => ({ ...prev, filters }))}
            onSortChange={(sorting: any) => setUI(prev => ({ ...prev, sorting }))}
            onSearch={(searchTerm: any) => setUI(prev => ({ ...prev, searchTerm }))}
          />
        </Card>
      )}
    </div>
  );
};
