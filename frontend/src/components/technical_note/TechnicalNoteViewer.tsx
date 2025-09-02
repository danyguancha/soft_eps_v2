// components/technical-note/TechnicalNoteViewer.tsx
import React from 'react';
import { Card, Typography, Row, Col, Spin, Alert, Space, Tooltip, Progress } from 'antd';
import { 
  UserOutlined, 
  TeamOutlined, 
  UsergroupAddOutlined,
  UserAddOutlined,
  UserSwitchOutlined,
  UserDeleteOutlined,
  FileTextOutlined,
  ClockCircleOutlined
} from '@ant-design/icons';
import { useTechnicalNote } from '../../hooks/useTechnicalNote';
import { DataTable } from '../dataTable/DataTable';

const { Title, Text } = Typography;

interface AgeGroupIcon {
  key: string;
  displayName: string;
  icon: React.ReactNode;
  color: string;
  description: string;
  filename?: string;
}

const TechnicalNoteViewer: React.FC = () => {
  const {
    // Estados básicos
    availableFiles,
    currentFileData,
    currentFileMetadata,
    loading,
    loadingFiles,
    loadingMetadata,
    selectedFile,
    
    // ✅ Estados para DataTable
    filteredData,
    pagination,
    
    // ✅ Estados de paginación del servidor
    currentPage,
    totalPages,
    
    // Acciones
    loadFileData,
    getFileByDisplayName,
    
    // ✅ Handlers para DataTable
    handlePaginationChange,
    handleFiltersChange,
    handleSortChange,
    handleDeleteRows,
    handleSearch,
    
    // Helpers
    hasData,
    columns,
    currentPageInfo
  } = useTechnicalNote();

  // Configuración de grupos etarios con iconos
  const ageGroups: AgeGroupIcon[] = [
    {
      key: 'primera-infancia',
      displayName: 'Primera Infancia',
      icon: <UserOutlined style={{ fontSize: '24px' }} />,
      color: '#ff7a45',
      description: 'Datos de primera infancia (0-5 años)',
      filename: 'PrimeraInfanciaNueva.csv'
    },
    {
      key: 'infancia',
      displayName: 'Infancia',
      icon: <TeamOutlined style={{ fontSize: '24px' }} />,
      color: '#40a9ff',
      description: 'Datos de población infantil (6-11 años)',
      filename: 'InfanciaNueva.csv'
    },
    {
      key: 'adolescencia',
      displayName: 'Adolescencia',
      icon: <UsergroupAddOutlined style={{ fontSize: '24px' }} />,
      color: '#73d13d',
      description: 'Datos de población adolescente (12-17 años)',
      filename: 'AdolescenciaNueva.csv'
    },
    {
      key: 'juventud',
      displayName: 'Juventud',
      icon: <UserAddOutlined style={{ fontSize: '24px' }} />,
      color: '#ffc53d',
      description: 'Datos de población joven (18-28 años)',
      filename: 'JuventudNueva.csv'
    },
    {
      key: 'adulto',
      displayName: 'Adultez',
      icon: <UserSwitchOutlined style={{ fontSize: '24px' }} />,
      color: '#9254de',
      description: 'Datos de población adulta (29-59 años)',
      filename: 'AdultezNueva.csv'
    },
    {
      key: 'vejez',
      displayName: 'Vejez',
      icon: <UserDeleteOutlined style={{ fontSize: '24px' }} />,
      color: '#f759ab',
      description: 'Datos de población adulto mayor (60+ años)',
      filename: 'VejezNueva.csv'
    }
  ];

  const handleAgeGroupClick = async (ageGroup: AgeGroupIcon) => {
    if (ageGroup.filename) {
      const fileInfo = getFileByDisplayName(ageGroup.displayName);
      const filename = fileInfo?.filename || ageGroup.filename;
      
      await loadFileData(filename);
    } else {
      console.warn(`No filename configured for ${ageGroup.displayName}`);
    }
  };

  // ✅ Crear objeto de paginación extendido (sin showTotal en la interfaz)
  const extendedPagination = {
    ...pagination,
    // Remover showTotal de aquí ya que no está en la interfaz DataTableProps
  };

  return (
    <div style={{ padding: '24px' }}>
      {/* Header */}
      <Row justify="space-between" align="middle" style={{ marginBottom: 24 }}>
        <Col>
          <Title level={3} style={{ margin: 0 }}>
            Nota Técnica de Indicadores de Salud
          </Title>
          <Text type="secondary">
            Selecciona un grupo etario para ver sus indicadores
          </Text>
        </Col>
      </Row>

      {/* ✅ Indicador de progreso para carga inicial */}
      {loadingFiles && (
        <Card style={{ marginBottom: 16 }}>
          <Row align="middle">
            <Col span={24}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Text>
                  <ClockCircleOutlined spin /> Cargando lista de archivos técnicos...
                </Text>
                <Progress percent={100} status="active" showInfo={false} />
              </Space>
            </Col>
          </Row>
        </Card>
      )}

      {/* Iconos de Grupos Etarios */}
      <Card 
        title="Grupos Etarios" 
        size="small" 
        style={{ marginBottom: 24 }}
        bodyStyle={{ padding: '20px 24px' }}
      >
        <Row gutter={[16, 16]} justify="center">
          {ageGroups.map((group) => {
            const isSelected = selectedFile && (selectedFile.includes(group.key) || getFileByDisplayName(group.displayName)?.filename === selectedFile);
            const isAvailable = availableFiles.some(f => f.display_name === group.displayName);
            
            return (
              <Col key={group.key} xs={12} sm={8} md={4}>
                <Tooltip title={group.description}>
                  <div
                    style={{
                      textAlign: 'center',
                      cursor: isAvailable ? 'pointer' : 'not-allowed',
                      padding: '16px 8px',
                      borderRadius: '8px',
                      border: `2px solid ${isSelected ? group.color : '#f0f0f0'}`,
                      backgroundColor: isSelected ? `${group.color}15` : isAvailable ? '#fafafa' : '#f5f5f5',
                      transition: 'all 0.3s ease',
                      opacity: isAvailable ? 1 : 0.6
                    }}
                    onClick={() => isAvailable && handleAgeGroupClick(group)}
                    onMouseEnter={(e) => {
                      if (isAvailable) {
                        e.currentTarget.style.backgroundColor = `${group.color}25`;
                        e.currentTarget.style.transform = 'translateY(-2px)';
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (isAvailable) {
                        e.currentTarget.style.backgroundColor = isSelected ? `${group.color}15` : '#fafafa';
                        e.currentTarget.style.transform = 'translateY(0)';
                      }
                    }}
                  >
                    <div style={{ color: isAvailable ? group.color : '#bfbfbf', marginBottom: 8 }}>
                      {group.icon}
                    </div>
                    <div style={{ 
                      fontSize: '12px', 
                      fontWeight: isSelected ? 'bold' : 'normal',
                      color: isAvailable ? '#262626' : '#bfbfbf'
                    }}>
                      {group.displayName}
                    </div>
                    {!isAvailable && (
                      <div style={{ fontSize: '10px', color: '#ff4d4f', marginTop: 4 }}>
                        No disponible
                      </div>
                    )}
                  </div>
                </Tooltip>
              </Col>
            );
          })}
        </Row>
      </Card>

      {/* ✅ Información del archivo seleccionado con metadatos */}
      {(currentFileData || currentFileMetadata) && (
        <Card 
          size="small" 
          style={{ marginBottom: 16 }}
          bodyStyle={{ padding: '12px 16px' }}
        >
          <Row justify="space-between" align="middle">
            <Col flex="auto">
              <Space>
                <FileTextOutlined style={{ color: '#1890ff' }} />
                <Text strong>
                  {currentFileData?.display_name || currentFileMetadata?.display_name}
                </Text>
                <Text type="secondary">
                  ({currentFileData?.filename || currentFileMetadata?.filename})
                </Text>
                {loadingMetadata && (
                  <Spin size="small" />
                )}
              </Space>
            </Col>
          </Row>
        </Card>
      )}

      {/* ✅ Indicador de progreso para carga de datos */}
      {loading && currentFileMetadata && (
        <Card style={{ marginBottom: 16 }}>
          <Row align="middle">
            <Col span={24}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Text>
                  <ClockCircleOutlined spin /> Cargando {currentFileMetadata.display_name} - 
                  Página {currentPage} de {totalPages || '...'}
                </Text>
                {totalPages > 0 && (
                  <Progress 
                    percent={Math.round((currentPage / totalPages) * 100)} 
                    status="active"
                    strokeColor="#1890ff"
                  />
                )}
              </Space>
            </Col>
          </Row>
        </Card>
      )}

      {/* ✅ DATATABLE con paginación del servidor */}
      {loading ? (
        <Card>
          <div style={{ textAlign: 'center', padding: '60px 0' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16 }}>
              <Text>Cargando datos del archivo técnico...</Text>
              {currentPageInfo && (
                <div style={{ marginTop: 8 }}>
                  <Text type="secondary">{currentPageInfo}</Text>
                </div>
              )}
            </div>
          </div>
        </Card>
      ) : hasData ? (
        <div>          
          <DataTable
            data={filteredData}
            columns={columns}
            loading={loading}
            pagination={extendedPagination}
            onPaginationChange={handlePaginationChange}
            onFiltersChange={handleFiltersChange}
            onSortChange={handleSortChange}
            onDeleteRows={handleDeleteRows}
            onSearch={handleSearch}
          />
        </div>
      ) : (
        <Card>
          <div style={{ textAlign: 'center', padding: '60px 0' }}>
            <div style={{ fontSize: '48px', color: '#bfbfbf', marginBottom: 16 }}>
              📊
            </div>
            <Text type="secondary">
              Selecciona un grupo etario para ver sus indicadores
            </Text>
            
            {availableFiles.length === 0 && !loadingFiles && (
              <div style={{ marginTop: 16 }}>
                <Alert 
                  message="No se encontraron archivos técnicos" 
                  description="Verifica que los archivos estén en la carpeta technical_note del servidor"
                  type="warning" 
                  showIcon 
                />
              </div>
            )}
           
          </div>
        </Card>
      )}
    </div>
  );
};

export default TechnicalNoteViewer;
