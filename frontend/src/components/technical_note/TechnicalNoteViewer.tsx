// components/technical-note/TechnicalNoteViewer.tsx - ✅ COMPLETO CON FILTROS GEOGRÁFICOS
import React, { useState } from 'react';
import { Card, Typography, Row, Col, Spin, Alert, Space, Tooltip, Progress, Button } from 'antd';
import { ClockCircleOutlined, ReloadOutlined, BarChartOutlined } from '@ant-design/icons';
import { useTechnicalNote } from '../../hooks/useTechnicalNote';
import { DataTable } from '../dataTable/DataTable';
import { Report } from './report/Report';

import primInfanciaIcon from '../../assets/icons/prim-inf.png';
import infanciaIcon from '../../assets/icons/infancia.png';
import adolescenciaIcon from '../../assets/icons/adolescencia.png';
import juventudIcon from '../../assets/icons/juventud.png';
import adultezIcon from '../../assets/icons/adulto.png';
import vejezIcon from '../../assets/icons/vejez.png';

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
  // ✅ Estados locales para configuración del reporte (mantener compatibilidad)
  const [fileSelectionLoading, setFileSelectionLoading] = useState(false);

  const {
    // Estados básicos
    availableFiles,
    currentFileMetadata,
    loading,
    loadingFiles,
    selectedFile,
    
    // Estados para DataTable
    filteredData,
    pagination,
    
    // Estados de paginación del servidor
    currentPage,
    totalPages,

    // ✅ Estados del reporte con filtros geográficos integrados
    keywordReport,
    loadingReport,
    showReport,
    hasReport,
    reportItemsCount,
    reportTotalRecords,
    reportKeywords,
    reportMinCount,
    showTemporalData,

    // ✅ Estados de filtros geográficos
    geographicFilters,
    departamentosOptions,
    municipiosOptions,
    ipsOptions,
    loadingGeoFilters,
    
    // Acciones básicas
    loadFileData,
    loadAvailableFiles,
    getFileByDisplayName,
    
    // Handlers para DataTable
    handlePaginationChange,
    handleFiltersChange,
    handleSortChange,
    handleDeleteRows,
    handleSearch,

    // ✅ Acciones del reporte con filtros geográficos
    loadKeywordAgeReport,
    toggleReportVisibility,
    regenerateReport,
    onSetReportKeywords,
    onSetReportMinCount,
    onSetShowTemporalData,
    onAddKeyword,
    onRemoveKeyword,

    // ✅ Handlers de filtros geográficos
    onDepartamentoChange,
    onMunicipioChange,
    onIpsChange,
    resetGeographicFilters,
    
    // Helpers
    hasData,
    columns,
    currentPageInfo,
    hasGeographicFilters,
    geographicSummary,
  } = useTechnicalNote();

  // Configuración de grupos etarios con iconos PNG
  const ageGroups: AgeGroupIcon[] = [
    {
      key: 'primera-infancia',
      displayName: 'Primera Infancia',
      icon: (
        <img 
          src={primInfanciaIcon} 
          alt="Primera Infancia" 
          style={{ 
            width: '32px', 
            height: '32px', 
            objectFit: 'contain',
            filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))'
          }} 
        />
      ),
      color: '#ff7a45',
      description: 'Datos de primera infancia (0-5 años)',
      filename: 'PrimeraInfanciaNueva.csv'
    },
    {
      key: 'infancia',
      displayName: 'Infancia',
      icon: (
        <img 
          src={infanciaIcon} 
          alt="Infancia" 
          style={{ 
            width: '32px', 
            height: '32px', 
            objectFit: 'contain',
            filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))'
          }} 
        />
      ),
      color: '#40a9ff',
      description: 'Datos de población infantil (6-11 años)',
      filename: 'InfanciaNueva.csv'
    },
    {
      key: 'adolescencia',
      displayName: 'Adolescencia',
      icon: (
        <img 
          src={adolescenciaIcon} 
          alt="Adolescencia" 
          style={{ 
            width: '32px', 
            height: '32px', 
            objectFit: 'contain',
            filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))'
          }} 
        />
      ),
      color: '#73d13d',
      description: 'Datos de población adolescente (12-17 años)',
      filename: 'AdolescenciaNueva.csv'
    },
    {
      key: 'juventud',
      displayName: 'Juventud',
      icon: (
        <img 
          src={juventudIcon} 
          alt="Juventud" 
          style={{ 
            width: '32px', 
            height: '32px', 
            objectFit: 'contain',
            filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))'
          }} 
        />
      ),
      color: '#ffc53d',
      description: 'Datos de población joven (18-28 años)',
      filename: 'JuventudNueva.csv'
    },
    {
      key: 'adulto',
      displayName: 'Adultez',
      icon: (
        <img 
          src={adultezIcon} 
          alt="Adultez" 
          style={{ 
            width: '32px', 
            height: '32px', 
            objectFit: 'contain',
            filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))'
          }} 
        />
      ),
      color: '#9254de',
      description: 'Datos de población adulta (29-59 años)',
      filename: 'AdultezNueva.csv'
    },
    {
      key: 'vejez',
      displayName: 'Vejez',
      icon: (
        <img 
          src={vejezIcon} 
          alt="Vejez" 
          style={{ 
            width: '32px', 
            height: '32px', 
            objectFit: 'contain',
            filter: 'drop-shadow(0 2px 4px rgba(0,0,0,0.1))'
          }} 
        />
      ),
      color: '#f759ab',
      description: 'Datos de población adulto mayor (60+ años)',
      filename: 'VejezNueva.csv'
    }
  ];

  // ✅ HANDLER PARA SELECCIÓN DE GRUPOS ETARIOS
  const handleAgeGroupClick = async (ageGroup: AgeGroupIcon) => {
    if (ageGroup.filename) {
      try {
        setFileSelectionLoading(true);
        
        const fileInfo = getFileByDisplayName(ageGroup.displayName);
        const filename = fileInfo?.filename || ageGroup.filename;
        
        console.log(`🔍 Cargando archivo: ${filename} para ${ageGroup.displayName}`);
        await loadFileData(filename);
        
        console.log(`✅ Archivo cargado exitosamente: ${ageGroup.displayName}`);
      } catch (error) {
        console.error(`❌ Error cargando ${ageGroup.displayName}:`, error);
      } finally {
        setFileSelectionLoading(false);
      }
    } else {
      console.warn(`No filename configured for ${ageGroup.displayName}`);
    }
  };

  // ✅ HANDLERS PARA COMPATIBILIDAD (delegación al hook)
  const handleRegenerateReport = () => {
    console.log(`🔄 Regenerando reporte con filtros geográficos:`, geographicFilters);
    regenerateReport();
  };

  const handleAddKeyword = (value: string) => {
    console.log(`➕ Agregando palabra clave: ${value}`);
    onAddKeyword(value);
  };

  const handleRemoveKeyword = (keyword: string) => {
    console.log(`➖ Removiendo palabra clave: ${keyword}`);
    onRemoveKeyword(keyword);
  };

  const handleSetReportKeywords = (keywords: string[]) => {
    console.log(`🔧 Estableciendo nuevas palabras clave: ${keywords}`);
    onSetReportKeywords(keywords);
  };

  return (
    <div style={{ padding: '24px' }}>
      {/* ✅ HEADER CON INFORMACIÓN DE FILTROS GEOGRÁFICOS */}
      <Row justify="space-between" align="middle" style={{ marginBottom: 24 }}>
        <Col>
          <Title level={3} style={{ margin: 0 }}>
            <BarChartOutlined /> Nota Técnica
          </Title>
          <Space direction="vertical" size="small">
            <Text type="secondary">
              Selecciona un grupo etario para ver datos y reportes con filtros geográficos
            </Text>
            {hasGeographicFilters && (
              <Text type="success" style={{ fontSize: '12px' }}>
                📍 Filtros activos: {geographicSummary}
              </Text>
            )}
          </Space>
        </Col>
        <Col>
          <Space>
            {hasGeographicFilters && (
              <Button 
                size="small"
                onClick={resetGeographicFilters}
                type="default"
              >
                Limpiar filtros
              </Button>
            )}
            <Button 
              icon={<ReloadOutlined />} 
              onClick={loadAvailableFiles}
              loading={loadingFiles}
            >
              Actualizar
            </Button>
          </Space>
        </Col>
      </Row>

      {/* ✅ INDICADOR DE PROGRESO PARA CARGA INICIAL */}
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

      {/* ✅ ICONOS DE GRUPOS ETARIOS CON INDICADOR DE FILTROS */}
      <Card 
        title={
          <Space>
            <span>Grupos Etarios</span>
            {hasGeographicFilters && (
              <Text type="secondary" style={{ fontSize: '12px' }}>
                (Con filtros geográficos aplicados)
              </Text>
            )}
          </Space>
        }
        size="small" 
        style={{ marginBottom: 24 }}
        bodyStyle={{ padding: '20px 24px' }}
      >
        <Row gutter={[16, 16]} justify="center">
          {ageGroups.map((group) => {
            const isSelected = selectedFile && getFileByDisplayName(group.displayName)?.filename === selectedFile;
            const isAvailable = availableFiles.some(f => f.display_name === group.displayName);
            const isLoading = fileSelectionLoading && isSelected;
            
            return (
              <Col key={group.key} xs={12} sm={8} md={4}>
                <Tooltip title={group.description}>
                  <div
                    style={{
                      textAlign: 'center',
                      cursor: isAvailable ? 'pointer' : 'not-allowed',
                      padding: '16px 8px',
                      borderRadius: '12px',
                      border: `2px solid ${isSelected ? group.color : '#f0f0f0'}`,
                      backgroundColor: isSelected 
                        ? `${group.color}15` 
                        : isAvailable ? '#fafafa' : '#f5f5f5',
                      transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                      opacity: isAvailable ? 1 : 0.6,
                      boxShadow: isSelected 
                        ? `0 4px 12px ${group.color}40` 
                        : '0 2px 8px rgba(0,0,0,0.06)',
                      transform: isSelected ? 'translateY(-1px)' : 'translateY(0)',
                      position: 'relative'
                    }}
                    onClick={() => isAvailable && !isLoading && handleAgeGroupClick(group)}
                    onMouseEnter={(e) => {
                      if (isAvailable && !isSelected && !isLoading) {
                        e.currentTarget.style.backgroundColor = `${group.color}10`;
                        e.currentTarget.style.transform = 'translateY(-3px)';
                        e.currentTarget.style.boxShadow = `0 6px 16px ${group.color}30`;
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (isAvailable && !isSelected && !isLoading) {
                        e.currentTarget.style.backgroundColor = '#fafafa';
                        e.currentTarget.style.transform = 'translateY(0)';
                        e.currentTarget.style.boxShadow = '0 2px 8px rgba(0,0,0,0.06)';
                      }
                    }}
                  >
                    {/* ✅ INDICADOR DE CARGA */}
                    {isLoading && (
                      <div style={{
                        position: 'absolute',
                        top: 0,
                        left: 0,
                        right: 0,
                        bottom: 0,
                        backgroundColor: 'rgba(255,255,255,0.8)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        borderRadius: '10px'
                      }}>
                        <Spin size="small" />
                      </div>
                    )}
                    
                    <div style={{ 
                      marginBottom: 12,
                      display: 'flex',
                      justifyContent: 'center',
                      alignItems: 'center',
                      height: '40px',
                      opacity: isAvailable && !isLoading ? 1 : 0.5,
                      filter: !isAvailable ? 'grayscale(100%)' : 'none'
                    }}>
                      {group.icon}
                    </div>
                    
                    <div style={{ 
                      fontSize: '13px', 
                      fontWeight: isSelected ? 600 : 500,
                      color: isAvailable ? '#262626' : '#bfbfbf',
                      lineHeight: 1.3
                    }}>
                      {group.displayName}
                    </div>
                    
                    {!isAvailable && (
                      <div style={{ 
                        fontSize: '10px', 
                        color: '#ff4d4f', 
                        marginTop: 6,
                        fontWeight: 500 
                      }}>
                        No disponible
                      </div>
                    )}
                    
                    {isSelected && !isLoading && (
                      <div style={{ 
                        fontSize: '10px', 
                        color: group.color, 
                        marginTop: 4,
                        fontWeight: 600,
                        textTransform: 'uppercase',
                        letterSpacing: '0.5px'
                      }}>
                        ● Activo
                      </div>
                    )}
                  </div>
                </Tooltip>
              </Col>
            );
          })}
        </Row>
      </Card>

      {/* ✅ INDICADOR DE PROGRESO PARA CARGA DE DATOS */}
      {loading && currentFileMetadata && (
        <Card style={{ marginBottom: 16 }}>
          <Row align="middle">
            <Col span={24}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Text>
                  <ClockCircleOutlined spin /> Cargando datos con filtros geográficos...
                </Text>
                {totalPages > 0 && (
                  <Progress 
                    percent={Math.round((currentPage / totalPages) * 100)} 
                    status="active"
                    strokeColor="#1890ff"
                  />
                )}
                {hasGeographicFilters && (
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    📍 Aplicando filtros: {geographicSummary}
                  </Text>
                )}
              </Space>
            </Col>
          </Row>
        </Card>
      )}

      {/* ✅ CONTENIDO PRINCIPAL */}
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
              {hasGeographicFilters && (
                <div style={{ marginTop: 8 }}>
                  <Text type="success" style={{ fontSize: '12px' }}>
                    📍 Con filtros: {geographicSummary}
                  </Text>
                </div>
              )}
            </div>
          </div>
        </Card>
      ) : hasData ? (
        <div>
          {/* ✅ TABLA PRINCIPAL DE DATOS */}
          {/* <DataTable
            data={filteredData}
            columns={columns}
            filename={selectedFile ?? undefined}
            loading={loading}
            pagination={pagination}
            onPaginationChange={handlePaginationChange}
            onFiltersChange={handleFiltersChange}
            onSortChange={handleSortChange}
            onDeleteRows={handleDeleteRows}
            onSearch={handleSearch}
          /> */}

          {/* ✅ COMPONENTE DE REPORTE CON FILTROS GEOGRÁFICOS INTEGRADOS */}
          <Report
            keywordReport={keywordReport}
            loadingReport={loadingReport}
            showReport={showReport}
            hasReport={hasReport}
            reportItemsCount={reportItemsCount}
            reportTotalRecords={reportTotalRecords}
            selectedFile={selectedFile}
            reportKeywords={reportKeywords}
            reportMinCount={reportMinCount}
            showTemporalData={showTemporalData}
            
            // ✅ PROPS DE FILTROS GEOGRÁFICOS
            geographicFilters={geographicFilters}
            departamentosOptions={departamentosOptions}
            municipiosOptions={municipiosOptions}
            ipsOptions={ipsOptions}
            loadingGeoFilters={loadingGeoFilters}
            
            // Handlers existentes
            onToggleReportVisibility={toggleReportVisibility}
            onRegenerateReport={handleRegenerateReport}
            onSetReportKeywords={handleSetReportKeywords}
            onSetReportMinCount={onSetReportMinCount}
            onSetShowTemporalData={onSetShowTemporalData}
            onLoadKeywordAgeReport={loadKeywordAgeReport}
            onAddKeyword={handleAddKeyword}
            onRemoveKeyword={handleRemoveKeyword}
            
            // ✅ HANDLERS DE FILTROS GEOGRÁFICOS
            onDepartamentoChange={onDepartamentoChange}
            onMunicipioChange={onMunicipioChange}
            onIpsChange={onIpsChange}
            resetGeographicFilters={resetGeographicFilters}
          />
        </div>
      ) : (
        <Card>
          <div style={{ textAlign: 'center', padding: '60px 0' }}>
            {availableFiles.length === 0 && !loadingFiles ? (
              <div style={{ marginTop: 16 }}>
                <Alert 
                  message="No se encontraron archivos técnicos" 
                  description="Verifica que los archivos estén en la carpeta technical_note del servidor"
                  type="warning" 
                  showIcon 
                />
              </div>
            ) : (
              <div>
                <BarChartOutlined style={{ fontSize: 48, color: '#d9d9d9' }} />
                <div style={{ marginTop: 16 }}>
                  <Title level={4} type="secondary">
                    Selecciona un grupo etario
                  </Title>
                  <Space direction="vertical" size="small">
                    <Text type="secondary">
                      Elige un grupo etario de los iconos de arriba para ver los datos
                    </Text>
                    <Text type="secondary">
                      Podrás generar reportes con filtros geográficos y múltiples palabras clave
                    </Text>
                  </Space>
                </div>
              </div>
            )}
          </div>
        </Card>
      )}
    </div>
  );
};

export default TechnicalNoteViewer;
