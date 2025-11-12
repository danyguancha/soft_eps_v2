// components/technical-note/MainContent.tsx - ✅ CORREGIDO

import React from 'react';
import { Card, Spin, Typography, Space, Alert, Button } from 'antd';
import { BarChartOutlined, CloudUploadOutlined } from '@ant-design/icons';
import { Report } from './report/Report';

const { Title, Text } = Typography;

interface MainContentProps {
  loading: boolean;
  hasData: boolean;
  availableFiles: any[];
  uploadedFiles: any[];
  loadingFiles: boolean;
  currentPageInfo?: string;
  hasGeographicFilters: boolean;
  geographicSummary: string;
  
  // DataTable props
  filteredData: any[];
  columns: any[];
  selectedFile: string | null;
  pagination: any;
  
  // Report props
  keywordReport: any;
  loadingReport: boolean;
  showReport: boolean;
  hasReport: boolean;
  reportTotalRecords: number;
  reportKeywords: string[];
  reportMinCount: number;
  showTemporalData: boolean;
  geographicFilters: any;
  departamentosOptions: string[];
  municipiosOptions: string[];
  ipsOptions: string[];
  loadingGeoFilters: any;
  cutoffDate?: string; 
  
  // Event handlers
  onPaginationChange: any;
  onFiltersChange: any;
  onSortChange: any;
  onDeleteRows: any;
  onSearch: any;
  onToggleReportVisibility: any;
  onRegenerateReport: any;
  onSetReportKeywords: any;
  onSetReportMinCount: any;
  onSetShowTemporalData: any;
  onLoadKeywordAgeReport: any;
  onAddKeyword: any;
  onRemoveKeyword: any;
  onDepartamentoChange: any;
  onMunicipioChange: any;
  onIpsChange: any;
  resetGeographicFilters: any;
  onShowUploadModal: () => void;
}

export const MainContent: React.FC<MainContentProps> = (props) => {
  if (props.loading) {
    return (
      <Card>
        <div style={{ textAlign: 'center', padding: '60px 0' }}>
          <Spin size="large" />
          <div style={{ marginTop: 16 }}>
            <Text>Cargando datos del archivo técnico...</Text>
            {props.currentPageInfo && (
              <div style={{ marginTop: 8 }}>
                <Text type="secondary">{props.currentPageInfo}</Text>
              </div>
            )}
            {props.hasGeographicFilters && (
              <div style={{ marginTop: 8 }}>
                <Text type="success" style={{ fontSize: '12px' }}>
                  📍 Con filtros: {props.geographicSummary}
                </Text>
              </div>
            )}
          </div>
        </div>
      </Card>
    );
  }

  if (props.hasData) {
    return (
      <div>
        {/* <DataTable
          data={props.filteredData}
          columns={props.columns}
          filename={props.selectedFile ?? undefined}
          loading={props.loading}
          pagination={props.pagination}
          onPaginationChange={props.onPaginationChange}
          onFiltersChange={props.onFiltersChange}
          onSortChange={props.onSortChange}
          onDeleteRows={props.onDeleteRows}
          onSearch={props.onSearch}
        /> */}

        {/* ✅ COMPONENTE REPORT SIN reportItemsCount */}
        <Report
          keywordReport={props.keywordReport}
          loadingReport={props.loadingReport}
          showReport={props.showReport}
          hasReport={props.hasReport}
          reportTotalRecords={props.reportTotalRecords}
          selectedFile={props.selectedFile}
          reportKeywords={props.reportKeywords}
          reportMinCount={props.reportMinCount}
          showTemporalData={props.showTemporalData}
          geographicFilters={props.geographicFilters}
          departamentosOptions={props.departamentosOptions}
          municipiosOptions={props.municipiosOptions}
          ipsOptions={props.ipsOptions}
          loadingGeoFilters={props.loadingGeoFilters}
          cutoffDate={props.cutoffDate} // ✅ PASAR FECHA DE CORTE
          onToggleReportVisibility={props.onToggleReportVisibility}
          onSetReportKeywords={props.onSetReportKeywords}
          onSetShowTemporalData={props.onSetShowTemporalData}
          onLoadKeywordAgeReport={props.onLoadKeywordAgeReport}
          onDepartamentoChange={props.onDepartamentoChange}
          onMunicipioChange={props.onMunicipioChange}
          onIpsChange={props.onIpsChange}
          resetGeographicFilters={props.resetGeographicFilters}
        />
      </div>
    );
  }

  // Estado vacío
  return (
    <Card>
      <div style={{ textAlign: 'center', padding: '60px 0' }}>
        {props.availableFiles.length === 0 && props.uploadedFiles.length === 0 && !props.loadingFiles ? (
          <div style={{ marginTop: 16 }}>
            <Alert
              message="No se encontraron archivos"
              description="No hay archivos precargados ni personalizados disponibles. Carga tu propio archivo para comenzar."
              type="warning"
              showIcon
              action={
                <Button
                  type="primary"
                  icon={<CloudUploadOutlined />}
                  onClick={props.onShowUploadModal}
                >
                  Cargar Archivo
                </Button>
              }
            />
          </div>
        ) : (
          <div>
            <BarChartOutlined style={{ fontSize: 48, color: '#d9d9d9' }} />
            <div style={{ marginTop: 16 }}>
              <Title level={4} type="secondary">
                Selecciona un archivo
              </Title>
              <Space direction="vertical" size="small">
                <Text type="secondary">
                  Elige un archivo de la grilla de arriba para ver los datos
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
  );
};
