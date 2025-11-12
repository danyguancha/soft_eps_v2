// components/cross/FileCrossManager.tsx
import React, { useState, useEffect } from 'react';
import {
  Card, Typography, Row, Col, Form, Select, Button, Checkbox, Steps, Alert, Space, Spin, 
  Divider, message, Tag, Modal, Progress
} from 'antd';
import {
  SwapOutlined, CheckCircleOutlined, DownloadOutlined, PlayCircleOutlined, 
  ArrowLeftOutlined, ReloadOutlined, WarningOutlined
} from '@ant-design/icons';
import type { FileInfo } from '../../types/api.types';
import { CrossService, type FileCrossRequest } from '../../services/CrossService';

const { Title, Text } = Typography;
const { Option } = Select;

interface FileCrossManagerProps {
  availableFiles: FileInfo[];
  onRefreshFiles: () => void;
  onCrossComplete?: (result: any) => void;
  onComplete: () => void;
}

const FileCrossManager: React.FC<FileCrossManagerProps> = ({
  availableFiles,
  onRefreshFiles,
  onCrossComplete
}) => {
  const [currentStep, setCurrentStep] = useState(0);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [form] = Form.useForm();

  // Estados para configuración
  const [file1, setFile1] = useState<FileInfo | null>(null);
  const [file2, setFile2] = useState<FileInfo | null>(null);
  const [file1Columns, setFile1Columns] = useState<string[]>([]);
  const [file2Columns, setFile2Columns] = useState<string[]>([]);
  const [selectedColumnsToAdd, setSelectedColumnsToAdd] = useState<string[]>([]);
  const [crossResult, setCrossResult] = useState<any>(null);

  // Estados para archivos grandes
  const [isLargeFile, setIsLargeFile] = useState(false);
  const [downloadProgress, setDownloadProgress] = useState(0);

  useEffect(() => {
    console.log('📁 availableFiles changed:', availableFiles.length);
    
    if (availableFiles.length === 0 && currentStep < 3) {
      handleRefreshFiles();
    }
    
    if (currentStep === 0) {
      const file1Id = form.getFieldValue('file1_id');
      const file2Id = form.getFieldValue('file2_id');
      
      if (file1Id && !availableFiles.find(f => f.file_id === file1Id)) {
        form.setFieldValue('file1_id', undefined);
        setFile1(null);
        setFile1Columns([]);
      }
      
      if (file2Id && !availableFiles.find(f => f.file_id === file2Id)) {
        form.setFieldValue('file2_id', undefined);
        setFile2(null);
        setFile2Columns([]);
        setSelectedColumnsToAdd([]);
      }
    }
  }, [availableFiles, currentStep, form]);

  useEffect(() => {
    if (availableFiles.length === 0) {
      handleRefreshFiles();
    }
  }, []);

  // CORRECCIÓN: Detectar archivos grandes
  useEffect(() => {
    if (file1 && file2) {
      const totalRows = (file1.total_rows || 0) + (file2.total_rows || 0);
      const totalColumns = (file1.columns?.length || 0) + (file2.columns?.length || 0);
      const isLarge = totalRows > 100000 || totalColumns > 200;
      
      setIsLargeFile(isLarge);
      
      if (isLarge) {
        // CORRECCIÓN: Usar .toLocaleString() en lugar de :,
        console.log(`🔔 Archivo grande detectado: ${totalRows.toLocaleString()} filas, ${totalColumns} columnas`);
      }
    }
  }, [file1, file2]);

  const handleRefreshFiles = async () => {
    try {
      setRefreshing(true);
      await onRefreshFiles();
      message.success('Lista de archivos actualizada');
    } catch (error) {
      message.error('Error al actualizar la lista de archivos');
    } finally {
      setRefreshing(false);
    }
  };

  const steps = [
    {
      title: 'Seleccionar Archivos',
      icon: <SwapOutlined />,
    },
    {
      title: 'Configurar Cruce',
      icon: <SwapOutlined />,
    },
    {
      title: 'Columnas a Agregar',
      icon: <CheckCircleOutlined />,
    },
    {
      title: 'Resultado',
      icon: isLargeFile ? <DownloadOutlined /> : <PlayCircleOutlined />,
    },
  ];

  const loadFileColumns = async (fileId: string, sheetName: string | undefined, fileNumber: 1 | 2) => {
    try {
      setLoading(true);
      const response = await CrossService.getFileColumnsForCross(fileId, sheetName);
      const columns = response.columns;

      if (fileNumber === 1) {
        setFile1Columns(columns);
      } else {
        setFile2Columns(columns);
        setSelectedColumnsToAdd([]);
      }
    } catch (error: any) {
      message.error(`Error al cargar columnas del archivo ${fileNumber}: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleNext = async () => {
    try {
      const allValues = form.getFieldsValue(true);

      let fieldsToValidate: string[] = [];

      if (currentStep === 0) {
        fieldsToValidate = ['file1_id', 'file2_id'];
      } else if (currentStep === 1) {
        fieldsToValidate = ['key_column_file1', 'key_column_file2'];
      }

      if (fieldsToValidate.length > 0) {
        await form.validateFields(fieldsToValidate);
      }

      if (currentStep === 0) {
        const selectedFile1 = availableFiles.find(f => f.file_id === allValues.file1_id);
        const selectedFile2 = availableFiles.find(f => f.file_id === allValues.file2_id);

        if (!selectedFile1 || !selectedFile2) {
          message.error('Debe seleccionar ambos archivos');
          return;
        }

        if (selectedFile1.file_id === selectedFile2.file_id) {
          message.error('No puede seleccionar el mismo archivo dos veces');
          return;
        }

        setFile1(selectedFile1);
        setFile2(selectedFile2);

        await Promise.all([
          loadFileColumns(allValues.file1_id, allValues.file1_sheet, 1),
          loadFileColumns(allValues.file2_id, allValues.file2_sheet, 2)
        ]);

      } else if (currentStep === 2) {
        if (!allValues.key_column_file1 || !allValues.key_column_file2) {
          message.error('Error: Columnas clave no configuradas');
          return;
        }

        if (selectedColumnsToAdd.length === 0) {
          message.error('Debe seleccionar al menos una columna para agregar');
          return;
        }

        await handleExecuteCross(allValues);
      }

      setCurrentStep(currentStep + 1);
    } catch (error) {
      console.error('❌ Error en handleNext:', error);
    }
  };

  const handleExecuteCross = async (allValues: any) => {
    try {
      const totalRows = (file1?.total_rows || 0) + (file2?.total_rows || 0);
      const totalColumns = (file1?.columns?.length || 0) + (file2?.columns?.length || 0);
      
      const request: FileCrossRequest = {
        file1_key: allValues.file1_id,
        file2_key: allValues.file2_id,
        file1_sheet: allValues.file1_sheet || undefined,
        file2_sheet: allValues.file2_sheet || undefined,
        key_column_file1: allValues.key_column_file1,
        key_column_file2: allValues.key_column_file2,
        cross_type: 'left',
        columns_to_include: {
          file1_columns: file1Columns,
          file2_columns: selectedColumnsToAdd
        }
      };

      if (isLargeFile) {
        await handleLargeFileCross(request, totalRows, totalColumns);
      } else {
        await handleSmallFileCross(request);
      }

    } catch (error: any) {
      console.error('❌ Error en cruce:', error);
      message.error(error.message || 'Error al ejecutar el cruce');
    }
  };

  const handleSmallFileCross = async (request: FileCrossRequest) => {
    setLoading(true);
    
    try {
      const result = await CrossService.crossFiles(request);
      
      if (result.error === 'LARGE_FILE_DETECTED') {
        await handleLargeFileDetected(request, result);
        return;
      }
      
      setCrossResult(result);
      
      if (onCrossComplete) {
        onCrossComplete(result);
      }
      
      // CORRECCIÓN: Usar .toLocaleString()
      message.success(`Cruce completado: ${result.total_rows.toLocaleString()} registros procesados`);
      
    } finally {
      setLoading(false);
    }
  };

  const handleLargeFileCross = async (request: FileCrossRequest, totalRows: number, totalColumns: number) => {
    Modal.confirm({
      title: '📊 Archivo Grande Detectado',
      icon: <WarningOutlined style={{ color: '#faad14' }} />,
      content: (
        <div>
          {/* CORRECCIÓN: Usar .toLocaleString() */}
          <p>Este cruce procesará <strong>{totalRows.toLocaleString()} registros</strong> con <strong>{totalColumns} columnas</strong>.</p>
          <p>Debido al tamaño, el resultado se <strong>descargará automáticamente como archivo CSV</strong> para optimizar el rendimiento.</p>
          <Alert 
            message="El proceso puede tardar varios minutos"
            description="El archivo se descargará automáticamente cuando esté listo"
            type="info"
            showIcon
            style={{ marginTop: 16 }}
          />
        </div>
      ),
      width: 520,
      okText: '🚀 Procesar y Descargar',
      cancelText: 'Cancelar',
      onOk: async () => {
        setLoading(true);
        setDownloadProgress(0);
        
        try {
          const progressInterval = setInterval(() => {
            setDownloadProgress(prev => {
              if (prev >= 95) return prev;
              return prev + Math.random() * 5;
            });
          }, 1000);
          
          await CrossService.crossFilesDownload(request);
          
          clearInterval(progressInterval);
          setDownloadProgress(100);
          
          setCrossResult({
            success: true,
            total_rows: totalRows,
            download_completed: true,
            message: 'Archivo descargado exitosamente'
          });
          
          // CORRECCIÓN: Usar .toLocaleString()
          message.success(`Cruce completado y descargado: ${totalRows.toLocaleString()} registros`);
          
        } catch (error: any) {
          setDownloadProgress(0);
          if (error.message.includes('timeout')) {
            // CORRECCIÓN: Usar .toLocaleString()
            message.warning(`⏱️ El proceso está tardando más de lo esperado. Esto es normal con archivos de ${totalRows.toLocaleString()} filas.`);
          } else {
            throw error;
          }
        } finally {
          setLoading(false);
        }
      }
    });
  };

  const handleLargeFileDetected = async (request: FileCrossRequest, result: any) => {
    const { total_rows, total_columns } = result;
    
    Modal.info({
      title: '🔔 Archivo Grande Detectado por el Servidor',
      content: (
        <div>
          <p>El servidor detectó que este cruce requiere procesamiento especial:</p>
          <ul>
            {/* CORRECCIÓN: Usar .toLocaleString() */}
            <li><strong>Total filas:</strong> {total_rows.toLocaleString()}</li>
            <li><strong>Total columnas:</strong> {total_columns}</li>
          </ul>
          <p>Se iniciará automáticamente la descarga optimizada.</p>
        </div>
      ),
      onOk: () => handleLargeFileCross(request, total_rows, total_columns)
    });
  };

  const handlePrev = () => {
    setCurrentStep(currentStep - 1);
  };

  const resetWizard = () => {
    setCurrentStep(0);
    setFile1(null);
    setFile2(null);
    setFile1Columns([]);
    setFile2Columns([]);
    setSelectedColumnsToAdd([]);
    setCrossResult(null);
    setIsLargeFile(false);
    setDownloadProgress(0);
    form.resetFields();
  };

  const renderStepContent = () => {
    switch (currentStep) {
      case 0:
        return (
          <Row gutter={[16, 16]}>
            <Col xs={24}>
              <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Text strong>Archivos disponibles: {availableFiles.length}</Text>
                <Button
                  icon={<ReloadOutlined />}
                  onClick={handleRefreshFiles}
                  loading={refreshing}
                  size="small"
                >
                  Actualizar Lista
                </Button>
              </div>

              {availableFiles.length === 0 && (
                <Alert
                  message="No hay archivos disponibles"
                  description="No se encontraron archivos cargados. Asegúrate de haber subido archivos antes de intentar hacer un cruce."
                  type="warning"
                  style={{ marginBottom: 16 }}
                  action={
                    <Button size="small" onClick={handleRefreshFiles} loading={refreshing}>
                      Actualizar
                    </Button>
                  }
                />
              )}

              {isLargeFile && (
                <Alert
                  message="📊 Archivo Grande Detectado"
                  description={
                    // CORRECCIÓN: Usar .toLocaleString()
                    `Total: ${((file1?.total_rows || 0) + (file2?.total_rows || 0)).toLocaleString()} filas. El resultado se descargará como CSV para optimizar el rendimiento.`
                  }
                  type="info"
                  showIcon
                  icon={<DownloadOutlined />}
                  style={{ marginBottom: 16 }}
                />
              )}
            </Col>

            <Col xs={24} md={12}>
              <Card title="📁 Archivo Principal" size="small">
                <Alert
                  message="Archivo Principal"
                  description="Este archivo mantendrá todos sus registros y columnas. Los datos del segundo archivo se agregarán como nuevas columnas."
                  type="info"
                  style={{ marginBottom: 16 }}
                />

                <Form.Item
                  name="file1_id"
                  label="Seleccionar archivo principal"
                  rules={[{ required: true, message: 'Seleccione el archivo principal' }]}
                >
                  <Select
                    placeholder="Archivo que mantendrá todos sus datos"
                    onChange={(value) => {
                      const selected = availableFiles.find(f => f.file_id === value);
                      setFile1(selected || null);
                      form.setFieldsValue({ file1_sheet: undefined });
                    }}
                    notFoundContent={refreshing ? <Spin size="small" /> : "No hay archivos disponibles"}
                    loading={refreshing}
                  >
                    {availableFiles.map((file) => (
                      <Option key={file.file_id} value={file.file_id}>
                        <div>
                          {file.original_name}
                          <br />
                          <Text type="secondary" style={{ fontSize: '12px' }}>
                            {/* CORRECCIÓN: Usar .toLocaleString() */}
                            {file.total_rows?.toLocaleString()} filas, {file.columns?.length} columnas
                            {(file.total_rows || 0) > 100000 && <Tag color="orange"  style={{ marginLeft: 4 }}>Grande</Tag>}
                          </Text>
                        </div>
                      </Option>
                    ))}
                  </Select>
                </Form.Item>

                <Form.Item name="file1_sheet" label="Hoja (opcional)">
                  <Select
                    placeholder="Seleccione la hoja"
                    allowClear
                    disabled={!file1?.sheets?.length}
                  >
                    {file1?.sheets?.map((sheet) => (
                      <Option key={sheet} value={sheet}>{sheet}</Option>
                    ))}
                  </Select>
                </Form.Item>

                {file1 && (
                  <Alert
                    message={file1.original_name}
                    description={
                      // CORRECCIÓN: Usar .toLocaleString()
                      `${file1.total_rows?.toLocaleString()} filas, ${file1.columns?.length} columnas`
                    }
                    type="success"
                  />
                )}
              </Card>
            </Col>

            <Col xs={24} md={12}>
              <Card title="🔍 Archivo de Búsqueda" size="small">
                <Alert
                  message="Archivo de Búsqueda"
                  description="Solo las columnas seleccionadas de este archivo se agregarán al resultado final."
                  type="warning"
                  style={{ marginBottom: 16 }}
                />

                <Form.Item
                  name="file2_id"
                  label="Seleccionar archivo de búsqueda"
                  rules={[{ required: true, message: 'Seleccione el archivo de búsqueda' }]}
                >
                  <Select
                    placeholder="Archivo del cual traer datos adicionales"
                    onChange={(value) => {
                      const selected = availableFiles.find(f => f.file_id === value);
                      setFile2(selected || null);
                      form.setFieldsValue({ file2_sheet: undefined });
                    }}
                    notFoundContent={refreshing ? <Spin size="small" /> : "No hay archivos disponibles"}
                    loading={refreshing}
                  >
                    {availableFiles.map((file) => (
                      <Option key={file.file_id} value={file.file_id}>
                        <div>
                          {file.original_name}
                          <br />
                          <Text type="secondary" style={{ fontSize: '12px' }}>
                            {/* CORRECCIÓN: Usar .toLocaleString() */}
                            {file.total_rows?.toLocaleString()} filas, {file.columns?.length} columnas
                            {(file.total_rows || 0) > 100000 && <Tag color="orange" style={{ marginLeft: 4 }}>Grande</Tag>}
                          </Text>
                        </div>
                      </Option>
                    ))}
                  </Select>
                </Form.Item>

                <Form.Item name="file2_sheet" label="Hoja (opcional)">
                  <Select
                    placeholder="Seleccione la hoja"
                    allowClear
                    disabled={!file2?.sheets?.length}
                  >
                    {file2?.sheets?.map((sheet) => (
                      <Option key={sheet} value={sheet}>{sheet}</Option>
                    ))}
                  </Select>
                </Form.Item>

                {file2 && (
                  <Alert
                    message={file2.original_name}
                    description={
                      // CORRECCIÓN: Usar .toLocaleString()
                      `${file2.total_rows?.toLocaleString()} filas, ${file2.columns?.length} columnas`
                    }
                    type="warning"
                  />
                )}
              </Card>
            </Col>
          </Row>
        );

      case 1:
        return (
          <Row gutter={[16, 16]}>
            <Col xs={24}>
              <Card title="🔑 Configurar Columnas de Cruce" size="small">
                <Alert
                  message="Columnas Clave para el Cruce"
                  description="Estas columnas se usarán para encontrar registros coincidentes entre los dos archivos. Los valores deben ser únicos o al menos consistentes."
                  type="info"
                  style={{ marginBottom: 16 }}
                />

                <Row gutter={[16, 16]}>
                  <Col xs={24} md={12}>
                    <Form.Item
                      name="key_column_file1"
                      label={`Columna clave en ${file1?.original_name}`}
                      rules={[{ required: true, message: 'Seleccione la columna clave' }]}
                    >
                      <Select placeholder="Ej: ID, Código, DNI">
                        {file1Columns.map((column) => (
                          <Option key={column} value={column}>{column}</Option>
                        ))}
                      </Select>
                    </Form.Item>
                  </Col>

                  <Col xs={24} md={12}>
                    <Form.Item
                      name="key_column_file2"
                      label={`Columna clave en ${file2?.original_name}`}
                      rules={[{ required: true, message: 'Seleccione la columna clave' }]}
                    >
                      <Select placeholder="Ej: ID, Código, DNI">
                        {file2Columns.map((column) => (
                          <Option key={column} value={column}>{column}</Option>
                        ))}
                      </Select>
                    </Form.Item>
                  </Col>
                </Row>
              </Card>
            </Col>
          </Row>
        );

      case 2:
        return (
          <Row gutter={[16, 16]}>
            <Col xs={24}>
              <Card title="📊 Seleccionar Columnas a Agregar" size="small">
                <Alert
                  message="Resultado Final"
                  description={`El resultado tendrá TODAS las columnas de "${file1?.original_name}" (${file1Columns.length} columnas) + las columnas seleccionadas de "${file2?.original_name}".`}
                  type="success"
                  style={{ marginBottom: 16 }}
                />

                {isLargeFile && (
                  <Alert
                    message="💾 Descarga Automática"
                    description="Debido al tamaño del archivo, el resultado se descargará automáticamente como CSV al completar el proceso."
                    type="info"
                    showIcon
                    icon={<DownloadOutlined />}
                    style={{ marginBottom: 16 }}
                  />
                )}

                <div style={{ marginBottom: 16 }}>
                  <Space>
                    <Button
                      size="small"
                      onClick={() => setSelectedColumnsToAdd(file2Columns)}
                    >
                      Seleccionar Todas
                    </Button>
                    <Button
                      size="small"
                      onClick={() => setSelectedColumnsToAdd([])}
                    >
                      Deseleccionar Todas
                    </Button>
                  </Space>
                </div>

                <div style={{
                  maxHeight: 300,
                  overflow: 'auto',
                  border: '1px solid #f0f0f0',
                  padding: 16,
                  borderRadius: 6,
                  backgroundColor: '#fafafa'
                }}>
                  <Checkbox.Group
                    value={selectedColumnsToAdd}
                    onChange={setSelectedColumnsToAdd}
                    style={{ width: '100%' }}
                  >
                    <Row>
                      {file2Columns.map((column) => (
                        <Col xs={24} key={column} style={{ marginBottom: 8 }}>
                          <Checkbox value={column}>
                            <strong>{column}</strong>
                          </Checkbox>
                        </Col>
                      ))}
                    </Row>
                  </Checkbox.Group>
                </div>

                <div style={{ marginTop: 16 }}>
                  <Alert
                    message={`${selectedColumnsToAdd.length} de ${file2Columns.length} columnas seleccionadas para agregar`}
                    type={selectedColumnsToAdd.length > 0 ? "success" : "warning"}
                  />
                </div>
              </Card>
            </Col>
          </Row>
        );

      case 3:
        return (
          <div>
            {crossResult && (
              <div>
                <Alert
                  message="🎉 Cruce Completado Exitosamente"
                  description={
                    crossResult.download_completed 
                      ? // CORRECCIÓN: Usar .toLocaleString()
                        `Se procesaron ${crossResult.total_rows?.toLocaleString()} registros y el archivo fue descargado automáticamente.`
                      : `Se procesaron ${crossResult.total_rows?.toLocaleString()} registros con ${crossResult.columns?.length} columnas.`
                  }
                  type="success"
                  style={{ marginBottom: 16 }}
                />

                {isLargeFile && loading && (
                  <Card title="📥 Progreso de Descarga" style={{ marginBottom: 16 }}>
                    <Progress 
                      percent={Math.round(downloadProgress)} 
                      status={downloadProgress >= 100 ? 'success' : 'active'}
                      strokeColor={{
                        '0%': '#108ee9',
                        '100%': '#87d068',
                      }}
                    />
                    <div style={{ marginTop: 8, textAlign: 'center' }}>
                      <Text type="secondary">
                        {downloadProgress >= 100 ? 'Descarga completada' : 'Procesando y descargando...'}
                      </Text>
                    </div>
                  </Card>
                )}

                {crossResult.download_completed && (
                  <Alert
                    message="📁 Archivo Descargado"
                    description="El archivo CSV ha sido descargado a tu carpeta de descargas. Puedes abrirlo con Excel o cualquier editor de hojas de cálculo."
                    type="info"
                    showIcon
                    icon={<DownloadOutlined />}
                  />
                )}
              </div>
            )}
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <Spin spinning={loading} tip={
      isLargeFile && loading ? (
        <div>
          <div>Procesando archivo grande...</div>
          <div style={{ fontSize: '12px', marginTop: 4 }}>
            {downloadProgress > 0 && `${Math.round(downloadProgress)}% completado`}
          </div>
        </div>
      ) : "Procesando cruce..."
    }>
      <div style={{ padding: '24px' }}>
        <Title level={4} style={{ marginBottom: 24 }}>
          🔄 Cruzar Archivos
          {isLargeFile && (
            <Tag color="processing" style={{ marginLeft: 8 }}>
              <DownloadOutlined /> Descarga Automática
            </Tag>
          )}
        </Title>

        <Steps current={currentStep} items={steps} style={{ marginBottom: 32 }} />

        <Form
          form={form}
          layout="vertical"
          style={{ minHeight: 400 }}
          preserve={true}
        >
          {renderStepContent()}
        </Form>

        <Divider />

        <div style={{ textAlign: 'right' }}>
          <Space>
            {currentStep > 0 && (
              <Button
                onClick={handlePrev}
                disabled={loading}
                icon={currentStep === 3 ? <ArrowLeftOutlined /> : undefined}
              >
                ← {currentStep === 3 ? 'Configurar Nuevo Cruce' : 'Atrás'}
              </Button>
            )}

            {currentStep < 3 && (
              <Button
                type="primary"
                onClick={handleNext}
                loading={loading}
                disabled={
                  (currentStep === 2 && selectedColumnsToAdd.length === 0) ||
                  (currentStep === 0 && availableFiles.length === 0)
                }
                icon={currentStep === 2 && isLargeFile ? <DownloadOutlined /> : undefined}
              >
                {currentStep === 2 
                  ? (isLargeFile ? '📥 Procesar y Descargar' : '🚀 Ejecutar Cruce')
                  : 'Siguiente →'
                }
              </Button>
            )}

            <Button onClick={resetWizard} disabled={loading}>
              🔄 {currentStep === 3 ? 'Nuevo Cruce' : 'Reiniciar'}
            </Button>
          </Space>
        </div>
      </div>
    </Spin>
  );
};

export default FileCrossManager;
