// components/cross/FileCrossManager.tsx
import React, { useState } from 'react';
import {Card,Typography,Row,Col,Form,Select,Button,Checkbox,Steps,Alert,Space,Spin,Divider,message,Tag } from 'antd';
import {SwapOutlined,CheckCircleOutlined,DownloadOutlined,PlayCircleOutlined,ArrowLeftOutlined } from '@ant-design/icons';
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
  const [form] = Form.useForm();

  // Estados para configuración
  const [file1, setFile1] = useState<FileInfo | null>(null);
  const [file2, setFile2] = useState<FileInfo | null>(null);
  const [file1Columns, setFile1Columns] = useState<string[]>([]);
  const [file2Columns, setFile2Columns] = useState<string[]>([]);
  const [selectedColumnsToAdd, setSelectedColumnsToAdd] = useState<string[]>([]);

  const [crossResult, setCrossResult] = useState<any>(null);

  // ✅ STEPS ACTUALIZADOS
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
      icon: <PlayCircleOutlined />,
    },
  ];

  // Cargar columnas cuando se selecciona un archivo
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

  // FUNCIÓN DE CRUCE 
  const handleExecuteCross = async (allValues: any) => {
    try {
      setLoading(true);

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

      console.log('🚀 REQUEST FINAL PARA CRUCE:', JSON.stringify(request, null, 2));

      const result = await CrossService.crossFiles(request);
      console.log('✅ Final result:', result);

      // ✅ GUARDAR RESULTADO LOCALMENTE
      setCrossResult(result);

      // ✅ NOTIFICAR AL PADRE SI EXISTE CALLBACK
      if (onCrossComplete) {
        onCrossComplete(result);
      }

      message.success(`Cruce completado: ${result.total_rows} registros procesados`);

    } catch (error: any) {
      console.error('❌ Error en cruce final:', error);
      message.error(error.response?.data?.detail || 'Error al ejecutar el cruce');
    } finally {
      setLoading(false);
    }
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
    form.resetFields();
  };

  const renderStepContent = () => {
    switch (currentStep) {
      case 0:
        return (
          <Row gutter={[16, 16]}>
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
                  >
                    {availableFiles.map((file) => (
                      <Option key={file.file_id} value={file.file_id}>
                        {file.original_name} ({file.total_rows?.toLocaleString()} filas)
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
                    description={`${file1.total_rows?.toLocaleString()} filas, ${file1.columns?.length} columnas`}
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
                  >
                    {availableFiles.map((file) => (
                      <Option key={file.file_id} value={file.file_id}>
                        {file.original_name} ({file.total_rows?.toLocaleString()} filas)
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
                    description={`${file2.total_rows?.toLocaleString()} filas, ${file2.columns?.length} columnas`}
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
              <>
                <Alert
                  message="🎉 Cruce Completado Exitosamente"
                  description={`Se procesaron ${crossResult.total_rows?.toLocaleString()} registros con ${crossResult.columns?.length} columnas.`}
                  type="success"
                  style={{ marginBottom: 16 }}
                />               
              </>
            )}
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <Spin spinning={loading} tip="Procesando cruce...">
      <div style={{ padding: '24px' }}>
        <Title level={4} style={{ marginBottom: 24 }}>
          🔄 Cruzar Archivos
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
                disabled={currentStep === 2 && selectedColumnsToAdd.length === 0}
              >
                {currentStep === 2 ? '🚀 Ejecutar Cruce' : 'Siguiente →'}
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
