// components/report/OptimizedKeywordSelect.tsx
import { memo, useMemo } from 'react';
import { Select, Tag } from 'antd';
import type { SelectProps } from 'antd';

interface OptimizedKeywordSelectProps {
  value: string[];
  onChange: (keywords: string[]) => void;
  placeholder?: string;
  disabled?: boolean;
}

const AVAILABLE_KEYWORDS = [
  { value: 'medicina', label: 'Medicina', color: '#1890ff' },
  { value: 'enfermeria', label: 'Enfermería', color: '#52c41a' },
  { value: 'odontológica', label: 'Odontología', color: '#722ed1' },
  { value: 'fluor', label: 'Flúor', color: '#13c2c2' },
  { value: 'placa', label: 'Placa', color: '#eb2f96' },
  { value: 'detartraje', label: 'Detartraje', color: '#fa8c16' },
  { value: 'sellantes', label: 'Sellantes', color: '#faad14' },
  { value: 'micronutrientes', label: 'Micronutrientes', color: '#a0d911' },
  { value: 'vitamina_a', label: 'Vitamina A', color: '#f5222d' },
  { value: 'sulfato_ferroso', label: 'Sulfato Ferroso', color: '#fa541c' },
  { value: 'diu', label: 'DIU', color: '#2f54eb' },
  { value: 'subdermico', label: 'Subdérmico', color: '#722ed1' },
  { value: 'asesoria_pre_test_vih', label: 'Asesoría Pre Test VIH', color: '#0777bcff' },
  { value: 'asesoria_post_test_vih', label: 'Asesoría Post Test VIH', color: '#fa541cff' },
  { value: 'tamizaje_vih', label: 'Prueba Rápida VIH', color: '#d46b08ff' },
  { value: 'lactancia_materna', label: 'lactancia materna', color: 'rgb(200, 148, 3)' },
  { value: 'hepatitis_b', label: 'hepatitis_b', color: '#fa541cff' },
  { value: 'hepatitis_c', label: 'hepatitis_c', color: '#b0c604ff' },
  { value: 'hematocrito', label: 'Hematocrito', color: 'rgb(4, 198, 33)' },
  { value: 'hemoglobina', label: 'Hemoglobina', color: 'rgb(4, 23, 198)' },
  { value: 'educacion_individual', label: 'Educación Individual', color: '#722ed1' },
  { value: 'desparasitacion', label: 'Desparacitación', color: '#fa8c16' },
  { value: 'citologia', label: 'Citología', color: '#faad14' },
  { value: 'adn-vph', label: 'ADN-VPH', color: '#a0d911' },
  { value: 'suministro_preservativos', label: 'Suministro de Preservativos', color: 'rgb(106, 100, 90)' },
  { value: 'anticonceptivo_oral', label: 'Anticonceptivo Oral', color: '#722ed1' },
  { value: 'anticonceptivo_inyectable_mensual', label: 'Anticonceptivo Inyectable Mensual', color: '#fa8c16' },
  { value: 'anticonceptivo_inyectable_trimestral', label: 'Anticonceptivo Inyectable Trimestral', color: '#faad14' },
  { value: 'sifilis', label: 'Sífilis', color: '#a0d911' },
  { value: 'cancer_mamografia', label: 'Cáncer de Mama - Mamografía', color: '#d46b08ff' },
  { value: 'valoracion_clinica_mama', label: 'Valoración Clínica Mama', color: '#fa541c' },
  { value: 'antigeno_prostatico', label: 'Antígeno Prostático (PSA)', color: 'rgb(4, 180, 157)' },
  { value: 'cancer_colon_sangre_oculta', label: 'Tamizaje para cáncer de colon (sangre oculta)', color: '#fa1ce7' },
  { value: 'prueba_embarazo', label: 'Prueba de Embarazo', color: '#722ed1' },

];

export const OptimizedKeywordSelect = memo<OptimizedKeywordSelectProps>(({
  value,
  onChange,
  placeholder = 'Seleccionar palabras clave',
  disabled = false
}) => {
  // Memoizar opciones para evitar recrearlas en cada render
  const options = useMemo(() => 
    AVAILABLE_KEYWORDS.map(kw => ({
      value: kw.value,
      label: kw.label,
    })),
    []
  );

  // Memoizar color map
  const colorMap = useMemo(() => {
    const map = new Map<string, string>();
    AVAILABLE_KEYWORDS.forEach(kw => {
      map.set(kw.value, kw.color);
    });
    return map;
  }, []);

  // Renderizar tag personalizado
  const tagRender: SelectProps['tagRender'] = (props) => {
    const { label, value: tagValue, closable, onClose } = props;
    const color = colorMap.get(tagValue as string) || '#1890ff';

    return (
      <Tag
        color={color}
        closable={closable}
        onClose={onClose}
        style={{
          marginRight: 4,
          marginBottom: 2,
          fontSize: '12px',
          padding: '2px 8px',
          borderRadius: '4px',
          display: 'inline-flex',
          alignItems: 'center',
          maxWidth: '100%',
        }}
      >
        {label}
      </Tag>
    );
  };

  return (
    <Select
      mode="multiple"
      value={value}
      onChange={onChange}
      options={options}
      placeholder={placeholder}
      disabled={disabled}
      tagRender={tagRender}
      style={{ width: '100%' }}
      size="middle"
      showSearch
      filterOption={(input, option) =>
        (option?.label?.toString().toLowerCase() ?? '').includes(input.toLowerCase())
      }
      // 🔥 CONFIGURACIÓN CLAVE: usar 'responsive' en lugar de número fijo
      maxTagCount="responsive"
      maxTagPlaceholder={(omittedValues) => (
        <Tag
          color="#d9d9d9"
          style={{
            fontSize: '12px',
            padding: '2px 8px',
            borderRadius: '4px',
            cursor: 'pointer',
            fontWeight: 500,
          }}
        >
          +{omittedValues.length}
        </Tag>
      )}
      // Mejorar el dropdown
      dropdownStyle={{ maxHeight: 400, overflow: 'auto' }}
      optionFilterProp="label"
      allowClear
      maxTagTextLength={20}
    />
  );
});

OptimizedKeywordSelect.displayName = 'OptimizedKeywordSelect';
