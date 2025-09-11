// components/common/CustomSelect.tsx
import React, { memo } from 'react';
import { Select } from 'antd';
import { createSelectFilterOption, getSelectPopupStyles } from '../../utils/ReportUtils';

const { Option } = Select;

interface CustomSelectProps {
  value?: string | null;
  placeholder: string;
  options: string[];
  onChange: (value: string | null) => void;
  disabled?: boolean;
  loading?: boolean;
  style?: React.CSSProperties;
}

export const CustomSelect = memo<CustomSelectProps>(({
  value,
  placeholder,
  options,
  onChange,
  disabled = false,
  loading = false,
  style = { width: '100%' }
}) => (
  <Select
    placeholder={placeholder}
    style={style}
    size="middle"
    value={value}
    onChange={onChange}
    loading={loading}
    disabled={disabled || loading}
    allowClear
    showSearch
    filterOption={createSelectFilterOption}
    styles={getSelectPopupStyles()}
  >
    {options.map(option => (
      <Option key={option} value={option}>
        {option}
      </Option>
    ))}
  </Select>
));

CustomSelect.displayName = 'CustomSelect';
