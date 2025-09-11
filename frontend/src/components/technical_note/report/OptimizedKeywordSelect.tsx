// components/report/OptimizedKeywordSelect.tsx
import React, { memo, useCallback } from 'react';
import { Select } from 'antd';
import { SELECT_OPTIONS } from '../../../config/reportKeywords.config';

// Función debounce
function debounce<T extends (...args: any[]) => any>(
  func: T,
  wait: number
): (...args: Parameters<T>) => void {
  let timeout: NodeJS.Timeout;
  return (...args: Parameters<T>) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

interface OptimizedKeywordSelectProps {
  value: string[];
  onChange: (values: string[]) => void;
  placeholder?: string;
  disabled?: boolean;
}

export const OptimizedKeywordSelect = memo<OptimizedKeywordSelectProps>(({ 
  value, 
  onChange, 
  placeholder, 
  disabled 
}) => {
  const debouncedOnChange = useCallback(
    debounce((values: string[]) => {
      onChange(values);
    }, 100),
    [onChange]
  );

  const handleChange = useCallback((values: string[]) => {
    if (JSON.stringify(values.sort()) !== JSON.stringify(value.sort())) {
      debouncedOnChange(values);
    }
  }, [debouncedOnChange, value]);

  return (
    <Select
      mode="multiple"
      value={value}
      onChange={handleChange}
      placeholder={placeholder}
      disabled={disabled}
      style={{ width: '100%' }}
      options={SELECT_OPTIONS}
      showSearch={true}
      allowClear={false}
      virtual={true}
      maxTagCount={3}
      maxTagTextLength={18}
      optionFilterProp="label"
      popupMatchSelectWidth={false}
      styles={{
        popup: {
          root: {
            minWidth: '280px',
            maxHeight: '240px',
            overflowY: 'auto'
          }
        }
      }}
      classNames={{
        popup: {
          root: 'keywords-select-dropdown'
        }
      }}
      listHeight={240}
      getPopupContainer={(triggerNode) => triggerNode.parentElement || document.body}
    />
  );
});

OptimizedKeywordSelect.displayName = 'OptimizedKeywordSelect';
