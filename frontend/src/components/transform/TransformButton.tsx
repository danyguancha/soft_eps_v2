import React from 'react';
import { Button } from 'antd';

interface Props {
  opKey: string;           
  label: string;           
  block?: boolean;
  size?: 'large' | 'middle' | 'small';
  onSelect: (op: string) => void;
}

export const TransformButton: React.FC<Props> = ({
  opKey,
  label,
  block = true,
  size = 'middle',
  onSelect,
}) => (
  <Button
    block={block}
    size={size}
    onClick={() => onSelect(opKey)}
  >
    {label}
  </Button>
);
