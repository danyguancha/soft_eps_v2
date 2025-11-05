// components/CrossPage.tsx
import { Box } from '@mui/material';
import React, { useEffect, useState } from 'react';
import api from '../../Api';
import type { FileInfo } from '../../types/api.types';
import FileCrossManager from './FileCrossManager';

const CrossPage: React.FC = () => {
  const [files, setFiles] = useState<FileInfo[]>([]);
  const [, setLoading] = useState(false);

  const loadFiles = async () => {
    try {
      setLoading(true);
      const response = await api.get('/files');
      setFiles(response.data.files || []);
    } catch (error) {
      console.error('Error loading files:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadFiles();
  }, []);

  return (
    <Box sx={{ p: 3 }}>
      <FileCrossManager 
        availableFiles={files}
        onRefreshFiles={loadFiles} onComplete={function (): void {
          throw new Error('Function not implemented.');
        } }      />
    </Box>
  );
};

export default CrossPage;
