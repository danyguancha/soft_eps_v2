// components/CrossPage.tsx
import React, { useState, useEffect } from 'react';
import { Box } from '@mui/material';
import FileCrossManager from './FileCrossManager';
import type { FileInfo } from '../../types/api.types';
import api from '../../Api';

const CrossPage: React.FC = () => {
  const [files, setFiles] = useState<FileInfo[]>([]);
  const [loading, setLoading] = useState(false);

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
        onRefreshFiles={loadFiles}
      />
    </Box>
  );
};

export default CrossPage;
