// types/fileTypes.tsx
export interface AgeGroupIcon {
  key: string;
  displayName: string;
  icon: React.ReactNode;
  color: string;
  description: string;
  filename?: string;
  isCustomFile?: boolean;
  fileSize?: number;
  uploadedAt?: Date;
}

export interface CustomUploadedFile {
  uid: string;
  name: string;
  filename: string;
  uploadedAt: Date;
  size: number;
  fileId: string;
}
