// src/Api.ts
import axios, { AxiosError, type InternalAxiosRequestConfig } from 'axios';

const api = axios.create({
  baseURL:'http://172.17.17.199:8000/api/v1',
  //baseURL:'https://soft-eps-v2.onrender.com/api/v1',
  timeout: 300000,
  headers: {
    'Content-Type': 'application/json',
  }
});

// ========== REQUEST INTERCEPTOR ==========
api.interceptors.request.use(
  (config) => {
    const method = config.method?.toUpperCase();
    const url = config.url;
    console.log(`API Request: ${method} ${url}`);
    return config;
  },
  (error) => {
    console.error('Request error:', error);
    return Promise.reject(error);
  }
);

// ========== RESPONSE INTERCEPTOR CON RETRY ==========
api.interceptors.response.use(
  (response) => {
    console.log(`API Response: ${response.status} ${response.config.url}`);
    return response;
  },
  async (error: AxiosError) => {
    const originalRequest = error.config as InternalAxiosRequestConfig & { _retry?: boolean };
    
    console.error('Response error:', error.response?.data || error.message);
    
    // ========== MANEJO DE ERROR 503 (Servidor reiniciándose) ==========
    if (error.response?.status === 503 && originalRequest && !originalRequest._retry) {
      originalRequest._retry = true;
      
      console.log('🔄 Servidor reiniciándose (503), esperando 5s antes de reintentar...');
      await new Promise(resolve => setTimeout(resolve, 5000));
      console.log('🔄 Reintentando petición...');
      
      return api.request(originalRequest);
    }
    
    // ========== MANEJO DE ERROR DE RED (ECONNREFUSED, etc) ==========
    if (error.code === 'ECONNABORTED' || error.code === 'ERR_NETWORK' || !error.response) {
      console.log('⚠️ Error de conexión, servidor posiblemente reiniciándose...');
      
      if (originalRequest && !originalRequest._retry) {
        originalRequest._retry = true;
        await new Promise(resolve => setTimeout(resolve, 3000));
        
        try {
          return await api.request(originalRequest);
        } catch (err) {
          return Promise.reject(err);
        }
      }
    }
    
    // ========== LOGGING DE ERRORES (PERO NO TRANSFORMAR) ==========
    if (error.response) {
      const status = error.response.status;
      const data = error.response.data as any;
      
      // Solo hacer logs, NO modificar el error
      if (status === 400) {
        console.error(`Bad Request (400):`, data?.detail || data?.message);
      } else if (status === 401) {
        console.error('No autorizado (401)');
      } else if (status === 403) {
        console.error('Acceso prohibido (403)');
      } else if (status === 404) {
        console.error('Recurso no encontrado (404)');
      } else if (status === 413) {
        console.error('Archivo demasiado grande (413)');
      } else if (status === 422) {
        console.error(`Error de validación (422):`, data?.detail);
      } else if (status >= 500) {
        console.error(`Error del servidor (${status}):`, data?.detail || data?.message);
      }
    } else if (error.code === 'ECONNABORTED') {
      console.error('Timeout: La operación tomó demasiado tiempo');
    } else if (error.code === 'ERR_NETWORK' || !error.response) {
      console.error('Error de red: No se pudo conectar con el servidor');
    }
    return Promise.reject(error);
  }
);

// ========== MÉTODO AUXILIAR PARA ACTUALIZAR BASE URL ==========
export const updateBaseURL = (newBaseURL: string) => {
  api.defaults.baseURL = newBaseURL;
  console.log(`📡 API baseURL actualizada: ${newBaseURL}`);
};

// ========== MÉTODO AUXILIAR PARA OBTENER BASE URL ACTUAL ==========
export const getCurrentBaseURL = (): string => {
  return api.defaults.baseURL || 'http://localhost:8000/api/v1';
};

export default api;
