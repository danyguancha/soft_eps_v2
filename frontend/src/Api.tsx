// src/Api.ts
import axios, { AxiosError, type InternalAxiosRequestConfig } from 'axios';

// Crear instancia de axios con configuración base
const api = axios.create({
  baseURL:'http://localhost:8000/api/v1',
  // baseURL: 'https://soft-eps.onrender.com/api/v1', // Producción
  timeout: 300000, // 5 minutos para archivos grandes
  headers: {
    'Content-Type': 'application/json',
  }
});

// ========== REQUEST INTERCEPTOR ==========
api.interceptors.request.use(
  (config) => {
    // Log de requests
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
    // Log de responses exitosas
    console.log(`API Response: ${response.status} ${response.config.url}`);
    return response;
  },
  async (error: AxiosError) => {
    const originalRequest = error.config as InternalAxiosRequestConfig & { _retry?: boolean };
    
    // Log del error
    console.error('Response error:', error.response?.data || error.message);
    
    // ========== MANEJO DE ERROR 503 (Servidor reiniciándose) ==========
    if (error.response?.status === 503 && originalRequest && !originalRequest._retry) {
      originalRequest._retry = true;
      
      console.log('🔄 Servidor reiniciándose (503), esperando 5s antes de reintentar...');
      
      // Esperar 5 segundos
      await new Promise(resolve => setTimeout(resolve, 5000));
      
      console.log('🔄 Reintentando petición...');
      
      // Reintentar la petición original
      return api.request(originalRequest);
    }
    
    // ========== MANEJO DE ERROR DE RED (ECONNREFUSED, etc) ==========
    if (error.code === 'ECONNABORTED' || error.code === 'ERR_NETWORK' || !error.response) {
      console.log('⚠️ Error de conexión, servidor posiblemente reiniciándose...');
      
      // Si es el primer intento, reintentar una vez después de 3s
      if (originalRequest && !originalRequest._retry) {
        originalRequest._retry = true;
        
        await new Promise(resolve => setTimeout(resolve, 3000));
        
        try {
          return await api.request(originalRequest);
        } catch (err) {
            throw Promise.reject(err);
        }
      }
    }
    
    // ========== FORMATEAR MENSAJE DE ERROR ==========
    let errorMessage = 'Error desconocido';
    if (error.response) {
      const status = error.response.status;
      const data = error.response.data as any;
      
      // Extraer mensaje de error del backend
      errorMessage = data?.detail || data?.message || error.response.statusText;
      
      // Mensajes personalizados según código de estado
      if (status === 400) {
        errorMessage = `Solicitud inválida: ${errorMessage}`;
      } else if (status === 401) {
        errorMessage = 'No autorizado. Por favor, inicia sesión.';
      } else if (status === 403) {
        errorMessage = 'Acceso prohibido.';
      } else if (status === 404) {
        errorMessage = 'Recurso no encontrado.';
      } else if (status === 413) {
        errorMessage = 'Archivo demasiado grande. Usa el modo de descarga.';
      } else if (status === 422) {
        errorMessage = `Error de validación: ${errorMessage}`;
      } else if (status === 503) {
        errorMessage = 'Servidor temporalmente no disponible. Reintentando...';
      } else if (status >= 500) {
        errorMessage = `Error del servidor (${status}): ${errorMessage}`;
      }
    } else if (error.code === 'ECONNABORTED') {
      errorMessage = 'La operación tomó demasiado tiempo. Por favor, intenta nuevamente.';
    } else if (error.code === 'ERR_NETWORK' || !error.response) {
      errorMessage = 'No se pudo conectar con el servidor. Verifica tu conexión.';
    } else {
      errorMessage = error.message || 'Error desconocido';
    }
    
    // Rechazar con el error formateado
    throw Promise.reject(new Error(errorMessage));
  }
);

// ========== MÉTODO AUXILIAR PARA ACTUALIZAR BASE URL ==========
/**
 * Actualiza la baseURL de la instancia axios (usado por HealthMonitor)
 * @param newBaseURL - Nueva URL base
 */
export const updateBaseURL = (newBaseURL: string) => {
  api.defaults.baseURL = newBaseURL;
  console.log(`📡 API baseURL actualizada: ${newBaseURL}`);
};

// ========== MÉTODO AUXILIAR PARA OBTENER BASE URL ACTUAL ==========
/**
 * Obtiene la baseURL actual
 */
export const getCurrentBaseURL = (): string => {
  return 'http://localhost:8000/api/v1';
};

export default api;
