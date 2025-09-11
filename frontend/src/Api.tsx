import axios from 'axios';

const api = axios.create({
    baseURL: 'http://localhost:8001/api/v1',
    //baseURL: 'https://soft-eps.onrender.com/api/v1',
    timeout: 3000000,
    headers: {
        'Content-Type': 'application/json',
    }
});

// Interceptor para requests (opcional para logging)
api.interceptors.request.use(
    (config) => {
        console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`);
        return config;
    },
    (error) => {
        console.error('Request error:', error);
        return Promise.reject(error);
    }
);

// Interceptor para responses (manejo de errores centralizado)
api.interceptors.response.use(
    (response) => {
        console.log(`API Response: ${response.status} ${response.config.url}`);
        return response;
    },
    (error) => {
        console.error('Response error:', error.response?.data || error.message);
        const errorMessage = error.response?.data?.detail || error.response?.data?.message || 'Error del servidor';
        return Promise.reject(new Error(errorMessage));
    }
);

export default api;
