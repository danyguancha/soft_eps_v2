# services/aux_duckdb_services/registry.py
"""
Registry Pattern para evitar importaciones circulares
Permite registrar y acceder a servicios globalmente sin dependencias directas
"""

class ServiceRegistry:
    """
    Singleton Registry para servicios globales
    Evita importaciones circulares proporcionando acceso centralizado a instancias
    """
    _instance = None
    _services = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def register(self, name: str, service):
        """
        Registra un servicio en el registry
        
        Args:
            name (str): Nombre único del servicio
            service: Instancia del servicio a registrar
        """
        self._services[name] = service
        print(f"Servicio registrado: {name}")
        
    def get(self, name: str):
        """
        Obtiene un servicio registrado
        
        Args:
            name (str): Nombre del servicio
            
        Returns:
            Instancia del servicio o None si no existe
        """
        return self._services.get(name)
    
    def is_registered(self, name: str) -> bool:
        """
        Verifica si un servicio está registrado
        
        Args:
            name (str): Nombre del servicio
            
        Returns:
            bool: True si está registrado, False en caso contrario
        """
        return name in self._services
    
    def unregister(self, name: str):
        """
        Desregistra un servicio
        
        Args:
            name (str): Nombre del servicio a desregistrar
        """
        if name in self._services:
            del self._services[name]
            print(f"Servicio desregistrado: {name}")
    
    def list_services(self):
        """
        Lista todos los servicios registrados
        
        Returns:
            list: Lista de nombres de servicios registrados
        """
        return list(self._services.keys())
    
    def clear(self):
        """Limpia todos los servicios registrados"""
        self._services.clear()
        print("Registry limpiado")
    
    def wait_for_service(self, name: str, max_attempts: int = 10, delay: float = 0.1):
        """
        Espera a que un servicio esté disponible
        
        Args:
            name (str): Nombre del servicio
            max_attempts (int): Máximo número de intentos
            delay (float): Tiempo de espera entre intentos en segundos
            
        Returns:
            service instance o None si no se encuentra después de los intentos
        """
        import time
        
        for attempt in range(max_attempts):
            if self.is_registered(name):
                return self.get(name)
            time.sleep(delay)
            
        return None

# Instancia global del registry
registry = ServiceRegistry()
