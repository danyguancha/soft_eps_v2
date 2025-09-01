# middleware/content_size_limit.py
from typing import Optional
from fastapi import HTTPException

class ContentSizeLimitMiddleware:
    """
    Middleware ASGI para limitar el tamaño del contenido de las peticiones.
    Rechaza requests que excedan el límite antes de cargar todo en memoria.
    """

    def __init__(
        self,
        app,
        max_content_size: Optional[int] = None,
    ):
        self.app = app
        self.max_content_size = max_content_size

    def receive_wrapper(self, receive):
        """Wrapper que intercepta y cuenta los bytes recibidos"""
        received = 0

        async def inner():
            nonlocal received
            message = await receive()
            
            # Solo aplicar límite a requests HTTP con body
            if message["type"] != "http.request" or self.max_content_size is None:
                return message
            
            body_len = len(message.get("body", b""))
            received += body_len

            # Rechazar si excede el límite
            if received > self.max_content_size:
                raise HTTPException(
                    status_code=413, 
                    detail=f"Request body too large. Maximum allowed: {self.max_content_size/1024/1024:.1f}MB"
                )

            return message

        return inner

    async def __call__(self, scope, receive, send) -> None:
        # Solo aplicar a peticiones HTTP
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Aplicar wrapper solo a rutas de upload para no afectar otras rutas
        path = scope.get("path", "")
        if "/upload" in path or "/api/v1/files" in path:
            wrapper = self.receive_wrapper(receive)
            await self.app(scope, wrapper, send)
        else:
            await self.app(scope, receive, send)
