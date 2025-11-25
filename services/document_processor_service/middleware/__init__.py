"""Middleware package for document processor service."""
from .security import SecurityHeadersMiddleware

__all__ = ["SecurityHeadersMiddleware"]
