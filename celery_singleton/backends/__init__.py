from .redis import RedisBackend
from .base import BaseBackend


_backend = None


def get_backend(config):
    """
    Get the celery-singleton backend.
    The backend instance is cached for subsequent calls.

    :param app: celery instance
    :type app: celery.Celery
    """
    global _backend
    if _backend:
        return _backend
    klass = config.backend_class
    kwargs = config.backend_kwargs
    url = config.backend_url
    _backend = klass(url, **kwargs)
    return _backend


__all__ = ["RedisBackend", "BaseBackend", "get_backend"]
