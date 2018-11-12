CELERY_SINGLETON_BACKEND_URL="redis://localhost"
# Auto detected based on url for built in backends
CEELRY_SINGLETON_BACKEND_CLASS="celery_singleton.backend.RedisBackend"
CELERY_SINGLETON_KEY_PREFIX="SINGLETONLOCK_"
