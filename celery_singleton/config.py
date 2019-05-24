from importlib import import_module


class Config:
    def __init__(self, app):
        self.app = app

    @property
    def key_prefix(self):
        return self.app.conf.get("singleton_key_prefix", "SINGLETONLOCK_")

    @property
    def backend_class(self):
        path_or_class = self.app.conf.get(
            "singleton_backend_class", "celery_singleton.backends.redis.RedisBackend"
        )
        if isinstance(path_or_class, str):
            path = path_or_class.split(".")
            mod_name, class_name = ".".join(path[:-1]), path[-1]
            mod = import_module(mod_name)
            return getattr(mod, class_name)
        return path_or_class

    @property
    def backend_kwargs(self):
        return self.app.conf.get("singleton_backend_kwargs", {})

    @property
    def backend_url(self):
        url = self.app.conf.get("singleton_backend_url")
        if url is not None:
            return url
        url = self.app.conf.get("result_backend")
        if not url or not url.startswith("redis://"):
            url = self.app.conf.get("broker_url")
        return url

    @property
    def raise_on_duplicate(self):
        return self.app.conf.get("singleton_raise_on_duplicate")

    @property
    def lock_expiry(self):
        return self.app.conf.get("singleton_lock_expiry")
