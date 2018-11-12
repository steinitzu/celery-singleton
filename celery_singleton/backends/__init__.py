
def get_backend(config):
    """
    :param app: celery instance
    :type app: celery.Celery
    """
    klass = config.backend_class
    kwargs = config.backend_kwargs
    url = config.backend_url
    return klass(url, **kwargs)
