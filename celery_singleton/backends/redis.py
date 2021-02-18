from redis import Redis
from redis.sentinel import Sentinel

import re

from .base import BaseBackend


class ParseSentinelURL(object):
    def __init__(self, sentinel_url):
        self.sentinel_url = sentinel_url
        self.default_port = 26379

    def parse(self):
        sentinel_config = list()

        for url in self.sentinel_url.split(";"):
            host = re.search(r"(?<=@)(.+?)(?=:|;|$)", url)
            host = host.group() if host else None

            port = re.search(r"(?<=:)(\d+)(?=;|$)", url)
            port = int(port.group()) if port else self.default_port
            sentinel_config.append((host, port))
        return sentinel_config


class RedisBackend(BaseBackend):
    def __init__(self, *args, **kwargs):
        """
        args and kwargs are forwarded to redis.from_url
        """

        if args[0].startswith(r"sentinel://"):
            sentinel_config = ParseSentinelURL(sentinel_url=args[0]).parse()
            broker_transport_options = kwargs['broker_transport_options']
            sentinel_kwargs = broker_transport_options.get('sentinel_kwargs')
            sentinel_password = sentinel_kwargs.get('password') if isinstance(sentinel_kwargs, dict) else None

            sentinel = Sentinel(sentinel_config,
                                sentinel_kwargs=sentinel_kwargs,
                                password=sentinel_password)
            
            self.redis = sentinel.master_for(broker_transport_options['master_name'])

        else:
            self.redis = Redis.from_url(*args, decode_responses=True, **kwargs)

    def lock(self, lock, task_id, expiry=None):
        return not not self.redis.set(lock, task_id, nx=True, ex=expiry)

    def unlock(self, lock):
        self.redis.delete(lock)

    def get(self, lock):
        return self.redis.get(lock)

    def clear(self, key_prefix):
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor=cursor, match=key_prefix + "*")
            for k in keys:
                self.redis.delete(k)
            if cursor == 0:
                break
