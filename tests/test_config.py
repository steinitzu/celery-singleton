import pytest
import sys

from celery_singleton.config import Config
from celery_singleton.backends.redis import RedisBackend


class TestBackendUrl:
    @pytest.mark.celery(result_backend="redis://test_backend_url")
    def test__defaults_to_celery_backend_url(self, celery_app):
        config = Config(celery_app)
        assert config.backend_url == "redis://test_backend_url"

    @pytest.mark.celery(broker_url="redis://test_broker_url")
    def test__defaults_to_celery_broker_url(self, celery_app):
        config = Config(celery_app)
        assert config.backend_url == "redis://test_broker_url"

    @pytest.mark.celery(singleton_backend_url="redis://override_url")
    def test__override_url(self, celery_app):
        config = Config(celery_app)
        assert config.backend_url == "redis://override_url"


class FakeBackendModule:
    class FakeBackend:
        pass


class TestBackend:
    def test__default_backend_class__redis_backend(self, celery_app):
        config = Config(celery_app)
        assert config.backend_class == RedisBackend

    @pytest.mark.celery(
        singleton_backend_class="singleton_backends.fake.FakeBackend"
    )
    def test__override_from_string__returns_class(
        self, celery_app, monkeypatch
    ):
        with monkeypatch.context() as monkey:
            monkey.setitem(
                sys.modules, "singleton_backends.fake", FakeBackendModule
            )
            config = Config(celery_app)
            assert config.backend_class == FakeBackendModule.FakeBackend

    @pytest.mark.celery(singleton_backend_class=FakeBackendModule.FakeBackend)
    def test__override_from_class__returns_class(self, celery_app):
        config = Config(celery_app)
        assert config.backend_class == FakeBackendModule.FakeBackend


class TestKeyPrefix:
    def test__default_key_prefix(self, celery_app):
        config = Config(celery_app)
        assert config.key_prefix == "SINGLETONLOCK_"

    @pytest.mark.celery(singleton_key_prefix="CUSTOM_KEY_PREFIX")
    def test__override_key_prefix(self, celery_app):
        config = Config(celery_app)
        assert config.key_prefix == "CUSTOM_KEY_PREFIX"
