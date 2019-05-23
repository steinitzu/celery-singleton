import pytest
from contextlib import contextmanager

from uuid import uuid4
from hashlib import md5
from celery_singleton.backends.redis import RedisBackend
from celery_singleton.backends import get_backend
from celery_singleton import backends


def random_hash():
    return "SINGLETON_TEST_KEY_PREFIX_" + md5(uuid4().bytes).hexdigest()


def random_task_id():
    return str(uuid4())


def clear_locks(backend):
    backend.clear("SINGLETON_TEST_KEY_PREFIX_")


@pytest.fixture
@contextmanager
def backend(redis_url):
    backend = RedisBackend(redis_url)
    try:
        yield backend
    finally:
        clear_locks(backend)
        backends._backend = None


class TestLock:
    def test__new_lock__is_set(self, backend):
        with backend as b:
            lock = random_hash()
            task_id = random_task_id()

            b.lock(lock, task_id)

            assert b.redis.get(lock) == task_id

    def test__new_lock__returns_true(self, backend):
        with backend as b:
            lock = random_hash()
            task_id = random_task_id()

            assert b.lock(lock, task_id) is True

    def test__lock_exists__is_not_set(self, backend):
        with backend as b:
            lock = random_hash()
            task_id = random_task_id()

            b.lock(lock, task_id)
            task_id2 = random_task_id()

            b.lock(lock, task_id2)

            assert b.redis.get(lock) == task_id and b.redis.get(lock) != task_id2

    def test__lock_exists__returns_false(self, backend):
        with backend as b:
            lock = random_hash()
            task_id = random_task_id()

            b.lock(lock, task_id)
            task_id2 = random_task_id()

            assert b.lock(lock, task_id2) is False


class TestUnlock:
    def test__unlock__deletes_key(self, backend):
        with backend as b:
            lock = random_hash()
            task_id = random_task_id()

            b.lock(lock, task_id)
            b.unlock(lock)

            assert b.redis.get(lock) is None


class TestClear:
    def test__clear_locks__all_gone(self, backend):
        with backend as b:
            locks = [random_hash() for i in range(10)]
            values = [random_task_id() for i in range(10)]

            for lock, value in zip(locks, values):
                b.lock(lock, value)

            b.clear("SINGLETON_TEST_KEY_PREFIX_")

            for lock in locks:
                assert b.get(lock) is None


class FakeBackend:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@pytest.fixture(scope="function")
def fake_config():
    class FakeConfig:
        backend_url = "redis://localhost"
        backend_kwargs = {}
        backend_class = FakeBackend

    try:
        yield FakeConfig()
    finally:
        backends._backend = None


class TestGetBackend:
    def test__correct_class(self, fake_config):
        backend = get_backend(fake_config)
        assert isinstance(backend, fake_config.backend_class)

    def test__receives_kwargs(self, fake_config):
        kwargs = dict(a=1, b=2, c=3)
        fake_config.backend_kwargs = kwargs

        backend = get_backend(fake_config)

        assert backend.kwargs == kwargs

    def test__receives_url_as_first_arg(self, fake_config):
        fake_config.backend_url = "test backend url"

        backend = get_backend(fake_config)

        assert backend.args[0] == fake_config.backend_url

    def test__get_backend_twice_returns_same_instance(self, fake_config):
        fake_config.backend_url = "test backend url"

        backend = get_backend(fake_config)
        backend2 = get_backend(fake_config)

        assert backend is backend2
