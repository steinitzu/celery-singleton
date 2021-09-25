import pytest
from unittest import mock
import time
from contextlib import contextmanager

from celery import Celery
from celery import Task as BaseTask
from celery_singleton.singleton import Singleton, clear_locks
from celery_singleton import util, DuplicateTaskError
from celery_singleton.backends.redis import RedisBackend
from celery_singleton.backends import get_backend
from celery_singleton.config import Config


@pytest.fixture(scope="session")
def celery_config(redis_url):
    return {
        "broker_url": redis_url,
        "result_backend": redis_url,
        "singleton_key_prefix": "lock_prefix:",
    }


@pytest.fixture(scope="session")
def celery_enable_logging():
    return True


@pytest.fixture
@contextmanager
def scoped_app(celery_app):
    try:
        yield celery_app
    finally:
        backend = get_backend(Config(celery_app))
        backend.redis.flushall()


class ExpectedTaskFail(Exception):
    pass


class TestSimpleTask:
    def test__queue_duplicates__same_id(self, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton)
            def simple_task(*args):
                return args

            tasks = [simple_task.apply_async(args=[1, 2, 3]) for i in range(10)]
            assert set(tasks) == set([tasks[0]])

    def test__queue_multiple_uniques__different_ids(self, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton)
            def simple_task(*args):
                return args

            tasks = [simple_task.apply_async(args=[i, i + 1, i + 2]) for i in range(5)]
            assert len(set(tasks)) == len(tasks)

    def test__queue_duplicate_after_success__different_ids(
        self, scoped_app, celery_session_worker
    ):
        with scoped_app:

            @celery_session_worker.app.task(base=Singleton)
            def simple_task(*args):
                return args

            celery_session_worker.reload()

            task1 = simple_task.apply_async(args=[1, 2, 3])
            task1.get()
            time.sleep(0.05)  # small delay for on_success
            task2 = simple_task.apply_async(args=[1, 2, 3])
            task2.get()

            assert task1 != task2

    def test__queue_duplicate_after_error__different_ids(
        self, scoped_app, celery_session_worker
    ):
        with scoped_app:

            @celery_session_worker.app.task(base=Singleton)
            def fails(*args):
                raise ExpectedTaskFail()

            celery_session_worker.reload()

            task1 = fails.apply_async(args=[1, 2, 3])
            try:
                task1.get()
            except Exception as e:
                assert type(e).__name__ == ExpectedTaskFail.__name__
            time.sleep(0.05)  # small delay for on_success
            task2 = fails.apply_async(args=[1, 2, 3])

            assert task1 != task2

    def test__get_existing_task_id(self, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton)
            def simple_task(*args):
                return args

            lock = simple_task.generate_lock("simple_task", task_args=[1, 2, 3])
            simple_task.aquire_lock(lock, "test_task_id")

            task_id = simple_task.get_existing_task_id(lock)

            assert task_id == "test_task_id"

    @mock.patch.object(
        BaseTask, "apply_async", side_effect=Exception("Apply async error")
    )
    def test__apply_async_fails__lock_cleared(self, mock_base, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton)
            def simple_task(*args):
                return args

            task_args = [1, 2, 3]
            lock = simple_task.generate_lock("simple_task", task_args=task_args)
            try:
                simple_task.apply_async(args=task_args)
            except Exception:
                pass
            assert simple_task.get_existing_task_id(lock) is None

    @mock.patch.object(
        BaseTask, "apply_async", side_effect=ExpectedTaskFail("Apply async error")
    )
    def test__apply_async_fails__exception_reraised(self, mock_base, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton)
            def simple_task(*args):
                return args

            with pytest.raises(ExpectedTaskFail):
                simple_task.apply_async(args=[1, 2, 3])

    def test__raise_on_duplicate__raises_duplicate_error(self, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton, raise_on_duplicate=True)
            def raise_on_duplicate_task(*args):
                return args

            t1 = raise_on_duplicate_task.delay(1, 2, 3)
            with pytest.raises(DuplicateTaskError) as exinfo:
                raise_on_duplicate_task.delay(1, 2, 3)
            assert exinfo.value.task_id == t1.task_id


class TestClearLocks:
    def test__clear_locks(self, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton)
            def simple_task(*args):
                return args

            [simple_task.apply_async(args=[i]) for i in range(5)]
            clear_locks(app)

            backend = simple_task.singleton_backend
            config = simple_task.singleton_config

            assert not backend.redis.keys(config.key_prefix + "*")


class TestUniqueOn:
    @mock.patch.object(
        util, "generate_lock", autospec=True, side_effect=util.generate_lock
    )
    def test__unique_on_pos_arg__lock_on_unique_args_only(
        self, mock_gen, scoped_app, celery_session_worker
    ):
        with scoped_app:

            @celery_session_worker.app.task(base=Singleton, unique_on=["a", "c"])
            def unique_on_args_task(a, b, c, d=4):
                return a * b * c * d

            celery_session_worker.reload()  # So task is registered

            result = unique_on_args_task.delay(2, 3, 4, 5)
            result.get()
            time.sleep(0.05)  # Small delay for on_success

            expected_args = [
                [
                    (unique_on_args_task.name, [], {"a": 2, "c": 4}),
                    {"key_prefix": unique_on_args_task.singleton_config.key_prefix},
                ]
            ] * 2
            assert mock_gen.call_count == 2
            assert [list(a) for a in mock_gen.call_args_list] == expected_args

    @mock.patch.object(
        util, "generate_lock", autospec=True, side_effect=util.generate_lock
    )
    def test__unique_on_kwargs__lock_on_unique_args_only(
        self, mock_gen, scoped_app, celery_session_worker
    ):
        with scoped_app:

            @celery_session_worker.app.task(base=Singleton, unique_on=["b", "d"])
            def unique_on_kwargs_task(a, b=2, c=3, d=4):
                return a * b * c * d

            celery_session_worker.reload()  # So task is registered

            result = unique_on_kwargs_task.delay(2, b=3, c=4, d=5)

            result.get()
            time.sleep(0.05)  # Small delay for on_success

            expected_args = [
                [
                    (unique_on_kwargs_task.name, [], {"b": 3, "d": 5}),
                    {"key_prefix": unique_on_kwargs_task.singleton_config.key_prefix},
                ]
            ] * 2
            assert mock_gen.call_count == 2
            assert [list(a) for a in mock_gen.call_args_list] == expected_args

    @mock.patch.object(
        util, "generate_lock", autospec=True, side_effect=util.generate_lock
    )
    def test__unique_on_empty__lock_on_task_name_only(
        self, mock_gen, scoped_app, celery_session_worker
    ):
        with scoped_app as app:

            @celery_session_worker.app.task(base=Singleton, unique_on=[])
            def unique_on_empty_task(a, b=2, c=3, d=4):
                return a * b * c * d

            celery_session_worker.reload()  # So task is registered

            result = unique_on_empty_task.delay(2, b=3, c=4, d=5)

            result.get()
            time.sleep(0.05)  # Small delay for on_success

            expected_args = [
                [
                    (unique_on_empty_task.name, [], {}),
                    {"key_prefix": unique_on_empty_task.singleton_config.key_prefix},
                ]
            ] * 2
            assert mock_gen.call_count == 2
            assert [list(a) for a in mock_gen.call_args_list] == expected_args

    @mock.patch.object(
        util, "generate_lock", autospec=True, side_effect=util.generate_lock
    )
    def test__unique_on_is_string_convertes_to_list(
        self, mock_gen, scoped_app, celery_session_worker
    ):
        with scoped_app as app:

            @celery_session_worker.app.task(base=Singleton, unique_on="c")
            def unique_on_string_task(a, b=2, c=3, d=4):
                return a * b * c * d

            celery_session_worker.reload()  # So task is registered

            result = unique_on_string_task.delay(2, b=3, c=4, d=5)

            result.get()
            time.sleep(0.05)  # Small delay for on_success

            expected_args = [
                [
                    (unique_on_string_task.name, [], {"c": 4}),
                    {"key_prefix": unique_on_string_task.singleton_config.key_prefix},
                ]
            ] * 2
            assert mock_gen.call_count == 2
            assert [list(a) for a in mock_gen.call_args_list] == expected_args


class TestRaiseOnDuplicateConfig:
    def test__default_false(self, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton)
            def mytask():
                pass

            assert mytask._raise_on_duplicate is False

    def test__task_cfg_overrides_app_cfg(self, celery_config):
        config = dict(celery_config, singleton_raise_on_duplicate=False)

        app = Celery()
        app.config_from_object(config)

        @app.task(base=Singleton, raise_on_duplicate=True)
        def mytask():
            pass

        assert mytask._raise_on_duplicate is True
        assert mytask.singleton_config.raise_on_duplicate is False

    def test__app_cfg_used_when_task_cfg_unset(self, celery_config):
        config = dict(celery_config, singleton_raise_on_duplicate=True)

        app = Celery()
        app.config_from_object(config)

        @app.task(base=Singleton)
        def mytask():
            pass

        assert mytask._raise_on_duplicate is True


class TestLockExpiry:
    @mock.patch.object(RedisBackend, "lock", return_value=True, autospec=True)
    def test__lock_expiry__sent_to_backend(self, mock_lock, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton, lock_expiry=60)
            def simple_task(*args):
                return args

            result = simple_task.delay(1, 2, 3)

            lock = simple_task.generate_lock(simple_task.name, task_args=[1, 2, 3])

            mock_lock.assert_called_once_with(
                simple_task.singleton_backend, lock, result.task_id, expiry=60
            )
