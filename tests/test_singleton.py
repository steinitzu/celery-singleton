import pytest
from unittest import mock
import time
from contextlib import contextmanager
from uuid import uuid4

import celery
from celery import Task as BaseTask
from celery_singleton.singleton import Singleton, clear_locks
from celery_singleton.config import Config
from celery_singleton.backends import get_backend


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
        clear_locks(celery_app)


class ExpectedTaskFail(Exception):
    pass


class TestSimpleTask:
    def test__queue_duplicates__same_id(self, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton)
            def simple_task(*args):
                return args

            tasks = [
                simple_task.apply_async(args=[1, 2, 3]) for i in range(10)
            ]
            assert set(tasks) == set([tasks[0]])

    def test__queue_multiple_uniques__different_ids(self, scoped_app):
        with scoped_app as app:

            @app.task(base=Singleton)
            def simple_task(*args):
                return args

            tasks = [
                simple_task.apply_async(args=[i, i + 1, i + 2])
                for i in range(5)
            ]
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

            lock = simple_task.generate_lock(
                "simple_task", task_args=[1, 2, 3]
            )
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
            lock = simple_task.generate_lock(
                "simple_task", task_args=task_args
            )
            try:
                simple_task.apply_async(args=task_args)
            except Exception:
                pass
            assert simple_task.get_existing_task_id(lock) is None

    @mock.patch.object(
        BaseTask,
        "apply_async",
        side_effect=ExpectedTaskFail("Apply async error"),
    )
    def test__apply_async_fails__exception_reraised(
        self, mock_base, scoped_app
    ):
        with scoped_app as app:

            @app.task(base=Singleton)
            def simple_task(*args):
                return args

            with pytest.raises(ExpectedTaskFail):
                simple_task.apply_async(args=[1, 2, 3])


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
