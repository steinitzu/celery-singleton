import pytest
import time
from contextlib import contextmanager
from uuid import uuid4

import celery
from celery_singleton.singleton import Singleton
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


def clear_locks(app):
    config = Config(app)
    backend = get_backend(config)
    backend.clear(config.key_prefix)


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

            lock = simple_task.generate_lock("simple_task", task_args=[1, 2, 3])
            simple_task.aquire_lock(lock, "test_task_id")

            task_id = simple_task.get_existing_task_id(lock)

            assert task_id == "test_task_id"
            
