import time

import celery
import pytest

from celery_singleton import Singleton


@celery.shared_task(base=Singleton)
def takes_a_sec(*args, **kwargs):
    time.sleep(0.5)
    return args, kwargs


@celery.shared_task(base=Singleton)
def throws_an_error(*args, **kwargs):
    time.sleep(0.5)
    raise ValueError('The celery worker prints a stacktrace here, this is normal')


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': 'redis://localhost:6379',
        'result_backend': 'redis://localhost:6379'
    }


def test_try_duplicate(celery_session_worker):
    """
    Spawn two identical tasks within the first task's runtime.
    Result is that the second task returns the first task's instance.
    """
    task1 = takes_a_sec.delay(1, 2, 3, key=4)
    task2 = takes_a_sec.delay(1, 2, 3, key=4)

    assert task1.id == task2.id
    assert task1.get() == task2.get()


def test_two_uniques(celery_session_worker):
    """
    Queue two unique tasks.
    The result is the same as a normal celery task delay()
    """
    task1 = takes_a_sec.delay(1, 2, 3, key=4)
    task2 = takes_a_sec.delay(3, 2, 1, key=4)

    assert task1.id != task2.id
    assert task1.get() != task2.get()


def test_duplicate_after_run(celery_session_worker):
    """
    Run a task and then another identical task after the first
    one has finished.
    The result is the same as a normal celery task delay()
    """
    task1 = takes_a_sec.delay(1, 2, 3, key=4)
    id1 = task1.id
    result1 = task1.get()

    task2 = takes_a_sec.delay(1, 2, 3, key=4)
    id2 = task2.id
    result2 = task2.get()

    assert id1 != id2
    assert result1 == result2


def test_lock_cleared_on_failure(celery_session_worker):
    """
    Run a task that throws an error.
    After the task runs its lock is cleared and
    a another identical task can be spawned.
    """
    task1 = throws_an_error.delay(1, 2, 3)
    id1 = task1.id
    try:
        task1.get()
    except Exception:
        pass

    task2 = throws_an_error.delay(1, 2, 3)
    id2 = task2.id
    try:
        task2.get()
    except Exception:
        pass
    assert id1 != id2


def test_no_kwargs(celery_session_worker):
    task = takes_a_sec.apply_async(args=(1, 2, 3))
    result = task.get()
    assert result == [[1, 2, 3], {}]


def test_no_args(celery_session_worker):
    task = takes_a_sec.apply_async()
    result = task.get()
    assert result == [[], {}]


def test_get_existing_task_id(celery_session_worker):
    lock = takes_a_sec.generate_lock(takes_a_sec.name, 1, 2, 3)
    takes_a_sec.aquire_lock(lock, 'testing_task_id')

    task_id = takes_a_sec.get_existing_task_id(lock)
    takes_a_sec.release_lock(1, 2, 3)

    assert task_id == 'testing_task_id'
    assert takes_a_sec.get_existing_task_id(lock) is None
