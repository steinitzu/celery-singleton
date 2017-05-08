import time

from celery import Celery
from celery.signals import celeryd_init
from celery_singleton import Singleton
from celery_singleton.singleton import clear_locks


celery_app = Celery(
    __name__,
    broker='redis://localhost:6379',
    backend='redis://localhost:6379'
)

@celeryd_init.connect()
def clear_all_locks(**kwargs):
    clear_locks(celery_app)


@celery_app.task(bind=True, name='lazy_return', base=Singleton)
def lazy_return(self, *args, **kwargs):
    print('running task')
    time.sleep(5)
    print('returning')
    return args, kwargs


if __name__ == '__main__':
    task1 = lazy_return.delay(1, 2, 3, key='abc')
    task2 = lazy_return.delay(1, 2, 3, key='abc')
    task3 = lazy_return.delay(3, 4, 5, key='abc')

    print(task1)
    print(task2)
    print(task3)
    assert task1 == task2
    assert task1 != task3
