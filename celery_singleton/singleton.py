import json
from hashlib import md5

from celery import Task as BaseTask
from kombu.utils.uuid import uuid


def clear_locks(app):
    rc = app.backend.client
    locks = rc.keys('SINGLETONLOCK_*')
    if locks:
        rc.delete(*locks)


class Singleton(BaseTask):
    abstract = True

    def aquire_lock(self, lock, task_id):
        """
        """
        app = self._get_app()
        return app.backend.client.setnx(lock, task_id)

    def get_existing_task_id(self, lock):
        app = self._get_app()
        task_id = app.backend.client.get(lock)
        return task_id.decode() if task_id else None

    def generate_lock(self, task_name, *args, **kwargs):
        task_args = json.dumps(args, sort_keys=True)
        task_kwargs = json.dumps(kwargs, sort_keys=True)
        return 'SINGLETONLOCK_'+md5(
            (task_name+task_args+task_kwargs).encode()
        ).hexdigest()

    def lock_and_run(self, lock, args=None, kwargs=None, task_id=None,
                     producer=None, link=None, link_error=None, shadow=None,
                     **options):
        lock_aquired = self.aquire_lock(lock, task_id)
        if lock_aquired:
            try:
                return super(Singleton, self).apply_async(
                    args=args, kwargs=kwargs,
                    task_id=task_id, producer=producer,
                    link=link, link_error=link_error,
                    shadow=shadow, **options
                )
            except:
                # Clear the lock if apply_async fails
                self.release_lock(*args, **kwargs)
                raise

    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None,
                    link=None, link_error=None, shadow=None, **options):
        args = args or []
        kwargs = kwargs or {}

        task_id = task_id or uuid()
        lock = self.generate_lock(self.name, *args, **kwargs)

        task = self.lock_and_run(
            lock, args=args, kwargs=kwargs,
            task_id=task_id, producer=producer,
            link=link, link_error=link_error,
            shadow=shadow, **options
        )
        if task:
            return task

        existing_task_id = self.get_existing_task_id(lock)
        while not existing_task_id:
            task = self.lock_and_run(
                lock, args=args, kwargs=kwargs,
                task_id=task_id, producer=producer,
                link=link, link_error=link_error,
                shadow=shadow, **options
            )
            if task:
                return task
            existing_task_id = self.get_existing_task_id(lock)
        return self.AsyncResult(existing_task_id)

    def release_lock(self, *args, **kwargs):
        app = self._get_app()
        lock = self.generate_lock(self.name, *args, **kwargs)
        app.backend.delete(lock)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.release_lock(*args, **kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        self.release_lock(*args, **kwargs)
