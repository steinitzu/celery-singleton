import json
from hashlib import md5

from celery import Task as BaseTask
from kombu.utils.uuid import uuid


def clear_locks(app):
    rc = app.backend.client
    rc.delete(*rc.keys('SINGLETONLOCK_*'))


class Singleton(BaseTask):
    abstract = True

    def aquire_lock(self, lock, task_id):
        """
        """
        app = self._get_app()
        return app.backend.client.setnx(lock, task_id)

    def get_existing_task_id(self, lock):
        app = self._get_app()
        return app.backend.client.get(lock).decode()

    def generate_lock(self, task_name, *args, **kwargs):
        task_args = json.dumps(args, sort_keys=True)
        task_kwargs = json.dumps(kwargs, sort_keys=True)
        return 'SINGLETONLOCK_'+md5(
            (task_name+task_args+task_kwargs).encode()
        ).hexdigest()

    def lock_and_run(self, lock, task_id, *args, **kwargs):
        lock_aquired = self.aquire_lock(lock, task_id)
        if lock_aquired:
            try:
                return self.apply_async(
                    args=args,
                    kwargs=kwargs,
                    task_id=task_id
                )
            except:
                # Clear the lock if apply_async fails
                self.release_lock(*args, **kwargs)
                raise

    def delay(self, *args, **kwargs):
        task_id = uuid()
        lock = self.generate_lock(self.name, *args, **kwargs)

        task = self.lock_and_run(lock, task_id, *args, **kwargs)
        if task:
            return task

        existing_task_id = self.get_existing_task_id(lock)
        while not existing_task_id:
            task = self.lock_and_run(lock, task_id, *args, **kwargs)
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
