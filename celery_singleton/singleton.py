from celery import Task as BaseTask
from kombu.utils.uuid import uuid
import inspect

from .backends import get_backend
from .config import Config
from .exceptions import DuplicateTaskError
from . import util


def clear_locks(app):
    config = Config(app)
    backend = get_backend(config)
    backend.clear(config.key_prefix)


class Singleton(BaseTask):
    abstract = True
    _singleton_backend = None
    _singleton_config = None
    unique_on = None
    raise_on_duplicate = None
    lock_expiry = None

    @property
    def _raise_on_duplicate(self):
        if self.raise_on_duplicate is not None:
            return self.raise_on_duplicate
        return self.singleton_config.raise_on_duplicate or False

    @property
    def singleton_config(self):
        if self._singleton_config:
            return self._singleton_config
        self._singleton_config = Config(self._get_app())
        return self._singleton_config

    @property
    def singleton_backend(self):
        if self._singleton_backend:
            return self._singleton_backend
        self._singleton_backend = get_backend(self.singleton_config)
        return self._singleton_backend

    def aquire_lock(self, lock, task_id):
        expiry = (
            self.lock_expiry
            if self.lock_expiry is not None
            else self.singleton_config.lock_expiry
        )
        return self.singleton_backend.lock(lock, task_id, expiry=expiry)

    def get_existing_task_id(self, lock):
        return self.singleton_backend.get(lock)

    def generate_lock(self, task_name, task_args=None, task_kwargs=None):
        unique_on = self.unique_on
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        if unique_on:
            if isinstance(unique_on, str):
                unique_on = [unique_on]
            sig = inspect.signature(self.run)
            bound = sig.bind(*task_args, **task_kwargs).arguments

            unique_args = []
            unique_kwargs = {key: bound[key] for key in unique_on}
        else:
            unique_args = task_args
            unique_kwargs = task_kwargs
        return util.generate_lock(
            task_name,
            unique_args,
            unique_kwargs,
            key_prefix=self.singleton_config.key_prefix,
        )

    def apply_async(
        self,
        args=None,
        kwargs=None,
        task_id=None,
        producer=None,
        link=None,
        link_error=None,
        shadow=None,
        **options
    ):
        args = args or []
        kwargs = kwargs or {}
        task_id = task_id or uuid()
        lock = self.generate_lock(self.name, args, kwargs)

        run_args = dict(
            lock=lock,
            args=args,
            kwargs=kwargs,
            task_id=task_id,
            producer=producer,
            link=link,
            link_error=link_error,
            shadow=shadow,
            **options
        )

        task = self.lock_and_run(**run_args)
        if task:
            return task

        existing_task_id = self.get_existing_task_id(lock)
        while not existing_task_id:
            task = self.lock_and_run(**run_args)
            if task:
                return task
            existing_task_id = self.get_existing_task_id(lock)
        return self.on_duplicate(existing_task_id)

    def lock_and_run(self, lock, *args, task_id=None, **kwargs):
        lock_aquired = self.aquire_lock(lock, task_id)
        if lock_aquired:
            try:
                return super(Singleton, self).apply_async(
                    *args, task_id=task_id, **kwargs
                )
            except Exception:
                # Clear the lock if apply_async fails
                self.unlock(lock)
                raise

    def release_lock(self, task_args=None, task_kwargs=None):
        lock = self.generate_lock(self.name, task_args, task_kwargs)
        self.unlock(lock)

    def unlock(self, lock):
        self.singleton_backend.unlock(lock)

    def on_duplicate(self, existing_task_id):
        if self._raise_on_duplicate:
            raise DuplicateTaskError(
                "Attempted to queue a duplicate of task ID {}".format(existing_task_id),
                task_id=existing_task_id,
            )
        return self.AsyncResult(existing_task_id)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.release_lock(task_args=args, task_kwargs=kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        self.release_lock(task_args=args, task_kwargs=kwargs)
