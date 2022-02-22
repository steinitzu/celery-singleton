# Celery-Singleton

Duplicate tasks clogging up your message broker? Do time based rate limits make you feel icky? Look no further!
This is a baseclass for celery tasks that ensures only one instance of the task can be queued or running at any given time. Uses the task's name+arguments to determine uniqueness.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Celery-Singleton](#celery-singleton)
    - [Prerequisites](#prerequisites)
    - [Quick start](#quick-start)
    - [How does it work?](#how-does-it-work)
    - [Handling deadlocks](#handling-deadlocks)
    - [Backends](#backends)
    - [Task configuration](#task-configuration)
        - [unique\_on](#uniqueon)
        - [raise\_on\_duplicate](#raiseonduplicate)
    - [App Configuration](#app-configuration)
    - [Testing](#testing)
    - [Contribute](#contribute)

<!-- markdown-toc end -->


## Prerequisites
celery-singleton uses the JSON representation of a task's `delay()` or `apply_async()` arguments to generate a unique lock and stores it in redis.
By default it uses the redis server of the celery [result backend](http://docs.celeryproject.org/en/latest/getting-started/first-steps-with-celery.html#keeping-results). If you use a different/no result backend or want to use a different redis server for celery-singleton, refer the [configuration section](#app-configuration) for how to customize the redis. To use something other than redis, refer to the section on [backends](#backends)

So in gist:
1. Make sure all your tasks arguments are JSON serializable
2. Your celery app is configured with a redis result backend or you have specified another redis/compatible backend in your config

If you're already using a redis backend and a mostly default celery config, you're all set!

## Quick start
`$ pip install celery-singleton`

```python
import time
from celery_singleton import Singleton
from somewhere import celery_app

@celery_app.task(base=Singleton)
def do_stuff(*args, **kwargs):
	time.sleep(4)
	return 'I just woke up'

# run the task as normal
async_result = do_stuff.delay(1, 2, 3, a='b')
async_result2 = do_stuff.delay(1, 2, 3, a='b')

assert async_result == async_result2  # These are the same, task is only queued once
```

That's it! Your task is a singleton and calls to `do_stuff.delay()` will either queue a new task or return an AsyncResult for the currently queued/running instance of the task.


## How does it work?

The `Singleton` class overrides `apply_async()` of the base task implementation to only queue a task if an identical task is not already running. Two tasks are considered identical if both have the same name and same arguments.

This is achieved by using redis for distributed locking.

When you call `delay()` or `apply_async()` on a singleton task it first attempts to aquire a lock in redis using a hash of [task_name+arguments] as a key and a new task ID as a value. `SETNX` is used for this to prevent race conditions.
If a lock is successfully aquired, the task is queued as normal with the `apply_async` method of the base class.
If another run of the task already holds a lock, we fetch its task ID instead and return an `AsyncResult` for it. This way it works seamlessly with a standard celery setup, there are no "duplicate exceptions" you need to handle, no timeouts. `delay()` always returns an `AsyncResult` as expected, either for the task you just spawned or for the task that aquired the lock before it.
So continuing on with the "Quick start" example:

```python
a = do_stuff.delay(1, 2, 3)
b = do_stuff.delay(1, 2, 3)

assert a == b  # Both are AsyncResult for the same task

c = do_stuff.delay(4, 5, 6)

assert a != c  # c has different arguments so it spawns a new task
```

The lock is released only when the task has finished running, using either the `on_success` or `on_failure` handler, after which you're free to start another identical run.

```python
# wait for a to finish
a.get()

# Now we can spawn a duplicate of it
d = do_stuff.delay(1, 2, 3)

assert a != d
```


## Handling deadlocks
Since the task locks are only released when the task is actually finished running (on success or on failure), you can sometimes end up in a situation where the lock remains but there's no task available to release it.
This can for example happen if your celery worker crashes before it can release the lock.

A convenience method is included to clear all existing locks, you can run it on celery worker startup or any other celery signal like so:

```python
from celery.signals import worker_ready
from celery_singleton import clear_locks
from somewhere import celery_app

@worker_ready.connect
def unlock_all(**kwargs):
    clear_locks(celery_app)
```

An alternative is to set a [lock expiry](#lock\_expiry) time in the task or app config. This makes it so that locks are always released after a given time.

## Backends

Redis is the default storage backend for celery singleton. This is where task locks are stored where they can be accessed across celery workers.
A custom redis url can be set using the `singleton_backend_url` config variable in the celery config. By default Celery Singleton attempts to use the redis url of the celery result backend and if that fails the celery broker.

If you don't want to use redis you can implement a custom storage backend.
An abstract base class to inherit from is included in `celery_singleton.backends.BaseBackend` and [the source code of `RedisBackend`](celery_singleton/backends/redis.py) serves as an example implementation.
Once you have your backend implemented, set the `singleton_backend_class` [configuration](#app-configuration) variables to point to your class.


## Task configuration

### unique\_on

This can be used to make celery-singleton only consider certain arguments when deciding whether two tasks are identical.
(By default, two tasks are considered identical to each other if their name and all arguments are the same).

For example, this task allows only one instance per username, other arguments don't matter:

```python
@app.task(base=Singleton, unique_on=['username', ])
def do_something(username, otherarg=None):
    time.sleep(5)


task1 = do_something.delay(username='bob', otherarg=99)
task2 = do_something.delay(username='bob', otherarg=100)  # this is a duplicate of task1
assert task1 == task2
```

Specify an empty list to consider the task name only.

### raise\_on\_duplicate

When this option is enabled the task's `delay` and `apply_async` method will raise a `DuplicateTaskError` exception when attempting to spawn a duplicate task instead of returning the existing task's `AsyncResult`
This is useful when you want only one of a particular task at a time, but want more control over what happens on duplicate attempts.

```python
from celery_singleton import Singleton, DuplicateTaskError


@app.task(base=Singleton, raise_on_duplicate=True)
def do_something(username):
    time.sleep(5)

task1 = do_something.delay('bob')
try:
    task2 = do_something.delay('bob')
except DuplicateTaskerror as e:
    print("You tried to create a duplicate of task with ID", e.task_id)
```

This option can also be applied globally to all `Singleton` tasks by setting `singleton_raise_on_duplicate` in the [app config](#app-configuration). The task level option always overrides the app config when supplied.

### lock\_expiry

Number of seconds until the task lock expires. This is useful when you want a max of one task queued within a given time frame rather than strictly one at a time.
This also adds some safety to your application as it guarantees that locks will eventually be released in case of worker crashes and network failures. For this use case it's recommended to set the lock expiry to a value slightly longer than the expected task duration.

Example

```python
@app.task(base=Singleton, lock_expiry=10)
def runs_for_12_seconds():
    self.time.sleep(12)


task1 = runs_for_12_seconds.delay()
time.sleep(11)
task2 = runs_for_12_seconds.delay()

assert task1 != task2  # These are two separate task instances
```

This option can be applied globally in the [app config](#app-configuration) with `singleton_lock_expiry`. Task option supersedes the app config.


## App Configuration

Celery singleton supports the following configuration option. These should be added to your Celery app config.
Note: if using old style celery config with uppercase variables and a namespace, make sure the singleton config matches. E.g. `CELERY_SINGLETON_BACKEND_URL` instead of `singleton_backend_url`


| Key                            | Default                                 | Description                                                                                                                                                          |
|--------------------------------|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `singleton_backend_url`        | `celery_backend_url`                    | The URL of the storage backend. If using the default backend implementation, this should be a redis URL. It is passed as the first argument to the backend class.    |
| `singleton_backend_class`      | `celery_singleton.backend.RedisBackend` | The fulll import path of a backend class as string or a reference to the class                                                                                       |
| `singleton_backend_kwargs`     | `{}`                                    | Passed as keyword arguments to the backend class                                                                                                                     |
| `singleton_key_prefix`         | `SINGLETONLOCK_`                        | Locks are stored as `<key_prefix><lock>`. Use to prevent collisions with other keys in your database.                                                                |
| `singleton_raise_on_duplicate` | `False`                                 | When `True` an attempt to queue a duplicate task will raise a `DuplicateTaskerror`. The default behavior is to return the `AsyncResult` for the existing task.       |
| `singleton_lock_expiry`        | `None` (Never expires)                  | Lock expiry time in second for singleton task locks. When lock expires identical tasks are allowed to run regardless of whether the locked task has finished or not. |
|                                |                                         |                                                                                                                                                                      |


## Testing

Tests are located in the `/tests` directory can be run with pytest

```
pip install -r dev-requirements.txt
python -m pytest
```

Some of the tests require a running redis server on `redis://localhost`
To use a redis server on a different url/host, set the env variable `CELERY_SINGLETON_TEST_REDIS_URL`


## Contribute
Please open an issue if you encounter a bug, have any questions or suggestions for improvements or run into any trouble at all using this package.
