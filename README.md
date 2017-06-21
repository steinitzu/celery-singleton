# Celery-Singleton

Duplicate tasks clogging up your message broker? Do time based rate limits make you feel icky? Look no further!  
This is a baseclass for celery tasks that ensures only one instance of the task can be queued or running at any given time. Uses the task's name+arguments to determine uniqueness.  

## Prerequisites
celery-singleton uses the JSON representation of a task's `delay()` arguments to generate a unique lock and stores it in redis.  
So to make use of this package, make sure that:  
1. Your celery app is configured with a redis [result backend](http://docs.celeryproject.org/en/latest/getting-started/first-steps-with-celery.html#keeping-results)
2. All task's arguments and keyword arguments are JSON serializable.   

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
```

That's it! Your task is a singleton and calls to `do_stuff.delay()` will either queue a new task or return an AsyncResult for the currently queued/running instance of the task. 


## How does it work?

The `Singleton` class overrides `apply_async()` and `delay()`, so this stuff works if you spawn your task using `apply_async()` or `delay()`.

Singleton uses celery's redis backend to store locks so they work across producers and consumers as long they're all using the same backend.  

When you call `delay()` on a singleton task it attempts to aquire a lock in redis using a hash of [task_name+arguments] as a key and a new task ID as a value. `SETNX` is used for this to prevent race conditions.  
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
This can for example happen when your celery worker crashes before it can release the lock.  

A convenience method is included to clear all existing locks, you can run it on celery worker startup or any other celery signal like so:  

```python
from celery.signals import worker_ready
from celery_singleton import clear_locks
from somewhere import celery_app

@worker_ready()
def unlock_all(**kwargs):
    clear_locks(celery_app)
```

## Contribute
Please open an issue if you encounter a bug, have any questions or suggestions for improvements or run into any trouble at all using this package.  
