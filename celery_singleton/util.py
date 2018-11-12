import json
from hashlib import md5


def generate_lock(
    task_name, task_args=None, task_kwargs=None, key_prefix="SINGLETONLOCK_"
):
    str_args = json.dumps(task_args or [], sort_keys=True)
    str_kwargs = json.dumps(task_kwargs or {}, sort_keys=True)
    task_hash = md5((task_name + str_args + str_kwargs).encode()).hexdigest()
    key_prefix = key_prefix
    return key_prefix + task_hash
