from celery import Celery

class CeleryTask:
    def __init__(self, id, name, args, kwargs):
        self.id = id
        self.name = name
        self.args = args
        self.kwargs = kwargs

def get_task_by_id(app, id):
    inspector = app.control.inspect()
    try:
        active_task_found = _get_task(id, inspector.active())
        if active_task_found:
            return active_task_found
        reserved_task_found = _get_task(id, inspector.reserved())
        if reserved_task_found:
            return reserved_task_found
        scheduled_task_found = _get_scheduled_task(id, inspector.scheduled())
        if scheduled_task_found:
            return scheduled_task_found
    except Exception:
        pass
    return None

def _get_task(id, active_task_dict):
    for worker, task_list in active_task_dict.items():
            for task in task_list:
                if task['id'] == id:
                    return CeleryTask(task['id'], task["name"], task["args"], task["kwargs"])
    return None

def _get_scheduled_task(id, scheduled_task_dict):
    for worker, task_scheduled_list in scheduled_task_dict.items():
            for scheduled in task_scheduled_list:
                task = scheduled['request']
                if task['id'] == id:
                    return CeleryTask(task["id"], task["name"], task["args"], task["kwargs"])
    return None

def are_worker_active(app):
    worke_pong = app.control.inspect().ping()
    if not worke_pong:
        return False
    try:
        for worker, response in worke_pong.items():
            if response['ok']:
                return True
    except Exception:
        pass
    return False