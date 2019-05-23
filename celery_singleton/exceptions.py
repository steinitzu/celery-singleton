class CelerySingletonException(Exception):
    pass


class DuplicateTaskError(CelerySingletonException):
    """
    Raised when attempting to queue a duplicat task
    and `raise_on_duplicate` is enabled
    """

    def __init__(self, message, task_id):
        self.task_id = task_id
        super().__init__(message)

    pass
