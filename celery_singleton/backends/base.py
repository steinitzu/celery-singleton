from abc import ABC, abstractmethod


class BaseBackend(ABC):
    @abstractmethod
    def lock(self, lock, task_id):
        """
        Store a lock for given lock value and task ID

        :param lock: Lock/mutex string
        :type lock: `str`
        :param task_id: Task id associated with the lock
        :type task_id: `str`
        :return: `True` if lock was aquired succesfully otherwise `False`
        :rtype: `bool`
        """

    @abstractmethod
    def unlock(self, lock):
        """
        Unlock the given lock

        :param lock: Lock/mutext string to unlock
        :type lock: `str`
        """

    @abstractmethod
    def get(self, lock):
        """
        Get task ID for given lock

        :param lock: Lock/mutext string
        :type lock: str
        :return: A task ID if exists, otherwise `None`
        :rtype: `str` or `None`
        """

    @abstractmethod
    def clear(self, key_prefix):
        """
        Clear all locks stored under given key_prefix

        :param key_prefix: Prefix of keys to clear
        :type key_prefix: str
        :return: `None`
        """
