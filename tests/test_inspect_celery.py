import pytest
import sys
from unittest import mock
from celery_singleton.inspect_celery import get_task_by_id, are_worker_active, CeleryTask


class TestInspectCelery:
    def test__get_task_by_id__should_return_matching_task__when_task_active(self):
        # Given
        task_id = '262a1cf9-2c4f-4680-8261-7498fb39756c'
        active_tasks = {'celery@worker_host': [{'id': task_id, 'name': 'simple_task', 'args': [1, 2, 3], 'kwargs': {}, 'type': 'simple_task', 'hostname': 'celery@worker_host', 'time_start': 1588508284.207397, 'acknowledged': True, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': None}, 'worker_pid': 45895}]}
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = active_tasks
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_task = get_task_by_id(mock_app, task_id)

        # Then
        assert active_task is not None
        assert active_task.id == task_id
        assert active_task.name == 'simple_task'
        assert active_task.args == [1, 2, 3]
        assert active_task.kwargs == {}
    
    def test__get_task_by_id__should_return_matching_task__when_task_scheduled(self):
        # Given
        task_id = '262a1cf9-2c4f-4680-8261-7498fb39756c'
        active_tasks = {'celery@worker_host': []}
        scheduled_tasks = {'celery@worker_host': [{'eta': '2020-05-12T17:31:32.886704+00:00', 'priority': 6, 'request': {'id': task_id, 'name': 'simple_task', 'args': [1, 2, 3], 'kwargs': {}, 'type': 'simple_task', 'hostname': 'celery@worker_host', 'time_start': None, 'acknowledged': False, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': None}, 'worker_pid': None}}]}
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = active_tasks
        inspect_mock.scheduled.return_value = scheduled_tasks
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_task = get_task_by_id(mock_app, task_id)

        # Then
        assert active_task is not None
        assert active_task.id == task_id
        assert active_task.name == 'simple_task'
        assert active_task.args == [1, 2, 3]
        assert active_task.kwargs == {}
    
    def test__get_task_by_id__should_return_matching_task__when_task_reserved(self):
        # Given
        task_id = '262a1cf9-2c4f-4680-8261-7498fb39756c'
        active_tasks = {'celery@worker_host': []}
        scheduled_tasks = {'celery@worker_host': []}
        reserved_tasks = {'celery@worker_host': [{'id': task_id, 'name': 'simple_task', 'args': [1, 2, 3], 'kwargs': {}, 'type': 'simple_task', 'hostname': 'celery@worker_host', 'time_start': 1588508284.207397, 'acknowledged': True, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': None}, 'worker_pid': 45895}]}
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = active_tasks
        inspect_mock.scheduled.return_value = scheduled_tasks
        inspect_mock.reserved.return_value = reserved_tasks
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_task = get_task_by_id(mock_app, task_id)

        # Then
        assert active_task is not None
        assert active_task.id == task_id
        assert active_task.name == 'simple_task'
        assert active_task.args == [1, 2, 3]
        assert active_task.kwargs == {}
    
    def test__get_task_by_id__should_return_none_when_no_matching_task_id(self):
        # Given
        active_tasks = {'celery@worker_host': [{'id': '262a1cf9-2c4f-4680-8261-7498fb39756c', 'name': 'simple_task', 'args': [1, 2, 3], 'kwargs': {}, 'type': 'simple_task', 'hostname': 'celery@worker_host', 'time_start': 1588508284.207397, 'acknowledged': True, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': None}, 'worker_pid': 45895}]}
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = active_tasks
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_task = get_task_by_id(mock_app, "bad_task_name")

        # Then
        assert active_task is None
    
    def test__get_task_by_id__should_return_none_when_no_active_tasks(self):
        # Given
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = None
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_task = get_task_by_id(mock_app, "any_name")

        # Then
        assert active_task is None
    
    def test__get_task_by_id__should_return_none_when_worker_answer_cannot_be_parsed(self):
        # Given
        inspect_mock = mock.MagicMock()
        inspect_mock.active.return_value = {'worker': ['bad_task_definition']}
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_task = get_task_by_id(mock_app, "bad_task_name")

        # Then
        assert active_task is None
    
    def test__are_worker_active__should_return_true_if_worker_responds_to_ping(self):
        # Given
        active_workers = {u'celery@host': {u'ok': u'pong'}}
        inspect_mock = mock.MagicMock()
        inspect_mock.ping.return_value = active_workers
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_workers_found = are_worker_active(mock_app)

        # Then
        assert active_workers_found
    
    def test__are_worker_active__should_return_false_if_worker_does_not_respond_to_ping(self):
        # Given
        active_workers = None
        inspect_mock = mock.MagicMock()
        inspect_mock.ping.return_value = active_workers
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_workers_found = are_worker_active(mock_app)

        # Then
        assert not active_workers_found
    
    def test__are_worker_active__should_return_false_if_worker_respond_ko_to_ping(self):
        # Given
        active_workers = {u'celery@host': {u'not_ok': u'pong'}}
        inspect_mock = mock.MagicMock()
        inspect_mock.ping.return_value = active_workers
        control_mock = mock.MagicMock()
        control_mock.inspect.return_value = inspect_mock
        mock_app = mock.MagicMock()
        mock_app.control = control_mock

        # When
        active_workers_found = are_worker_active(mock_app)

        # Then
        assert not active_workers_found

