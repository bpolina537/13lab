import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orchestrator.orchestrator import PipelineOrchestrator


class TestOrchestratorModule:
    """Тесты для orchestrator/orchestrator.py"""

    @pytest.mark.asyncio
    async def test_connect_creates_subscription(self):
        orch = PipelineOrchestrator()

        with patch('nats.connect', new_callable=AsyncMock) as mock_connect:
            mock_nc = AsyncMock()
            mock_connect.return_value = mock_nc

            await orch.connect()

            assert orch.nc is not None
            mock_nc.subscribe.assert_any_call("tasks.completed", cb=orch._on_result)

    @pytest.mark.asyncio
    async def test_send_task_publishes_message(self):
        orch = PipelineOrchestrator()
        orch.nc = AsyncMock()
        orch.results = {}

        with patch('asyncio.Future') as mock_future:
            mock_future_instance = AsyncMock()
            mock_future.return_value = mock_future_instance

            with patch('asyncio.wait_for', side_effect=asyncio.TimeoutError()):
                with pytest.raises(Exception):
                    await orch.send_task("test.subject", {"key": "value"}, timeout=0.1, max_retries=1)

        # Проверяем, что publish был вызван
        assert orch.nc.publish.called

    @pytest.mark.asyncio
    async def test_on_result_sets_future(self):
        orch = PipelineOrchestrator()
        orch.results = {}

        future = asyncio.Future()
        orch.results["task-123"] = future

        # Создаём правильный мок сообщения
        mock_msg = MagicMock()
        mock_msg.data = b'{"task_id": "task-123", "result": {"status": "ok"}}'

        await orch._on_result(mock_msg)

        assert future.done()
        result = future.result()
        assert result["task_id"] == "task-123"

    @pytest.mark.asyncio
    async def test_on_result_with_nested_task_id(self):
        orch = PipelineOrchestrator()
        orch.results = {}

        future = asyncio.Future()
        orch.results["task-456"] = future

        mock_msg = MagicMock()
        mock_msg.data = b'{"result": {"task_id": "task-456", "data": "ok"}}'

        await orch._on_result(mock_msg)

        assert future.done()

    @pytest.mark.asyncio
    async def test_on_result_without_task_id_does_nothing(self):
        orch = PipelineOrchestrator()
        orch.results = {}

        future = asyncio.Future()
        orch.results["task-789"] = future

        mock_msg = MagicMock()
        mock_msg.data = b'{"agent": "search", "result": {"places": ["A1"]}}'

        await orch._on_result(mock_msg)

        assert not future.done()

    @pytest.mark.asyncio
    async def test_run_pipeline_success(self):
        orch = PipelineOrchestrator()

        async def mock_send_task(subject, payload, **kwargs):
            if "search" in subject:
                return {"result": {"places": ["A1"]}}
            elif "book" in subject:
                return {"result": {"booking_id": "test-123"}}
            elif "access" in subject:
                return {"result": {}}
            elif "payment" in subject:
                return {"result": {"amount": 200}}
            return {}

        orch.send_task = mock_send_task

        result = await orch.run_pipeline("A", "TEST", 2)

        assert result["status"] == "success"
        assert result["place_id"] == "A1"
        assert result["booking_id"] == "test-123"
        assert result["amount"] == 200

    @pytest.mark.asyncio
    async def test_run_pipeline_no_places(self):
        orch = PipelineOrchestrator()

        async def mock_send_task(subject, payload, **kwargs):
            if "search" in subject:
                return {"result": {"places": []}}
            return {}

        orch.send_task = mock_send_task

        result = await orch.run_pipeline("A", "TEST", 2)

        assert result["status"] == "failed"
        assert "Нет свободных мест" in result["error"]

    @pytest.mark.asyncio
    async def test_run_pipeline_no_booking_id(self):
        orch = PipelineOrchestrator()

        async def mock_send_task(subject, payload, **kwargs):
            if "search" in subject:
                return {"result": {"places": ["A1"]}}
            elif "book" in subject:
                return {"result": {"booking_id": None}}
            return {}

        orch.send_task = mock_send_task

        result = await orch.run_pipeline("A", "TEST", 2)

        assert result["status"] == "failed"

    @pytest.mark.asyncio
    async def test_run_pipeline_booking_error(self):
        orch = PipelineOrchestrator()

        async def mock_send_task(subject, payload, **kwargs):
            if "search" in subject:
                return {"result": {"places": ["A1"]}}
            elif "book" in subject:
                raise Exception("Booking service unavailable")
            return {}

        orch.send_task = mock_send_task

        result = await orch.run_pipeline("A", "TEST", 2)

        assert result["status"] == "failed"
        assert "Booking" in result["error"]

    @pytest.mark.asyncio
    async def test_run_pipeline_access_error(self):
        orch = PipelineOrchestrator()

        async def mock_send_task(subject, payload, **kwargs):
            if "search" in subject:
                return {"result": {"places": ["A1"]}}
            elif "book" in subject:
                return {"result": {"booking_id": "123"}}
            elif "access" in subject:
                raise Exception("Access denied")
            return {}

        orch.send_task = mock_send_task

        result = await orch.run_pipeline("A", "TEST", 2)

        assert result["status"] == "failed"

    @pytest.mark.asyncio
    async def test_run_pipeline_payment_error(self):
        orch = PipelineOrchestrator()

        async def mock_send_task(subject, payload, **kwargs):
            if "search" in subject:
                return {"result": {"places": ["A1"]}}
            elif "book" in subject:
                return {"result": {"booking_id": "123"}}
            elif "access" in subject:
                return {"result": {}}
            elif "payment" in subject:
                raise Exception("Payment failed")
            return {}

        orch.send_task = mock_send_task

        result = await orch.run_pipeline("A", "TEST", 2)

        assert result["status"] == "failed"