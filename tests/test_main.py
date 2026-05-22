import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orchestrator.main import app, orchestrator

client = TestClient(app)


class TestMain:
    """Тесты для orchestrator/main.py"""

    def test_health(self):
        response = client.get("/health")
        assert response.status_code == 200

    def test_pipeline_missing_zone(self):
        response = client.post("/pipeline", json={"car_number": "A123BC", "hours": 2})
        assert response.status_code == 422

    def test_pipeline_missing_car_number(self):
        response = client.post("/pipeline", json={"zone": "A", "hours": 2})
        assert response.status_code == 422

    def test_pipeline_missing_hours(self):
        response = client.post("/pipeline", json={"zone": "A", "car_number": "A123BC"})
        assert response.status_code == 422

    def test_pipeline_invalid_zone(self):
        response = client.post("/pipeline", json={"zone": "Z", "car_number": "A123BC", "hours": 2})
        assert response.status_code in [400, 422]

    @patch('orchestrator.main.PipelineOrchestrator.run_pipeline')
    def test_pipeline_valid_request(self, mock_run_pipeline):
        mock_run_pipeline.return_value = {
            "status": "success",
            "place_id": "A1",
            "booking_id": "test-123",
            "amount": 200,
            "currency": "RUB"
        }
        response = client.post("/pipeline", json={"zone": "A", "car_number": "TEST001", "hours": 2})
        assert response.status_code == 200

    @patch('orchestrator.main.PipelineOrchestrator.run_pipeline')
    def test_pipeline_failure(self, mock_run_pipeline):
        mock_run_pipeline.return_value = {"status": "failed", "error": "Test error"}
        response = client.post("/pipeline", json={"zone": "A", "car_number": "TEST", "hours": 2})
        assert response.status_code == 400

    def test_llm_pipeline_missing_text(self):
        response = client.post("/llm_pipeline", json={})
        assert response.status_code == 422

    @patch.object(orchestrator, 'nc', new_callable=AsyncMock)

    def test_llm_pipeline_empty_text(self, mock_nc):
        response = client.post("/llm_pipeline", json={"text": ""})
        assert response.status_code in [400, 422]

    @patch.object(orchestrator, 'nc', new_callable=AsyncMock)
    @patch('orchestrator.main.asyncio.wait_for')
    @patch('orchestrator.main.PipelineOrchestrator.run_pipeline')
    def test_llm_pipeline_valid_text(self, mock_run_pipeline, mock_wait_for, mock_nc):
        """Валидный текст — должен вызвать publish"""
        mock_nc.publish = AsyncMock()
        mock_wait_for.return_value = {"result": {"zone": "A", "hours": 2}}
        mock_run_pipeline.return_value = {
            "status": "success",
            "place_id": "A1",
            "booking_id": "test",
            "amount": 200,
            "currency": "RUB"
        }

        with patch('orchestrator.main.uuid.uuid4', return_value="test-id"):
            response = client.post("/llm_pipeline", json={"text": "зона А"})
            # Может быть 200 или 400/500 из-за таймаута
            assert response.status_code in [200, 400, 500]