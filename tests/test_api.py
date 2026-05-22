import pytest
import requests

BASE_URL = "http://localhost:8000"


class TestParkingAPI:
    """Тесты реального API (требуют запущенного оркестратора на http://localhost:8000)"""

    @pytest.fixture(scope="module")
    def check_server(self):
        try:
            response = requests.get(f"{BASE_URL}/health", timeout=2)
            if response.status_code != 200:
                pytest.skip("Оркестратор не запущен")
        except:
            pytest.skip("Оркестратор не запущен")
        return True

    def test_health(self, check_server):
        response = requests.get(f"{BASE_URL}/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_pipeline_missing_zone(self, check_server):
        response = requests.post(f"{BASE_URL}/pipeline", json={
            "car_number": "A123BC",
            "hours": 2
        })
        assert response.status_code == 422

    def test_pipeline_missing_car_number(self, check_server):
        response = requests.post(f"{BASE_URL}/pipeline", json={
            "zone": "A",
            "hours": 2
        })
        assert response.status_code == 422

    def test_pipeline_missing_hours(self, check_server):
        response = requests.post(f"{BASE_URL}/pipeline", json={
            "zone": "A",
            "car_number": "A123BC"
        })
        assert response.status_code == 422

    def test_pipeline_invalid_zone(self, check_server):
        response = requests.post(f"{BASE_URL}/pipeline", json={
            "zone": "Z",
            "car_number": "A123BC",
            "hours": 2
        })
        assert response.status_code in [400, 422]

    def test_pipeline_valid_request(self, check_server):
        response = requests.post(f"{BASE_URL}/pipeline", json={
            "zone": "A",
            "car_number": "TEST001",
            "hours": 2
        })
        assert response.status_code in [200, 400]
        if response.status_code == 200:
            data = response.json()
            assert "status" in data