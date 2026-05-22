import pytest
from unittest.mock import patch, AsyncMock


class TestParkingLLMAgent:
    """Тесты LLM-агента для парковки"""

    @pytest.mark.asyncio
    async def test_parse_with_llm_success_zone_a(self):
        from agents.llm_agent import parse_with_llm

        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_response = AsyncMock()
            mock_response.json = AsyncMock(return_value={
                "response": '{"zone": "A", "preference": "standard", "hours": 2}'
            })
            mock_post.return_value.__aenter__.return_value = mock_response

            result = await parse_with_llm("место в зоне А на 2 часа")
            assert result["zone"] == "A"
            assert result["hours"] == 2

    @pytest.mark.asyncio
    async def test_parse_with_llm_success_zone_c_elevator(self):
        from agents.llm_agent import parse_with_llm

        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_response = AsyncMock()
            mock_response.json = AsyncMock(return_value={
                "response": '{"zone": "C", "preference": "elevator", "hours": 3}'
            })
            mock_post.return_value.__aenter__.return_value = mock_response

            result = await parse_with_llm("хочу место у лифта в зоне С на 3 часа")
            assert result["zone"] == "C"
            assert result["preference"] == "elevator"
            assert result["hours"] == 3

    @pytest.mark.asyncio
    async def test_parse_with_llm_fallback(self):
        from agents.llm_agent import parse_with_llm

        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_response = AsyncMock()
            mock_response.json = AsyncMock(return_value={
                "response": "непонятный ответ"
            })
            mock_post.return_value.__aenter__.return_value = mock_response

            result = await parse_with_llm("test")
            assert "zone" in result
            assert "hours" in result

    @pytest.mark.asyncio
    async def test_parse_with_llm_extracts_hours(self):
        from agents.llm_agent import parse_with_llm

        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_response = AsyncMock()
            mock_response.json = AsyncMock(return_value={
                "response": '{"zone": "B", "preference": "exit", "hours": 5}'
            })
            mock_post.return_value.__aenter__.return_value = mock_response

            result = await parse_with_llm("у выхода на 5 часов")
            assert result["hours"] == 5
            assert result["preference"] == "exit"