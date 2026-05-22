# tests/test_scaler.py
import pytest
import subprocess
import sys


class TestScaler:

    def test_threshold_exists(self):
        """Проверяет, что константа THRESHOLD определена в файле"""
        with open("scaler/monitor.py", "r") as f:
            content = f.read()

        assert "THRESHOLD = 5" in content or "THRESHOLD=5" in content
        assert "CHECK_INTERVAL = 10" in content or "CHECK_INTERVAL=10" in content

    def test_scaler_syntax_is_valid(self):
        """Проверяет, что файл не содержит синтаксических ошибок"""
        result = subprocess.run([sys.executable, "-m", "py_compile", "scaler/monitor.py"],
                                capture_output=True)
        assert result.returncode == 0