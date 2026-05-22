import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestWebUI:

    def test_app_file_exists(self):
        assert os.path.exists("webui/app.py")

    def test_contains_streamlit_import(self):
        with open("webui/app.py", "r", encoding="utf-8") as f:
            content = f.read()
            assert "streamlit" in content