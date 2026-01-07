"""
Автор: Андуганов Д.Г.
Тема практики: Автоматическая генерация новостных сообщений из плана мероприятий организации с помощью нейронных сетей
"""

import subprocess
import sys
from pathlib import Path


def main() -> None:
    app_path = Path(__file__).parent / "src" / "app" / "ui_streamlit.py"
    cmd = [sys.executable, "-m", "streamlit", "run", str(app_path)]
    raise SystemExit(subprocess.call(cmd))


if __name__ == "__main__":
    main()
