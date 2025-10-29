import os
from pathlib import Path
from dotenv import load_dotenv


def load_env():

    base_dir = Path(__file__).resolve().parent
    env_file = os.getenv("ENV_FILE")

    if env_file:
        dotenv_path = Path(env_file).expanduser()
        if not dotenv_path.is_absolute():
            dotenv_path = base_dir / dotenv_path
    else:
        dotenv_path = base_dir / ".env"

    load_dotenv(dotenv_path=dotenv_path)
