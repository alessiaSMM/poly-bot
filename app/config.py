import os
from dotenv import load_dotenv

# ROOT del progetto = cartella sopra /app
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))

# path file .env
ENV_PATH = os.path.join(ROOT_DIR, ".env")

# carica variabili
load_dotenv(ENV_PATH)

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION_ID = os.getenv("LOCATION_ID")
KEYRING_ID = os.getenv("KEYRING_ID")
KEY_ID = os.getenv("KEY_ID")
KEY_VERSION = os.getenv("KEY_VERSION")
RPC_URL = os.getenv("RPC_URL")
