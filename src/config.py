# config.py
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

# Configuración
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
