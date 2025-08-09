import os
from dotenv import load_dotenv
load_dotenv()  # load from .env file
print(os.getenv("DB_PORT"))