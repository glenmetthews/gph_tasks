from celery_core import redis_db
from db.lora_db import LoraDB
from config import DBSettings


db_settings = DBSettings()
lora_db = LoraDB(credentials=db_settings.pg_lora)

def update_service_data():
    pass
