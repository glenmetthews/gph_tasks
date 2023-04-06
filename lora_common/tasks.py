from celery_core import app, redis_db
from lora_common.db.lora_db import LoraDB
from lora_common.config import DBSettings


db_settings = DBSettings()
lora_db = LoraDB(credentials=db_settings.pg_lora)

@app.task
def update_service_info():
    for device_id in lora_db.get_device_list():
        if redis_db.get(f'service_{device_id}') is None:
            service_data = lora_db.get_last_service_package(
                device_id=device_id
            )
            redis_db.set(f'service_{device_id}', service_data['time'])
        else:
            service_data = lora_db.update_service_data(
                device_id=device_id,
                last_package_time=redis_db.get(f'service_{device_id}')
            )
            if service_data:
                redis_db.set(f'service_{device_id}', service_data['time'])
                print(service_data)



if __name__ == '__main__':
    update_service_info()