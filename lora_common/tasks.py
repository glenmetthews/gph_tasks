import pytz as pytz

from celery_core import app, redis_db
from lora_common.db.lora_db import LoraDB
from lora_common.config import DBSettings
from lora_common.db.grafana_bd import GrafanaDB
from datetime import datetime

db_settings = DBSettings()
lora_db = LoraDB(credentials=db_settings.pg_lora)
grafana_db = GrafanaDB(credentials=db_settings.pg_grafana)
@app.task
def update_service_info():
    for device_id in lora_db.get_device_list():
        if redis_db.get(f'service_{device_id}') is None:
            service_data = lora_db.get_last_service_package(
                device_id=device_id
            )
            grafana_db.set_service_data(
                device_id=device_id,
                time=datetime.utcfromtimestamp(service_data['time']//1000),
                vcc=service_data['vcc'],
                vcc_p=service_data['vcc_p'],
                error_code=service_data['error_code'],
                rs=service_data['rs']
            )
            redis_db.set(f'service_{device_id}', service_data['time'])
        else:
            service_data = lora_db.update_service_data(
                device_id=device_id,
                last_package_time=redis_db.get(f'service_{device_id}')
            )

            if service_data:
                grafana_db.update_service_data(
                    device_id=device_id,
                    time=datetime.utcfromtimestamp(service_data['time']//1000),
                    vcc=service_data['vcc'],
                    vcc_p=service_data['vcc_p'],
                    error_code=service_data['error_code'],
                    rs=service_data['rs']
                )
                redis_db.set(f'service_{device_id}', service_data['time'])
                print(service_data)

@app.task
def update_disp_data():
    data = lora_db.get_last_disp_data('0729331405303640')
    timezone = pytz.timezone('Europe/Moscow')
    time_value = datetime.fromtimestamp(data['time'])
    time_with_tz = timezone.localize(time_value)
    grafana_db.write_disp_to_db(
        device_id=data['device_id'],
        time=time_with_tz,
        value=data['displacement'],
        temp=data['temperature'],
        replacement='Трещиномер'
    )


if __name__ == '__main__':
    update_disp_data()