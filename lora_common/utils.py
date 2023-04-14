from celery_core import redis_db
from db.lora_db import LoraDB
from db.grafana_bd import GrafanaDB
from config import DBSettings


db_settings = DBSettings()
lora_db = LoraDB(credentials=db_settings.pg_lora)
grafana_db = GrafanaDB(credentials=db_settings.pg_grafana)


def get_all_device_data(device_id):
    data = lora_db.get_all_data_package_of_device(device_id)
    for record in data:
        grafana_db.write_disp_to_db(
            device_id=record['device_id'],
            time=record['time'],
            value=record['displacement'],
            temp=record['temperature'],
            replacement='Трещиномер'
        )


if __name__ == '__main__':

    get_all_device_data('0729331405303640')

