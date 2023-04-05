import redis
from pydantic import BaseSettings, PostgresDsn


class DBSettings(BaseSettings):

    pg_lora: PostgresDsn = 'postgres://georec:weq636@192.168.105.161:5433/gorizont_common'
    pg_grafana: PostgresDsn = 'postgres://postgres:Qwertyu*@192.168.105.187:5432/gorizont'


r = redis.Redis(
    host='127.0.0.1',
    port=6379,
    )

