from celery import Celery
import redis

app = Celery('gph_tasks',
             broker='redis://127.0.0.1',
             backend='redis://127.0.0.1',
             include=['lora_common.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)


app.conf.beat_schedule = {
    'update_service_data': {
        'task': 'lora_common.tasks.update_service_info',
        'schedule': 300
    },

}

redis_db = redis.StrictRedis(
    host='127.0.0.1',
    port=6379,
    charset="utf-8",
    decode_responses=True
)

if __name__ == '__main__':
    args = ['worker', '--loglevel=INFO', '--beat']
    app.worker_main(argv=args)
