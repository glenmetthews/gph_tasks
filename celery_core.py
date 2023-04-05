from celery import Celery


app = Celery('gph_tasks',
             broker='redis://127.0.0.1',
             backend='redis://127.0.0.1',
             include=['lora_common.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)


app.conf.beat_schedule = {
    'run-me-every-ten-seconds': {
        'task': 'lora_common.tasks.check',
        'schedule': 10.0
    },
    'run-me-every-20-seconds': {
            'task': 'lora_common.tasks.check',
            'schedule': 20.0
        },
}

if __name__ == '__main__':
    args = ['worker', '--loglevel=INFO', '--beat']
    app.worker_main(argv=args)
