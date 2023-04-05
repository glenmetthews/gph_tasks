from celery_core import app


@app.task
def update_service_info():
    pass