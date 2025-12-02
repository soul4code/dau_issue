import os

from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dau_issue.settings')

app = Celery('dau_issue')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.autodiscover_tasks()
