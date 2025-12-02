from celery import shared_task
from dateutil.relativedelta import relativedelta
from django.utils import timezone

from event.models import EventDauRollup, Event


@shared_task
def fill_dau_rollup():
    yesterday = (timezone.now() - relativedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    dau = Event.objects.dau(start=yesterday)
    for dau_row in dau:
        EventDauRollup.objects.update_or_create(
            day=dau_row['day'],
            defaults={
                'users': dau_row['users'],
                'returned': dau_row['returned'],
            },
        )
