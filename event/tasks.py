from celery import shared_task
from dateutil.relativedelta import relativedelta
from django.utils import timezone

from event.models import EventDauRollup, Event


@shared_task(
    max_retries=5,
    default_retry_delay=10,
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=False,
)
def fill_dau_rollup():
    """
    В целом в postgres есть расширение для cron и можно было бы
    вообще обойтись без таски, но думаю все же правильнее будет rollup в celery
    заполнять, для пущего контроля. Если делать на postgres то обязательно забудут
    завести cron например при переезде на другой инстанс
    """
    yesterday = (timezone.now() - relativedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    dau = Event.objects.dau(start=yesterday)
    # Тут можно было бы и одним запросом сделать но это просто набросок
    # В новой django есть обработка коллизий для таких штук, а тут пришлось
    # бы писать обертку или сырой запрос
    for dau_row in dau:
        EventDauRollup.objects.update_or_create(
            day=dau_row['day'],
            defaults={
                'users': dau_row['users'],
                'returned': dau_row['returned'],
            },
        )
