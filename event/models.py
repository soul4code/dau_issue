import datetime
from typing import Optional

from dateutil.relativedelta import relativedelta
from django.db import models
from django.contrib.postgres.fields import JSONField
from django.db.models import F, Index, When
from django.db.models.functions import TruncDate
from django.utils import timezone
from django_cte import With, CTEManager, CTEQuerySet

from dau_issue.expressions import CustomIndex


class EventQuerySet(CTEQuerySet):
    def dau(
        self,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
    ):
        """
            За диапазон дат возвращается 3 поля: day, users и returned.
            В users лежит количество уникальных юзеров за день,
            а в returned - сколько из них посещали сайт в любой из предыдущих дней
            за всю историю.
        """
        end = end or timezone.now()
        start = start or (timezone.now() - relativedelta(days=6)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        first_activity = With(
            Event.objects.values('user_id')
            .annotate(
                first_day=TruncDate(
                    models.Min('performed_at', tzinfo=timezone.get_current_timezone())
                )
            )
            .values('user_id', 'first_day'),
            name='first_activity',
        )

        active = With(
            Event.objects.filter(performed_at__gte=start, performed_at__lte=end)
            .annotate(day=TruncDate('performed_at', tzinfo=timezone.get_current_timezone()))
            .values('user_id', 'day'),
            name='active',
        )

        daily_stats = With(
            first_activity.join(active.queryset(), user_id=first_activity.col.user_id)
            .annotate(first_day=first_activity.col.first_day)
            .values('day')
            .annotate(
                users=models.Count('user_id', distinct=True),
                returned=models.Count(
                    models.Case(
                        When(first_day__lt=F('day'), then=F('user_id')),
                    ),
                    distinct=True,
                ),
            )
            .values('day', 'users', 'returned'),
            name='daily_stats',
        )

        return (
            daily_stats.queryset()
            .with_cte(first_activity)
            .with_cte(active)
            .with_cte(daily_stats)
            .values('day', 'users', 'returned')
        )

    def dau_from_rollup(
        self,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
    ):
        """
            Возврат предзаполненного DAU
        """
        end = end or timezone.now()
        start = start or timezone.now().date() - relativedelta(days=6)
        return EventDauRollup.objects.filter(
            day__gte=start.date(),
            day__lte=end.date(),
        ).values('day', 'users')

    def users_active_today_and_on_week(self, day: datetime.date = None):
        """
            Возвращаются user_ids  которые были на сайте сегодня
            и в любой из предыдущих семи дней. Если не был сегодня но был на неделе или
            был сегодня но на неделе не был - в выборку не попадет
        """
        day = day or timezone.now().date()
        start = datetime.datetime.combine(
            day, datetime.time.min, tzinfo=timezone.get_current_timezone()
        )
        end = start - relativedelta(days=7)
        active_today = Event.objects.filter(
            performed_at__gte=start,
        ).values_list('user_id', flat=True)

        active_on_week = Event.objects.filter(
            performed_at__lt=start,
            performed_at__gte=end,
        ).values_list('user_id', flat=True)

        return active_today.intersection(active_on_week).values('user_id')


class Event(models.Model):
    user_id = models.IntegerField(db_index=True)
    course_id = models.IntegerField(db_index=True)
    kind = models.TextField(null=False, blank=True, default='')
    performed_at = models.DateTimeField(null=False, db_index=True)
    properties = JSONField(null=False, blank=True, default=dict)

    objects = CTEManager.from_queryset(EventQuerySet)()

    def __str__(self):
        return f'{self.performed_at.timestamp()}: {self.user_id} - {self.course_id}'

    class Meta:
        db_table = 'dau_issue_event_event'
        indexes = [
            CustomIndex(
                f"date(performed_at AT TIME ZONE '{timezone.get_current_timezone_name()}')",
                name='idx_event_performed_at_date',
            ),
            Index(fields=['user_id', 'performed_at']),
        ]


class EventDauRollup(models.Model):
    day = models.DateField(db_index=True, unique=True)
    users = models.PositiveIntegerField()
    returned = models.PositiveIntegerField()

    def __str__(self):
        return f'{self.day}: {self.users}'

    class Meta:
        db_table = 'dau_issue_event_dau_rollup'


class UserActivityRollup(models.Model):
    day = models.DateField(db_index=True)
    user_id = models.IntegerField(db_index=True)

    def __str__(self):
        return f'{self.day}: {self.user_id}'

    class Meta:
        db_table = 'dau_issue_user_activity_rollup'
        unique_together = ('day', 'user_id')
