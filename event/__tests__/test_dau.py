import pytest
from dateutil.relativedelta import relativedelta
from django.utils import timezone

from event.models import Event


@pytest.mark.django_db
def test_dau_request(event_factory):
    _today_event = event_factory(
        user_id=1,
        performed_at=timezone.now(),
    )
    _yesterday_event = event_factory(
        user_id=1,
        performed_at=timezone.now() - relativedelta(days=1),
    )
    _very_old_event = event_factory(
        user_id=2,
        performed_at=timezone.now() - relativedelta(years=1),
    )
    _before_yesterday_event = event_factory(
        user_id=2,
        performed_at=timezone.now() - relativedelta(days=2),
    )
    dau = Event.objects.dau()
    assert len(dau) == 3
    assert dau[0]['users'] == 1
    assert dau[0]['returned'] == 1
    assert dau[1]['users'] == 1
    assert dau[1]['returned'] == 0
    assert dau[2]['users'] == 1
    assert dau[2]['returned'] == 1


@pytest.mark.django_db
def test_get_users_active_today_and_on_week(event_factory):
    _today_event = event_factory(
        user_id=1,
        performed_at=timezone.now(),
    )
    _yesterday_event = event_factory(
        user_id=1,
        performed_at=timezone.now() - relativedelta(days=1),
    )

    _inactive_event_today = event_factory(
        user_id=3,
        performed_at=timezone.now() - relativedelta(days=3),
    )

    _inactive_event_on_week = event_factory(
        user_id=4,
        performed_at=timezone.now(),
    )

    _inactive_event = event_factory(
        user_id=2,
        performed_at=timezone.now() - relativedelta(days=8),
    )
    users = Event.objects.users_active_today_and_on_week()
    assert Event.objects.count() == 5
    assert len(users) == 1
    assert users[0]['user_id'] == 1
