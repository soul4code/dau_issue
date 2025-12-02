import pytest

from event.__tests__.factories import EventFactory


@pytest.fixture
def event_factory():
    def factory(**kwargs):
        return EventFactory.create(**kwargs)

    return factory
