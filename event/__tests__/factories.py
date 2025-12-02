import factory
from django.utils import timezone
from factory.django import DjangoModelFactory
from factory.fuzzy import FuzzyInteger, FuzzyText, FuzzyDateTime


class EventFactory(DjangoModelFactory):
    user_id = FuzzyInteger(1, 10000)
    course_id = FuzzyInteger(1, 10000)
    kind = FuzzyText()
    performed_at = FuzzyDateTime(timezone.now())
    properties = factory.Dict(
        dict(
            some_param_1=FuzzyText(length=50),
            some_param_2=FuzzyText(length=50),
            some_param_3=FuzzyInteger(1, 10000),
        )
    )

    class Meta:
        model = 'event.Event'
