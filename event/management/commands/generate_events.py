import asyncio

import factory
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from factory.fuzzy import FuzzyInteger, FuzzyText, FuzzyDateTime
from sqlalchemy import Column, Integer, Text, DateTime, JSON
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncAttrs,
    AsyncSession,
)
from sqlalchemy.orm import declarative_base
import tqdm

db_url = DB_URL = (
    f'postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASSWORD}@'
    f'{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}'
)

Base = declarative_base(cls=AsyncAttrs)


class Event(Base):
    __tablename__ = 'dau_issue_event_event'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    course_id = Column(Integer)
    kind = Column(Text, default='')
    performed_at = Column(DateTime(timezone=True))
    properties = Column(JSON, default=dict)


performed_start = timezone.now() - relativedelta(years=1)


performed_end = timezone.now()

engine = create_async_engine(db_url, echo=False, future=True)

async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


class EventFactory(factory.alchemy.SQLAlchemyModelFactory):
    user_id = FuzzyInteger(1, 10000)
    course_id = FuzzyInteger(1, 10000)
    kind = FuzzyText()
    performed_at = FuzzyDateTime(performed_start, performed_end)
    properties = factory.Dict(
        dict(
            some_param_1=FuzzyText(length=50),
            some_param_2=FuzzyText(length=50),
            some_param_3=FuzzyInteger(1, 10000),
        )
    )

    class Meta:
        model = Event
        strategy = 'build'


class Command(BaseCommand):
    help = 'Generate synthetic events'

    def add_arguments(self, parser):
        parser.add_argument(
            '--count',
            dest='count',
            nargs='?',
            type=int,
            default=1000,
            help='Count of events to generate',
        )
        parser.add_argument(
            '--batch-size',
            dest='batch-size',
            nargs='?',
            type=int,
            default=500,
            help='Count of events for one transaction',
        )

    async def _commit_and_close(self, session: AsyncSession):
        try:
            await session.commit()
        except Exception as e:
            self.stderr.write(f'Commit failed: {e}')
            await session.close()
            raise e
        else:
            await session.close()

    async def async_handle(self, *args, **options):
        count = options['count']
        batch_size = options['batch-size']
        async_tasks = []
        self.stdout.write(f'Start generating {count} events')
        semaphore = asyncio.Semaphore(5)
        session = async_session_maker()
        for i in tqdm.trange(count):
            instance = EventFactory()
            session.add(instance)
            if (i + 1) % batch_size == 0 or i + 1 == count:
                async with semaphore:
                    async_tasks.append(asyncio.create_task(self._commit_and_close(session)))
                session = async_session_maker()
        await session.close()
        await asyncio.gather(*async_tasks)
        self.stdout.write(f'{count} events generated successfully')

    def handle(self, *args, **options):
        asyncio.run(self.async_handle(*args, **options))
