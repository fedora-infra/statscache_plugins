import collections
import datetime

import statscache.plugins
import statscache.schedule
from statscache_plugins.volume.utils import VolumePluginMixin

import sqlalchemy as sa


class PluginMixin(VolumePluginMixin):
    name = "volume, by category"
    summary = "the count of messages, organized by category"
    description = """
    For any given time window, the number of messages that come across
    the bus for each category.
    """

    def make_model(self):
        class Result(statscache.plugins.BaseModel):
            __tablename__ = 'data_volume_by_category_%s' % self.frequency
            timestamp = sa.Column(sa.DateTime, nullable=False, index=True)
            volume = sa.Column(sa.Integer, nullable=False)
            category = sa.Column(sa.UnicodeText, nullable=False, index=True)

        return Result

    def handle(self, session, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            msg_timestamp = datetime.datetime.fromtimestamp(msg['timestamp'])
            volumes[(msg['topic'].split('.')[3],
                     self.frequency.next(msg_timestamp))] += 1

        for key, volume in volumes.items():
            category, timestamp = key
            result = session.query(self.model)\
                .filter(self.model.category == category)\
                .filter(self.model.timestamp == timestamp)
            row = result.first()
            if row:
                row.volume += volume
            else:
                row = self.model(
                    timestamp=timestamp,
                    volume=volume,
                    category=category)
            session.add(row)
            session.commit()


class OneSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1s')


class FiveSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('5s')


class OneMinuteFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1m')
