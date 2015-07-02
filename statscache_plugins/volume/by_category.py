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
        freq = str(self.frequency)

        return type('VolumeByCategory' + freq + 'Model',
                    (statscache.plugins.BaseModel,),
                    {
                        '__tablename__': 'data_volume_by_category_' + freq,
                        'timestamp': sa.Column(sa.DateTime, nullable=False, index=True),
                        'volume': sa.Column(sa.Integer, nullable=False),
                        'category': sa.Column(sa.UnicodeText, nullable=False, index=True),
                    })

    def handle(self, session, timestamp, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            msg_timestamp = datetime.datetime.fromtimestamp(msg['timestamp'])
            volumes[(msg['topic'].split('.')[3],
                     self.frequency + msg_timestamp)] += 1

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
    frequency = statscache.plugins.Frequency(seconds=1)


class FiveSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.plugins.Frequency(seconds=5)


class OneMinuteFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.plugins.Frequency(minutes=1)

plugins = [OneSecondFrequencyPlugin, FiveSecondFrequencyPlugin, OneMinuteFrequencyPlugin]
