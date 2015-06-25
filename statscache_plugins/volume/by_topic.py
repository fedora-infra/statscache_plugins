import collections
import datetime

import statscache.plugins
from statscache_plugins.volume.utils import VolumePluginMixin

import sqlalchemy as sa


class PluginMixin(VolumePluginMixin):
    name = "volume, by topic"
    summary = "the count of messages, organized by topic"
    description = """
    For any given time window, the number of messages that come across
    the bus for each topic.
    """

    def make_model(self):
        freq = str(self.frequency)

        return type('VolumeByTopic' + freq + 'Model',
                    (statscache.plugins.BaseModel,),
                    {
                        '__tablename__': 'data_volume_by_topic_' + freq,
                        'timestamp': sa.Column(sa.DateTime, nullable=False, index=True),
                        'volume': sa.Column(sa.Integer, nullable=False),
                        'topic': sa.Column(sa.UnicodeText, nullable=False, index=True),
                    })

    def handle(self, session, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            msg_timestamp = datetime.datetime.fromtimestamp(msg['timestamp'])
            volumes[(msg['topic'],
                     self.frequency.next(msg_timestamp))] += 1

        for key, volume in volumes.items():
            topic, timestamp = key
            result = session.query(self.model)\
                .filter(self.model.topic == topic)\
                .filter(self.model.timestamp == timestamp)
            row = result.first()
            if row:
                row.volume += volume
            else:
                row = self.model(
                    timestamp=timestamp,
                    volume=volume,
                    topic=topic)
            session.add(row)
        session.commit()


class OneSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1s')


class FiveSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('5s')


class OneMinuteFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1m')
