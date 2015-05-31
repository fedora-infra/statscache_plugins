import collections

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
        class Result(statscache.plugins.BaseModel):
            __tablename__ = 'data_volume_by_topic_%s' % self.frequency
            timestamp = sa.Column(sa.DateTime, nullable=False, index=True)
            volume = sa.Column(sa.Integer, nullable=False)
            topic = sa.Column(sa.UnicodeText, nullable=False, index=True)

        return Result

    def handle(self, session, timestamp, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            volumes[msg['topic']] += 1

        for topic, volume in volumes.items():
            result = self.model(
                timestamp=timestamp,
                volume=len(messages),
                topic=topic)
            session.add(result)


class OneSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1s')


class FiveSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('5s')


class OneMinuteFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1m')
