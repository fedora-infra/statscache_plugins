import collections

import statscache.plugins
from statscache_plugins.volume.utils import VolumePluginMixin

import fedmsg.meta
import sqlalchemy as sa


class PluginMixin(VolumePluginMixin):
    name = "volume, by user"
    summary = "the count of messages, organized by user"
    description = """
    For any given time window, the number of messages that come across
    the bus for each user.
    """

    def make_model(self):
        class Result(statscache.plugins.BaseModel):
            __tablename__ = 'data_volume_by_user_%s' % self.frequency
            timestamp = sa.Column(sa.DateTime, nullable=False, index=True)
            volume = sa.Column(sa.Integer, nullable=False)
            user = sa.Column(sa.UnicodeText, nullable=False, index=True)

        return Result

    def handle(self, session, timestamp, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            users = fedmsg.meta.msg2usernames(msg, **self.config)
            for user in users:
                volumes[user] += 1

        for user, volume in volumes.items():
            result = self.model(
                timestamp=timestamp,
                volume=len(messages),
                user=user)
            session.add(result)


class OneSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1s')


class FiveSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('5s')


class OneMinuteFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1m')
