import collections

import statscache.plugins
from statscache_plugins.volume.utils import VolumePluginMixin

import fedmsg.meta
import sqlalchemy as sa


class PluginMixin(VolumePluginMixin):
    name = "volume, by package"
    summary = "the count of messages, organized by package"
    description = """
    For any given time window, the number of messages that come across
    the bus for each package.
    """

    def make_model(self):
        class Result(statscache.plugins.BaseModel):
            __tablename__ = 'data_volume_by_package_%s' % self.frequency
            timestamp = sa.Column(sa.DateTime, nullable=False, index=True)
            volume = sa.Column(sa.Integer, nullable=False)
            package = sa.Column(sa.UnicodeText, nullable=False, index=True)

        return Result

    def handle(self, session, timestamp, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            packages = fedmsg.meta.msg2packages(msg, **self.config)
            for package in packages:
                volumes[package] += 1

        for package, volume in volumes.items():
            result = self.model(
                timestamp=timestamp,
                volume=len(messages),
                package=package)
            session.add(result)


class OneSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1s')


class FiveSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('5s')


class OneMinuteFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1m')
