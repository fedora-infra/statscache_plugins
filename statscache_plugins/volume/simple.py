import statscache.plugins
import statscache.schedule
from statscache_plugins.volume.utils import VolumePluginMixin


class PluginMixin(VolumePluginMixin):
    name = "volume"
    summary = "the number of messages coming across the bus"
    description = """
    This is perhaps the most simple metric catalogued by statscache.
    For any given time window, the number of messages are simply counted.
    It can give you a baseline quantity against which you could normalize
    other statistics.
    """

    def make_model(self):
        class Result(statscache.plugins.ScalarModel):
            __tablename__ = 'data_volume_%s' % self.frequency
        return Result

    def handle(self, session, timestamp, messages):
        result = self.model(timestamp=timestamp, scalar=len(messages))
        session.add(result)


class OneSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1s')


class FiveSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('5s')


class OneMinuteFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1m')
