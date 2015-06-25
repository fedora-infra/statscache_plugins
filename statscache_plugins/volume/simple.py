import collections
import datetime

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
        freq = str(self.frequency)

        return type('Volume' + freq + 'Model',
                    (statscache.plugins.ScalarModel,),
                    {
                        '__tablename__': 'data_volume_' + freq,
                    })

    def handle(self, session, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            msg_timestamp = datetime.datetime.fromtimestamp(msg['timestamp'])
            volumes[msg_timestamp] += 1
        for timestamp, volume in volumes.items():

            result = session.query(self.model)\
                .filter(self.model.timestamp == timestamp)
            row = result.first()
            if row:
                row.scalar += volume
            else:
                row = self.model(
                    timestamp=timestamp,
                    scalar=volume)
            session.add(row)
        session.commit()


class OneSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1s')


class FiveSecondFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('5s')


class OneMinuteFrequencyPlugin(PluginMixin, statscache.plugins.BasePlugin):
    frequency = statscache.schedule.Frequency('1m')
