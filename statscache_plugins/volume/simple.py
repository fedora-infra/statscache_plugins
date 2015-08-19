import datetime

from statscache_plugins.volume.utils import VolumePluginMixin, plugin_factory


class PluginMixin(VolumePluginMixin):
    name = "volume"
    summary = "the number of messages coming across the bus"
    description = """
    This is perhaps the most simple metric catalogued by statscache.
    For any given time window, the number of messages are simply counted.
    It can give you a baseline quantity against which you could normalize
    other statistics.
    """
    _keys = ['timestamp']

    def process(self, message):
        timestamp = datetime.datetime.fromtimestamp(message['timestamp'])
        self._volumes[(timestamp,)] += 1


plugins = plugin_factory(
    [datetime.timedelta(seconds=s) for s in [1, 5, 60]],
    PluginMixin,
    "Volume",
    "data_volume_",
    {
        'volume': sa.Column(sa.Integer, nullable=False),
    }
)
