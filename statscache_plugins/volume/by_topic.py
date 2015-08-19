import datetime

from statscache_plugins.volume.utils import VolumePluginMixin, plugin_factory

import sqlalchemy as sa


class PluginMixin(VolumePluginMixin):
    name = "volume, by topic"
    summary = "the count of messages, organized by topic"
    description = """
    For any given time window, the number of messages that come across
    the bus for each topic.
    """
    _keys = ['topic', 'timestamp']

    def process(self, message):
        timestamp = self.schedule.next(
            now=datetime.datetime.fromtimestamp(msg['timestamp'])
        )
        self._volumes[(msg['topic'], timestamp)] += 1


plugins = plugin_factory(
    [datetime.timedelta(seconds=s) for s in [1, 5, 60]],
    PluginMixin,
    "VolumeByTopic",
    "data_volume_by_topic_",
    {
        'volume': sa.Column(sa.Integer, nullable=False),
        'topic': sa.Column(sa.UnicodeText, nullable=False, index=True),
    }
)
