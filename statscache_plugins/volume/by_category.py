import datetime

from statscache_plugins.volume.utils import VolumePluginMixin, plugin_factory

import sqlalchemy as sa


class PluginMixin(VolumePluginMixin):
    name = "volume, by category"
    summary = "the count of messages, organized by category"
    description = """
    For any given time window, the number of messages that come across
    the bus for each category.
    """
    _keys = ['category', 'timestamp']

    def process(self, message):
        timestamp = self.schedule.next(
            now=datetime.datetime.fromtimestamp(message['timestamp'])
        )
        category = message['topic'].split('.')[3]
        self._volumes[(category, timestamp)] += 1


plugins = plugin_factory(
    [datetime.timedelta(seconds=s) for s in [1, 5, 60]],
    PluginMixin,
    "VolumeByCategory",
    "data_volume_by_category_",
    {
        'volume': sa.Column(sa.Integer, nullable=False),
        'category': sa.Column(sa.UnicodeText, nullable=False, index=True),
    }
)
