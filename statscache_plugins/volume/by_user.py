import datetime

from statscache_plugins.volume.utils import VolumePluginMixin, plugin_factory

import fedmsg.meta
import sqlalchemy as sa


class PluginMixin(VolumePluginMixin):
    name = "volume, by user"
    summary = "the count of messages, organized by user"
    description = """
    For any given time window, the number of messages that come across
    the bus for each user.
    """
    _keys = ['user', 'timestamp']

    def process(self, message):
        timestamp = self.schedule.next(
            now=datetime.datetime.fromtimestamp(message['timestamp'])
        )
        users = fedmsg.meta.msg2usernames(message, **self.config)
        for user in users:
            self._volumes[(user, timestamp)] += 1


plugins = plugin_factory(
    [datetime.timedelta(seconds=s) for s in [1, 5, 60]],
    PluginMixin,
    "VolumeByUser",
    "data_volume_by_user_",
    {
        'volume': sa.Column(sa.Integer, nullable=False),
        'user': sa.Column(sa.UnicodeText, nullable=False, index=True),
    }
)
