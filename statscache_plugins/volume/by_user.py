import collections
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

    def handle(self, session, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            msg_timestamp = datetime.datetime.fromtimestamp(msg['timestamp'])
            users = fedmsg.meta.msg2usernames(msg, **self.config)
            for user in users:
                volumes[(user, self.frequency.next(now=msg_timestamp))] += 1

        for key, volume in volumes.items():
            user, timestamp = key
            result = session.query(self.model)\
                .filter(self.model.user == user)\
                .filter(self.model.timestamp == timestamp)
            row = result.first()
            if row:
                row.volume += volume
            else:
                row = self.model(
                    timestamp=timestamp,
                    volume=volume,
                    user=user)
            session.add(row)
        session.commit()


plugins = plugin_factory(
    [datetime.timedelta(seconds=s) for s in [1, 5, 60]],
    PluginMixin,
    "VolumeByUser",
    "data_volume_by_user_",
    columns={
        'volume': sa.Column(sa.Integer, nullable=False),
        'user': sa.Column(sa.UnicodeText, nullable=False, index=True),
    }
)
