import collections
import datetime

from statscache_plugins.volume.utils import VolumePluginMixin, plugin_factory

import fedmsg.meta
import sqlalchemy as sa


class PluginMixin(VolumePluginMixin):
    name = "volume, by package"
    summary = "the count of messages, organized by package"
    description = """
    For any given time window, the number of messages that come across
    the bus for each package.
    """

    def handle(self, session, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            msg_timestamp = datetime.datetime.fromtimestamp(msg['timestamp'])
            packages = fedmsg.meta.msg2packages(msg, **self.config)
            for package in packages:
                volumes[
                    (package, self.frequency.next(now=msg_timestamp))] += 1

        for key, volume in volumes.items():
            package, timestamp = key
            result = session.query(self.model)\
                .filter(self.model.package == package)\
                .filter(self.model.timestamp == timestamp)
            row = result.first()
            if row:
                row.volume += volume
            else:
                row = self.model(
                    timestamp=timestamp,
                    volume=volume,
                    package=package)
            session.add(row)
        session.commit()


plugins = plugin_factory(
    [datetime.timedelta(seconds=s) for s in [1, 5, 60]],
    PluginMixin,
    "VolumeByPackage",
    "data_volume_by_package_",
    columns={
        'volume': sa.Column(sa.Integer, nullable=False),
        'package': sa.Column(sa.UnicodeText, nullable=False, index=True),
    }
)
