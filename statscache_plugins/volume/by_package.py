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

    def process(self, message):
        timestamp = self.schedule.next(
            now=datetime.datetime.fromtimestamp(message['timestamp'])
        )
        packages = fedmsg.meta.msg2packages(message, **self.config)
        for package in packages:
            self._volumes[(package, timestamp)] += 1

    def update(self, session):
        for key, volume in self._volumes.items():
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
        self._volumes.clear()


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
