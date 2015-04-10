import collections

import statscache.plugins

import fedmsg.meta
import sqlalchemy as sa


def make_model(period):
    class Result(statscache.plugins.BaseModel):
        __tablename__ = 'data_volume_by_package_%i' % period
        timestamp = sa.Column(sa.DateTime, nullable=False, index=True)
        volume = sa.Column(sa.Integer, nullable=False)
        package = sa.Column(sa.UnicodeText, nullable=False, index=True)

    return Result


class Plugin(statscache.plugins.BasePlugin):
    name = "volume, by package"
    summary = "the count of messages, organized by package"
    description = """
    For any given time window, the number of messages that come across
    the bus for each package.
    """
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
