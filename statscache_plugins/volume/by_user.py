import collections

import statscache.plugins

import fedmsg.meta
import sqlalchemy as sa


def make_model(period):
    class Result(statscache.plugins.BaseModel):
        __tablename__ = 'data_volume_by_user_%i' % period
        timestamp = sa.Column(sa.DateTime, nullable=False, index=True)
        volume = sa.Column(sa.Integer, nullable=False)
        user = sa.Column(sa.UnicodeText, nullable=False, index=True)

    return Result


class Plugin(statscache.plugins.BasePlugin):
    name = "volume, by user"
    summary = "the count of messages, organized by user"
    description = """
    For any given time window, the number of messages that come across
    the bus for each user.
    """
    def handle(self, session, timestamp, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            users = fedmsg.meta.msg2usernames(msg, **self.config)
            for user in users:
                volumes[user] += 1

        for user, volume in volumes.items():
            result = self.model(
                timestamp=timestamp,
                volume=len(messages),
                user=user)
            session.add(result)
