import collections

import statscache.plugins

import sqlalchemy as sa


def make_model(period):
    class Result(statscache.plugins.BaseModel):
        __tablename__ = 'data_volume_by_category_%i' % period
        timestamp = sa.Column(sa.DateTime, nullable=False, index=True)
        volume = sa.Column(sa.Integer, nullable=False)
        category = sa.Column(sa.UnicodeText, nullable=False, index=True)

    return Result


class Plugin(statscache.plugins.BasePlugin):
    name = "volume, by category"
    summary = "the count of messages, organized by category"
    description = """
    For any given time window, the number of messages that come across
    the bus for each category.
    """
    def handle(self, session, timestamp, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            volumes[msg['topic'].split('.')[3]] += 1

        for category, volume in volumes.items():
            result = self.model(
                timestamp=timestamp,
                volume=len(messages),
                category=category)
            session.add(result)
