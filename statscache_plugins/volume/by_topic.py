import collections

import statscache.plugins

import sqlalchemy as sa


def make_model(period):
    class Result(statscache.plugins.BaseModel):
        __tablename__ = 'data_volume_by_topic_%i' % period
        id = sa.Column(sa.Integer, primary_key=True)
        timestamp = sa.Column(sa.DateTime, nullable=False, index=True)
        volume = sa.Column(sa.Integer, nullable=False)
        topic = sa.Column(sa.UnicodeText, nullable=False, index=True)

    return Result


class Plugin(statscache.plugins.BasePlugin):
    name = "volume, by topic"
    summary = "the count of messages, organized by topic"
    description = """
    For any given time window, the number of messages that come across
    the bus for each topic.
    """
    def handle(self, session, timestamp, messages):
        topics = self.get_default_topics(session)
        volumes = collections.defaultdict(int)
        volumes.update(zip(topics, [0] * len(topics)))
        for msg in messages:
            volumes[msg['topic']] += 1

        for topic, volume in volumes.items():
            result = self.model(
                timestamp=timestamp,
                volume=len(messages),
                topic=topic)
            session.add(result)

    def get_default_topics(self, session):
        # TODO -- get the existing topics out of the sessiona
        return []
