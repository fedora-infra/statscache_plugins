import statscache.plugins

import sqlalchemy as sa


def make_model(period):
    class Result(statscache.plugins.BaseModel):
        __tablename__ = 'data_volume_%i' % period
        timestamp = sa.Column(sa.DateTime, nullable=False, index=True)
        volume = sa.Column(sa.Integer, nullable=False)
    return Result


class Plugin(statscache.plugins.BasePlugin):
    name = "volume"
    summary = "the number of messages coming across the bus"
    description = """
    This is perhaps the most simple metric catalogued by statscache.
    For any given time window, the number of messages are simply counted.
    It can give you a baseline quantity against which you could normalize
    other statistics.
    """
    def handle(self, session, timestamp, messages):
        result = self.model(timestamp=timestamp, volume=len(messages))
        session.add(result)
