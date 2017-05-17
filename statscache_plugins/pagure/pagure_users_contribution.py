import datetime

from statscache_plugins.pagure.utils import PagurePluginMixin, plugin_factory

import sqlalchemy as sa
import fedmsg.meta


class PluginMixin(PagurePluginMixin):
    name = "Number of contribution per user to pagure.io"
    summary = "Count the number of user's contribution to pagure.io per day"
    description = """
    Counts the number of messages on fedmsg related to pagure.io per user for each day.
    """
    _keys = ['user', 'timestamp']

    def process(self, message):
        if 'io.pagure.prod' not in message['topic']:
            return
        timestamp = self.schedule.next(
            now=datetime.datetime.fromtimestamp(message['timestamp'])
        )
        users = fedmsg.meta.msg2usernames(message, **self.config)
        for user in users:
            self._volumes[(user, timestamp)] += 1

plugins = plugin_factory(
    [datetime.timedelta(seconds=86400)],
    PluginMixin,
    "PagureUserContribution",
    "data_pagure_user_contibution_",
    {
        'volume': sa.Column(sa.Integer, nullable=False),
        'user': sa.Column(sa.UnicodeText, nullable=False, index=True),
    }
)
