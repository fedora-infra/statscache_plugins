import datetime

from statscache_plugins.pagure.utils import PagurePluginMixin, plugin_factory

import sqlalchemy as sa


class PluginMixin(PagurePluginMixin):
    name = "Number of contribution to pagure.io"
    summary = "Count the number of contribution to pagure.io per day"
    description = """
    Counts the number of messages on fedmsg related to pagure.io for each day.
    """
    _keys = ['timestamp']

    def process(self, message):
        if 'io.pagure.prod' not in message['topic']:
            return
        timestamp = self.schedule.next(
            now=datetime.datetime.fromtimestamp(message['timestamp'])
        )
        self._volumes[(timestamp,)] += 1

plugins = plugin_factory(
    [datetime.timedelta(seconds=86400)],
    PluginMixin,
    "PagureContribution",
    "data_pagure_contibution_",
    {
        'volume': sa.Column(sa.Integer, nullable=False),
    }
)
