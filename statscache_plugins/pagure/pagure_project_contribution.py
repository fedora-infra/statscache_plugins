import datetime

from statscache_plugins.pagure.utils import PagurePluginMixin, plugin_factory

import sqlalchemy as sa


class PluginMixin(PagurePluginMixin):
    name = "Number of contribution per project to pagure.io"
    summary = "Count the number of project's contribution to pagure.io per day"
    description = """
    Counts the number of messages on fedmsg related to pagure.io per project for each day.
    """
    _keys = ['project', 'timestamp']

    def process(self, message):

        if 'io.pagure.prod' not in message['topic']\
            or 'io.pagure.prod.pagure.issue.comment.edited' == message['topic']: 
            return
        projects = set()

        if 'io.pagure.prod.pagure.pull-request' in message['topic']:
            projects.add(message['msg']['pullrequest']['project']['name'])
        elif 'io.pagure.prod.pagure.git.receive' == message['topic']:
            projects.add(message['msg']['repo']['name'])
        else: 
            projects.add(message['msg']['project']['name'])

        timestamp = self.schedule.next(
            now=datetime.datetime.fromtimestamp(message['timestamp'])
        )
        for project in projects:
            self._volumes[(project, timestamp)] += 1

plugins = plugin_factory(
    [datetime.timedelta(seconds=86400)],
    PluginMixin,
    "PagureProjectContribution",
    "data_pagure_project_contibution_",
    {
        'volume': sa.Column(sa.Integer, nullable=False),
        'project': sa.Column(sa.UnicodeText, nullable=False, index=True),
    }
)
