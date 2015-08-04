from __future__ import absolute_import
import statscache.plugins

import datetime
import json
import re
import requests


class Plugin(statscache.plugins.BasePlugin):
    name = "releng, compose"
    summary = "Compose logs"
    description = """
    Latest compose logs
    """
    topics = [
        "org.fedoraproject.prod.compose.rawhide.mash",
        "org.fedoraproject.prod.compose.rawhide.pungify",
        "org.fedoraproject.prod.compose.rawhide.rsync",
        "org.fedoraproject.prod.compose.rawhide",
        "org.fedoraproject.prod.compose.branched.mash",
        "org.fedoraproject.prod.compose.branched.pungify",
        "org.fedoraproject.prod.compose.branched.rsync",
        "org.fedoraproject.prod.compose.branched"
    ]
    architectures = ['', 'arm', 'ppc', 's390']
    p = re.compile(r'(?P<topic>[\w.]+)\.(?P<status>start|complete)')

    def __init__(self, *args, **kwargs):
        super(Plugin, self).__init__(*args, **kwargs)
        self._seen = {}
        self._queue = {}

    def process(self, message):
        m = self.p.match(message['topic'])
        if m is None:
            return
        topic, status = m.groups()
        if topic not in self.topics:
            return
        tokens = topic.split('.')
        # agent = (len(tokens) == 6 and tokens[-1]) or 'compose'
        arch = message['msg'].get('arch') or 'primary'
        branch = tokens[4]
        name = tokens[-1]
        category = 'compose-{}_{}'.format(branch, arch)
        category_constraint = name
        timestamp = datetime.datetime.fromtimestamp(message['timestamp'])

        # check if this message already is out-of-date
        last_seen = self._seen.get((category, category_constraint))
        if last_seen and timestamp <= last_seen:
            return
        self._seen[(category, category_constraint)] = timestamp

        msg = json.dumps({
            'name': name.title(),
            'status': status,
        })
        self._queue[(category, category_constraint)] = (timestamp, msg)

    def update(self, session):
        for ((category, category_constraint),
             (timestamp, message)) in self._queue.items():
            result = session.query(self.model)\
                .filter(self.model.category == category)\
                .filter(self.model.category_constraint == category_constraint)
            row = result.first()
            if row:
                row.timestamp = timestamp
                row.message = message
            else:
                row = self.model(
                    timestamp=timestamp,
                    message=message,
                    category=category,
                    category_constraint=category_constraint
                )
            session.add(row)
        session.commit()
        self._queue.clear()
