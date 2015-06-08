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

    def handle(self, session, messages):
        rows = []
        for message in messages:
            m = self.p.match(message['topic'])
            if m is None:
                continue
            topic, status = m.groups()
            if topic not in self.topics:
                continue
            tokens = topic.split('.')
            # agent = (len(tokens) == 6 and tokens[-1]) or 'compose'
            arch = message['msg'].get('arch') or 'primary'
            branch = tokens[4]
            name = tokens[-1]
            category = 'compose-{}_{}'.format(branch, arch)
            category_constraint = name
            msg = json.dumps({
                'name': name.title(),
                'status': status,
            })
            msg_timestamp = datetime.datetime.fromtimestamp(
                message['timestamp'])
            last_seen_key = '{}:{}'.format(category, category_constraint)
            last_seen = self._seen.get(last_seen_key)
            if last_seen and msg_timestamp <= last_seen:
                continue
            self._seen[last_seen_key] = msg_timestamp
            result = session.query(self.model)\
                .filter(self.model.category == category)\
                .filter(self.model.category_constraint == category_constraint)
            row = result.first()
            if row:
                row.timestamp = msg_timestamp
                row.message = msg
            else:
                row = self.model(
                    timestamp=msg_timestamp,
                    message=msg,
                    category=category,
                    category_constraint=category_constraint
                )
            rows.append(row)
        return rows

    def initialize(self, session, datagrepper_endpoint=None):
        latest = session.query(self.model).filter(
            self.model.category.startswith('compose-')).order_by(
                self.model.timestamp.desc()).first()
        delta = 2000000
        if latest:
            delta = int(
                (datetime.datetime.now() - latest.timestamp).total_seconds())
        for topic in self.topics:
            params = {
                'delta': delta,
                'rows_per_page': 100,
                'order': 'desc',
                'topic': ['{}.start'.format(topic),
                          '{}.complete'.format(topic)]
            }
            resp = requests.get(
                datagrepper_endpoint,
                params=params
            )
            rows = self.handle(session, datetime.datetime.now(),
                               resp.json().get('raw_messages', []))
            session.add_all(rows)
            session.commit()
