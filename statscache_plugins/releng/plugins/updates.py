import datetime
import json
import requests

import statscache.plugins

import logging
log = logging.getLogger("statscache")


class Plugin(statscache.plugins.BasePlugin):
    name = "releng, updates"
    summary = "Updates to yum repos"
    description = """
    Last time updates were mashed and synced to the master mirror
    """
    topics = [
        'org.fedoraproject.prod.bodhi.updates.fedora.sync',
        'org.fedoraproject.prod.bodhi.updates.epel.sync',
    ]

    def __init__(self, *args, **kwargs):
        super(Plugin, self).__init__(*args, **kwargs)
        self._seen = {}

    def handle(self, session, timestamp, messages):
        log.info("In handle with %i messages" % len(messages))
        for message in messages:
            if not message['topic'] in self.topics:
                continue
            status = 'synced out'
            msg_timestamp = datetime.datetime.fromtimestamp(message['timestamp'])

            distro = message['topic'].split('.')[-2]
            release = message['msg']['release']
            repo = message['msg']['repo']
            name = "%s-%s" % (release, repo)

            category = 'updates-{}'.format(distro)

            last_seen = self._seen.get(name)
            if last_seen and msg_timestamp <= last_seen:
                continue
            self._seen[name] = msg_timestamp

            msg = json.dumps({
                'name': name,
                'status': status
                #'extra_text': 'wat',  # No extra text for updates..
                #'extra_link': 'wat',  # No link for updates messages
            })
            result = session.query(self.model)\
                .filter(self.model.category == category)\
                .filter(self.model.category_constraint == name)
            row = result.first()
            if row:
                log.info('found')
                row.timestamp = msg_timestamp
                row.message = msg
            else:
                row = self.model(
                    timestamp=msg_timestamp,
                    message=msg,
                    category=category,
                )
                session.add(row)
                log.info("Adding %r" % msg)

    def initialize(self, session, datagrepper_endpoint=None):
        latest = session.query(self.model).filter(
            self.model.category.startswith('updates-')).order_by(
                self.model.timestamp.desc()).first()
        delta = 31536000
        if latest:
            delta = datetime.datetime.now() - latest.timestamp
            delta = int(delta.total_seconds())
        for topic in self.topics:
            resp = requests.get(
                datagrepper_endpoint,
                params={
                    'delta': delta,
                    'rows_per_page': 100,
                    'order': 'desc',
                    'topic': topic,
                }
            )
            now = datetime.datetime.now()
            messages = resp.json().get('raw_messages', [])
            self.handle(session, now, messages)
            session.commit()
