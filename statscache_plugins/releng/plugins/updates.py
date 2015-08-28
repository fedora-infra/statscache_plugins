from __future__ import absolute_import
import datetime
import json

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
        self._queue = {}

    def process(self, message):
        if not message['topic'] in self.topics:
            return
        status = 'synced out'
        timestamp = datetime.datetime.fromtimestamp(message['timestamp'])

        distro = message['topic'].split('.')[-2]
        release = message['msg']['release']
        repo = message['msg']['repo']
        name = "%s-%s" % (release, repo)

        category = 'updates-{}'.format(distro)

        # check if this message already is out-of-date
        last_seen = self._seen.get(name)
        if last_seen and timestamp <= last_seen:
            return
        self._seen[name] = timestamp

        msg = json.dumps({
            'name': name,
            'status': status
            # 'extra_text': 'wat',  # No extra text for updates..
            # 'extra_link': 'wat',  # No link for updates messages
        })
        self._queue[name] = (timestamp, msg, category)

    def update(self, session):
        for (name, (timestamp, message, category)) in self._queue.items():
            result = session.query(self.model)\
                .filter(self.model.category == category)\
                .filter(self.model.category_constraint == name)
            row = result.first()
            if row:
                row.timestamp = timestamp
                row.message = message
            else:
                row = self.model(
                    timestamp=timestamp,
                    message=message,
                    category=category,
                )
            log.info("Adding %r" % message)
            session.add(row)
        session.commit()
        self._queue.clear()
