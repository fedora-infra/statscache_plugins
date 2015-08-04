from __future__ import absolute_import
import datetime
import json
import requests

import statscache.plugins


class Plugin(statscache.plugins.BasePlugin):
    name = "releng, amis"
    summary = "Build logs for successful upload/test of cloud images"
    description = """
    Latest build logs for successful upload/test of cloud images
    """
    topics = [
        'org.fedoraproject.prod.fedimg.image.upload',
        'org.fedoraproject.prod.fedimg.image.test'
    ]

    def __init__(self, *args, **kwargs):
        super(Plugin, self).__init__(*args, **kwargs)
        self._seen = {}
        self._queue = {}

    def process(self, message):
        if not (message['topic'] in self.topics and
                message['msg'].get('status') == 'completed'):
            return
        status = 'completed'
        # arch = message['msg']['image_name'].split('.')[1]
        tokens = message['msg']['image_name'].split('.')[0].split('-')
        # flavour = tokens[2].lower()
        version = tokens[3].lower()
        # tstamp = tokens[4]
        ec2_region = message['msg']['destination'].split('(')[1][:-1]
        timestamp = datetime.datetime.fromtimestamp(message['timestamp'])

        branch = version if version == 'rawhide' else 'branched'

        category = 'ami-{}'.format(branch)
        category_constraint = '{}-{}'.format(message['msg']['image_name'],
                                             ec2_region)

        # check if this message already is out-of-date
        last_seen = self._seen.get((category, category_constraint))
        if last_seen and timestamp <= last_seen:
            return
        self._seen[(category, category_constraint)] = timestamp
        # TODO: dequeue any outdated model revisions

        msg = json.dumps({
            'name': message['msg']['image_name'],
            'extra_text': '{region}'.format(
                region=message['msg']['destination']),
            'extra_link': (
                "https://redirect.fedoraproject.org/console.aws."
                "amazon.com/ec2/v2/home?region={region}"
                "#LaunchInstanceWizard:ami={name}".format(
                    name=message['msg']['extra']['id'],
                    region=message['msg']['destination'])
            ),
            'status': status
        })
        self._queue[(category, category_constraint)] = (timestamp, msg)

    def update(self, session):
        for ((category, category_constraint),
             (timestamp, message)) in self._queue.items():
            result = session.query(self.model)\
                .filter_by(category=category)\
                .filter_by(category_constraint=category_constraint)
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
        # let the main plugin commit the transaction

    def initialize(self, session, datagrepper_endpoint=None):
        latest = session.query(self.model).filter(
            self.model.category.startswith('ami-')).order_by(
                self.model.timestamp.desc()).first()
        delta = 31536000
        if latest:
            delta = int(
                (datetime.datetime.now() - latest.timestamp).total_seconds())
        for topic in self.topics:
            resp = requests.get(
                datagrepper_endpoint,
                params={
                    'delta': delta,
                    'rows_per_page': 100,
                    'order': 'desc',
                    'meta': 'link',
                    'topic': topic,
                    'contains': 'completed'
                }
            )
            map(self.process, resp.json().get('raw_messages', []))
            self.update(session)
