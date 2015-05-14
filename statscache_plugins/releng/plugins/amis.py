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

    def handle(self, session, timestamp, messages):
        rows = []
        for message in messages:
            if not (message['topic'] in self.topics and
                    message['msg'].get('status') == 'completed'):
                continue
            status = 'completed'
            arch = message['msg']['image_name'].split('.')[1]
            tokens = message['msg']['image_name'].split('.')[0].split('-')
            flavour = tokens[2].lower()
            version = tokens[3].lower()
            tstamp = tokens[4]
            ec2_region = message['msg']['destination'].split('(')[1][:-1]
            msg_timestamp = datetime.datetime.fromtimestamp(message['timestamp'])

            branch = version if version == 'rawhide' else 'branched'

            category = 'ami-{}'.format(branch)
            category_constraint = '{}-{}'.format(
                message['msg']['image_name'], ec2_region)
            last_seen_key = '{}:{}'.format(category, category_constraint)
            last_seen = self._seen.get(last_seen_key)
            if last_seen and msg_timestamp <= last_seen:
                continue
            self._seen[last_seen_key] = msg_timestamp
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
            rows = self.handle(session, datetime.datetime.now(),
                               resp.json().get('raw_messages', []))
            session.add_all(rows)
            session.commit()

