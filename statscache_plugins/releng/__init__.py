import datetime
import json

import statscache.plugins


FREQUENCIES = [60]


def make_model(period):
    class Result(statscache.plugins.BaseModel):
        __tablename__ = 'data_releng_dashboard'

    return Result


class Plugin(statscache.plugins.BasePlugin):
    name = "Release engineering event logs"
    summary = "Release engineering event logs, organized by category."
    description = """
    Recent release engineering event logs to be used for rendering
    release engineering dashboard.
    """

    def handle(self, session, timestamp, messages):
        rows = []
        try:
            for message in messages:
                for formatter in self.formatters:
                    row = formatter(message)
                    if row:
                        session.add(row)
            session.commit()
        except:
            session.flush()

    def cleanup(self):
        pass

    @property
    def formatters(self):
        if getattr(self, '_formatters', None):
            return self._formatters
        self._formatters = [
            getattr(self, item) for item in dir(self)
            if item.startswith('clean_')
        ]
        return self._formatters

    def clean_cloud_images_upload_base(self, message):
        if message['topic'] != 'org.fedoraproject.prod.fedimg.image.upload':
            return
        if message['msg'].get('status') != 'completed':
            return
        return self.model(
            timestamp=datetime.datetime.fromtimestamp(message['timestamp']),
            message=json.dumps({
                'name': message['msg']['image_name'],
                'ami_name': '{name}, {region}'.format(
                    name=message['msg']['extra']['id'],
                    region=message['msg']['destination']),
                'ami_link': (
                    "https://redirect.fedoraproject.org/console.aws."
                    "amazon.com/ec2/v2/home?region={region}"
                    "#LaunchInstanceWizard:ami={name}".format(
                        name=message['msg']['extra']['id'],
                        region=message['msg']['destination'])
                )
            }),
            category: 'cloud_images_upload_base'
        )
