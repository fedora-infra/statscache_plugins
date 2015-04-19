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

    def handle(self, session, timestamp, messages):
        rows = []
        for message in messages:
            if not (message['topic'] in self.topics and
                    message['msg'].get('status') == 'completed'):
                continue
            rows.append(
                self.model(
                    timestamp=message['timestamp'],
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
                    category='ami-{}'.format(message['topic'].split('.')[-1])
                )
            )
        return rows

