import statscache.plugins

import re


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

    def handle(self, session, timestamp, messages):
        rows = []
        for message in messages:
            m = self.p.match(message['topic'])
            if m is None:
                continue
            topic, status = m.groups()
            if topic in self.topics:
                continue
            tokens = topic.split('.')
            agent = (len(tokens) == 6 and tokens[-1]) or 'compose'
            arch = message['msg'].get('arch') or 'primary'
            category = 'compose-{}'.format(topic)
            message = json.dumps({
                'user': message['user'],
                'status': status,
                'arch': arch,
                'agent': agent
            })
            result = session.query(self.model).filter(
                self.model.category == category)
            if result and len(result) == 1:
                row = result[0]
                row.timestamp = message['timestamp']
                row.message = message
                row.save()
            else:
                self.model(
                    timestamp=message['timestamp'],
                    message=message,
                    category='compose-{}'.format(topic)
                )
            rows.append(row)
        return rows

