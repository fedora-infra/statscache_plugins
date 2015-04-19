import statscache.plugins


class Plugin(statscache.plugins.BasePlugin):
    name = "releng, artifacts"
    summary = "Build logs for successful upload/test of cloud images"
    description = """
    Latest build logs for successful upload/test of cloud images
    """
    artifacts = ('appliance', 'livecd') 

    def handle(self, session, timestamp, messages):
        rows = []
        for message in messages:
            if not (message['msg']['owner'] == 'masher' and
                    message['msg']['method'] in self.artifacts):
                continue
            artifact = method['msg']['method']
            srpm_name, srpm_link = self.get_srpm_details(message['msg'])
            # FIXME: Have to include logic for overwriting older log
            rows.append(
                self.model(
                    timestamp=message['timestamp'],
                    message=json.dumps({
                        'name': message['msg']['image_name'],
                        'srpm_name': srpm_name,
                        'srpm_link': srpm_link,
                        'status': message['msg']['new'],
                        'details_link': message['msg']['meta']['link']
                    }),
                    category='artifact-{}'.format(artifact)
                )
            )
        return rows

    def get_srpm_details(self, msg):
        tokens = msg['srpm'].split('-')
        arch = tokens[-1]
        info = msg.get('info')
        srpm = msg['srpm']
        if isinstance(info, dict):
            options = info['request'][-1];
            if options.get('format'):
                srpm = '{} ({})'format(
                    srpm, options['format'])
            branch = info['request'][1]
            if branch != 'rawhide':
                branch = 'branched'
            if branch:
                srpm = '{} ({})'.format(srpm, branch)
        # Try to generate SRPM link
        srpm_link = None
        children = info.get('children')
        if (msg.new == 'CLOSED' and isinstance(children, list) and
                len(children) == 1):
            id = children[0]['id']
            result = info['result'].split(' ')[-1]
            tokens = result.split('/')
            thing = token[5]
            r = info['request']
            opts = r[5]
            filename = '{}-{}-{}'.format(r[0], r[1], opts['release'])

            if opts.get('format') is None:
                filename += '.iso'
            elif opts['format'] == 'qcow2':
                filename += '-sda.qcow2'
            elif opts['format'] == 'raw':
                filename += '-sda.raw.xz'
            base = "https://kojipkgs.fedoraproject.org/work/tasks/"
            srpm_link = '{}{}/{}/{}'.format(base, thing, id, file)
        return srpm, srpm_link
