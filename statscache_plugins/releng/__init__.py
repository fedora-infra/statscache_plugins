import logging
import os.path
import pkgutil

import statscache.plugins

log = logging.getLogger('statscache')


class Plugin(statscache.plugins.BasePlugin):
    name = "Release engineering event logs"
    summary = "Release engineering event logs, organized by category."
    description = """
    Recent release engineering event logs to be used for rendering
    release engineering dashboard.
    """
    datagrepper_endpoint = 'https://apps.fedoraproject.org/datagrepper/raw/'

    def __init__(self, config):
        super(Plugin, self).__init__(config)
        self._plugins = None
        self._plugins = self.load_plugins(config, self.model)

    def make_model(self):
        class Result(statscache.plugins.ConstrainedCategorizedLogModel):
            __tablename__ = 'data_releng_dashboard'

        return Result

    @property
    def layout(self):
        # why not: layout = { ... }
        return {
            'groups': [
                {
                    'id': 'compose',
                    'components': [
                        {
                            'id': 'compose-rawhide_primary',
                            'name': 'Rawhide Compose (Primary)',
                        },
                        {
                            'id': 'compose-rawhide_arm',
                            'name': 'Rawhide Compose (arm)',
                        },
                        {
                            'id': 'compose-rawhide_ppc',
                            'name': 'Rawhide Compose (ppc)',
                        },
                        {
                            'id': 'compose-rawhide_s390',
                            'name': 'Rawhide Compose (s390)',
                        }
                    ]
                },
                {
                    'id': 'updates',
                    'components': [
                        {
                            'id': 'updates-fedora',
                            'name': 'Repo Updates (Fedora)',
                        },
                        {
                            'id': 'updates-epel',
                            'name': 'Repo Updates (Fedora)',
                        }
                    ]
                },
                {
                    'id': 'amis',
                    'components': [
                        {
                            'id': 'ami-branched',
                            'name': 'AMIs (branched)',
                            'link_for': 'name',
                            'show_time_at': False
                        },
                        {
                            'id': 'ami-rawhide',
                            'name': 'AMIs (rawhide)',
                            'link_for': 'message',
                            'show_time_at': False
                        }
                    ]
                },
                {
                    'id': 'appliance',
                    'name': 'Appliance builds (rawhide)',
                    'components': [
                        {
                            'id': 'artifact-appliance_armhfp',
                            'name': 'armhfp',
                            'show_status': False,
                            'show_time_at': False
                        }
                    ]
                },
                {
                    'id': 'livecd',
                    'name': 'LiveCD builds (rawhide)',
                    'components': [
                        {
                            'id': 'artifact-livecd_x86_64',
                            'name': 'x86_64',
                            'show_time_at': False
                        },
                        {
                            'id': 'artifact-livecd_i686',
                            'name': 'i686',
                            'show_time_at': False
                        }
                    ]
                },
            ],
            'default_options': {
                'show_time_at': True,
                'show_status': True,
                'status_verb_map': {
                    'complete': 'completed',
                    'start': 'started',
                    'open': 'opened',
                    'close': 'closed'
                },
                'status_class_map': {
                    'complete': 'text-primary',
                    'start': 'text-warning',
                    'failed': 'text-danger',
                    'closed': 'text-primary',
                    'open': 'text-warning',
                    'closed': 'text-primary',
                    'opened': 'text-warning',
                    'failed': 'text-danger'
                },
                'old_log_threshold': {
                    'key': 'hours',
                    'value': 24
                }
            }
        }

    def handle(self, session, timestamp, messages):
        rows = []
        try:
            for plugin in self._plugins:
                try:
                    rows.extend(plugin.handle(session, timestamp, messages) or [])
                except Exception as e:
                    log.exception(
                        "Error in releng plugin '{}': {}".format(
                            plugin.ident, e), exc_info=True)
            # FIXME: need to write in a single db hit
            session.add_all(rows)
            session.commit()
        except Exception as e:
            log.exception(
                "Error in releng plugin: {}".format(e), exc_info=True)
            session.flush()

    def initialize(self, session):
        for plugin in self._plugins:
            if getattr(plugin, 'initialize', None) is None:
                continue
            try:
                plugin.initialize(session, self.datagrepper_endpoint)
            except Exception as e:
                log.exception(
                    "Error during initializing releng plugin '{}': {}".format(
                        plugin.ident, e), exc_info=True)

    def cleanup(self):
        pass

    def load_plugins(self, config, model):
        # TODO: plugins for a plugin? This shoudln't be necessary
        if getattr(self, '_plugins', None):
            return self._plugins
        self._plugins = []
        for importer, package_name, _ in pkgutil.iter_modules(
                [os.path.join(__path__[0], 'plugins')]):
            full_package_name = '{}.{}'.format('plugins', package_name)
            module = importer.find_module(
                package_name).load_module(full_package_name)
            plugin = getattr(module, 'Plugin', None)
            if plugin and issubclass(plugin,
                                    statscache.plugins.BasePlugin):
                log.info("Loading plugin %r" % plugin)
                self._plugins.append(plugin(config, model))
            else:
                log.info("Not loading %r.  Not a plugin." % plugin)
        return self._plugins
