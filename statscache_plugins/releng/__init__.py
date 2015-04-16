import json
import os.path
import pkgutil

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

    def __init__(self, config, model):
        super(Plugin, self).__init__(config, model)
        self._plugins = None
        self._plugins = self.load_plugins(config, model)

    def handle(self, session, timestamp, messages):
        rows = []
        try:
            for plugin in self._plugins:
                rows.extend(plugin,handle(session, timestamp, messages))
            # FIXME: need to write in a single db hit
            for row in rows:
                session.add(row)
            session.commit()
        except:
            session.flush()

    def cleanup(self):
        pass

    def load_plugins(self, config, model):
        if getattr(self, '_plugins', None):
            return self._plugins
        self._plugins = []
        for importer, package_name, _ in pkgutil.iter_modules(['./plugins']):
            full_package_name = '{}.{}'.format('plugins', package_name)
            module = importer.find_module(
                package_name).load_module(full_package_name)
            plugin = getattr(module, 'Plugin', None)
            if plugin and issubclass(
                    plugin, statscache.plugins.BasePlugin):
                self._plugins.append(plugin(config, model))
        return self._plugins
