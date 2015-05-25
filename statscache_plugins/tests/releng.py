import unittest
import statscache.plugins
import statscache_plugins.releng


class TestRelengPlugin(unittest.TestCase):

    def setUp(self):
        self.config = {
            # Consumer stuff
            "statscache.consumer.enabled": True,
            "statscache.sqlalchemy.uri": "sqlite:////var/tmp/statscache-dev-db.sqlite",
        }
        self.model = statscache_plugins.releng.make_model(60)

    def _make_session(self):
        uri = self.config['statscache.sqlalchemy.uri']
        statscache.plugins.create_tables(uri)
        return statscache.plugins.init_model(uri)

    def test_init(self):
        session = self._make_session()
        plugin = statscache_plugins.releng.Plugin(self.config, self.model)
        plugin.initialize(session)
