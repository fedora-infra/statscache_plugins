import unittest
import datetime
import statscache.utils
import statscache.plugins
import statscache_plugins.releng


class TestRelengPlugin(unittest.TestCase):

    def setUp(self):
        self.config = {
            # Consumer stuff
            "statscache.consumer.enabled": True,
            "statscache.sqlalchemy.uri": "sqlite:////var/tmp/statscache-dev-db.sqlite",
        }

    def _make_session(self):
        uri = self.config['statscache.sqlalchemy.uri']
        statscache.utils.create_tables(uri)
        return statscache.utils.init_model(uri)

    def test_init(self):
        """ Test the ability of the releng plugin(s) to initialize """
        return statscache_plugins.releng.Plugin(None, self.config)

if __name__ == '__main__':
    unittest.main()
