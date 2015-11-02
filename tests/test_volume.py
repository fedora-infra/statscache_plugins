import unittest
import datetime
import statscache.utils
import statscache.plugins
import statscache_plugins.volume.by_category


class TestVolumePlugin(unittest.TestCase):

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
        """ Test the ability of an arbitrary volume plugin to initialize itself """
        plugin_class = list(statscache_plugins.volume.by_category.plugins)[-1]
        schedule = statscache.plugins.Schedule(interval=plugin_class.interval)
        return plugin_class(schedule, self.config)

if __name__ == '__main__':
    unittest.main()
