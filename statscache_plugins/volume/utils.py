import datetime
import requests
import collections
import copy
from statscache.frequency import Frequency
from statscache.plugins import BasePlugin, BaseModel, ScalarModel


class VolumePluginMixin(object):

    def __init__(self, *args, **kwargs):
        super(VolumePluginMixin, self).__init__(*args, **kwargs)
        self._volumes = collections.defaultdict(int)

    def initialize(self, session, datagrepper_endpoint=None):
        latest = session.query(self.model).order_by(
            self.model.timestamp.desc()).first()
        delta = 2000000
        if latest:
            latest.volume = 0
            session.add(latest)
            session.commit()
            delta = int(
                (datetime.datetime.now() -
                 self.frequency.last(now=latest.timestamp)).total_seconds()
            )
        resp = requests.get(
            self.datagrepper_endpoint,
            params={
                'delta': delta,
                'rows_per_page': 100
            }
        )
        map(self.process, resp.json().get('raw_messages', []))
        self.update(session)


def plugin_factory(intervals, mixin_class, class_prefix, table_prefix,
                   columns=None):
    for interval in intervals:
        s = str(Frequency(interval)) # pretty-print timedelta
        plugin = type(
            s.join([class_prefix, "Plugin"]),
            (mixin_class, BasePlugin),
            { 'interval': interval }
        )
        attributes = { '__tablename__': table_prefix + s }
        attributes.update(copy.deepcopy(columns or {}))
        plugin.model = type(
            s.join([class_prefix, "Model"]),
            (BaseModel if columns is not None else ScalarModel,),
            attributes
        )
        yield plugin
