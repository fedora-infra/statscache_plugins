import datetime
import requests
import copy
from statscache.frequency import Frequency
from statscache.plugins import BasePlugin, BaseModel, ScalarModel


class VolumePluginMixin(object):

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
        self.handle(session, resp.json().get('raw_messages', []))


def plugin_factory(intervals, plugin_mixin_class, class_prefix, table_prefix,
                   columns=None):
    for interval in intervals:
        s = str(Frequency(interval)) # pretty-print timedelta
        class PluginAnon(plugin_mixin_class, BasePlugin):
            pass
        PluginAnon.__name__ = s.join([class_prefix, "Plugin"])
        PluginAnon.interval = interval
        modelAttributes = { '__tablename__': table_prefix + s }
        modelAttributes.update(copy.deepcopy(columns or {}))
        PluginAnon.model = type(
            s.join([class_prefix, "Model"]),
            (BaseModel if columns is not None else ScalarModel,),
            modelAttributes
        )
        yield PluginAnon
