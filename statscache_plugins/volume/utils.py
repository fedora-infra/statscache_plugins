import copy
import datetime
import collections
from statscache.schedule import Schedule
from statscache.plugins import BasePlugin, BaseModel, ScalarModel


class VolumePluginMixin(object):

    def __init__(self, *args, **kwargs):
        super(VolumePluginMixin, self).__init__(*args, **kwargs)
        self._volumes = collections.defaultdict(int)


def plugin_factory(intervals, mixin_class, class_prefix, table_prefix,
                   columns=None):
    for interval in intervals:
        # Use a dummy Schedule for pretty-printing (epoch doesn't matter)
        sched = str(Schedule(interval, datetime.datetime.now()))
        plugin = type(
            sched.join([class_prefix, "Plugin"]),
            (mixin_class, BasePlugin),
            { 'interval': interval }
        )
        attributes = { '__tablename__': table_prefix + sched }
        attributes.update(copy.deepcopy(columns or {}))
        plugin.model = type(
            sched.join([class_prefix, "Model"]),
            (BaseModel if columns is not None else ScalarModel,),
            attributes
        )
        yield plugin
