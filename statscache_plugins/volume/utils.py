import copy
import collections
from statscache.frequency import Frequency
from statscache.plugins import BasePlugin, BaseModel, ScalarModel


class VolumePluginMixin(object):

    def __init__(self, *args, **kwargs):
        super(VolumePluginMixin, self).__init__(*args, **kwargs)
        self._volumes = collections.defaultdict(int)


def plugin_factory(intervals, mixin_class, class_prefix, table_prefix,
                   columns=None):
    for interval in intervals:
        # Use a dummy Frequency for pretty-printing (epoch doesn't matter)
        s = str(Frequency(interval, datetime.datetime.now()))
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
