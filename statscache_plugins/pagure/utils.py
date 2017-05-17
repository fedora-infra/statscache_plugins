import copy
import datetime
import collections
from statscache.plugins import BasePlugin, BaseModel, Schedule


class PagurePluginMixin(object):
    """
    Consolidates common logic among the various volume plugins, which need
    to define a list of strings '_keys' which must correspond to the names of
    the columns by which volume is being tracked. The order that they are given
    must also match the order that the values are provided in the tuple keys to
    the '_volumes' defaultdict.
    """

    def __init__(self, *args, **kwargs):
        super(PagurePluginMixin, self).__init__(*args, **kwargs)
        self._volumes = collections.defaultdict(int)

    def update(self, session):
        for key, volume in self._volumes.items():
            keys_to_values = dict(zip(self._keys, list(key)))
            row = session.query(self.model)\
                .filter_by(**keys_to_values)\
                .first()
            if row:
                row.volume += volume
            else:
                row = self.model(volume=volume, **keys_to_values)
            session.add(row)
        session.commit()
        self._volumes.clear()


def plugin_factory(intervals, mixin_class, class_prefix, table_prefix,
                   columns):
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
            (BaseModel,),
            attributes
        )
        yield plugin
