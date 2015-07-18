import datetime
import requests


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
