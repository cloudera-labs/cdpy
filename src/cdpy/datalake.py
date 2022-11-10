# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch


class CdpyDatalake(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def list_datalakes(self, name=None):
        return self.sdk.call(
            svc='datalake', func='list_datalakes', ret_field='datalakes', squelch=[Squelch('NOT_FOUND')],
            environmentName=name
        )

    def is_datalake_running(self, environment_name):
        resp = self.list_datalakes(environment_name)
        if resp and len(resp) == 1:
            if resp[0]['status'] in self.sdk.STARTED_STATES:
                return True
        return False

    def describe_datalake(self, name):
        return self.sdk.call(
            svc='datalake', func='describe_datalake', ret_field='datalake',
            squelch=[Squelch('NOT_FOUND'), Squelch('UNKNOWN')
                     ],
            datalakeName=name
        )

    def delete_datalake(self, name, force=False):
        return self.sdk.call(
            svc='datalake', func='delete_datalake', ret_field='datalake', squelch=[Squelch('NOT_FOUND')],
            datalakeName=name, force=force
        )

    def describe_all_datalakes(self, environment_name=None):
        datalakes_listing = self.list_datalakes(environment_name)
        if datalakes_listing:
            return [self.describe_datalake(datalake['datalakeName']) for datalake in datalakes_listing]
        return datalakes_listing
