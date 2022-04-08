# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch

ENTITLEMENT_DISABLED='Operational Database not enabled on CDP Tenant'


class CdpyOpdb(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def describe_database(self, name=None, env=None):
        return self.sdk.call(
            svc='opdb', func='describe_database', ret_field='databaseDetails', squelch=[
                Squelch('NOT_FOUND'),
                Squelch('INVALID_ARGUMENT'),
                Squelch('UNKNOWN'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            databaseName=name,
            environmentName=env,
        )

    def list_databases(self, env=None):
        return self.sdk.call(
            svc='opdb', func='list_databases', ret_field='databases', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No OpDB Databases found in Tenant'),
                Squelch(value='PATH_DISABLED', default=list(),
                        warning=ENTITLEMENT_DISABLED)
            ],
            environmentName=env
        )

    def describe_all_databases(self, env=None):
        ws_list = self.list_databases(env)
        resp = []
        for db in ws_list:
            db_desc = self.describe_database(db['databaseName'], db['environmentCrn'])
            if db_desc is not None:
                resp.append(db_desc)
        return resp

    def drop_database(self, name, env):
        return self.sdk.call(
            svc='opdb', func='drop_database', ret_field='status', squelch=[
                Squelch('NOT_FOUND'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            databaseName=name,
            environmentName=env,
        )

    def create_database(self, name, env):
        return self.sdk.call(
            svc='opdb', func='create_database', ret_field='databaseDetails',
            squelch=[
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)  
            ],
            databaseName=name,
            environmentName=env,
        )

    def start_database(self, name, env):
        return self.sdk.call(
            svc='opdb', func='start_database', squelch=[
                Squelch('NOT_FOUND'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            databaseName=name,
            environmentName=env,
        )

    def stop_database(self, name, env):
        return self.sdk.call(
            svc='opdb', func='stop_database', squelch=[
                Squelch('NOT_FOUND'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            databaseName=name,
            environmentName=env,
        )