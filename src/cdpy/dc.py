# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpError

ENTITLEMENT_DISABLED='Data Catalog not enabled on CDP Tenant'


class CdpyDc(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def list_dbcs(self, cluster_id):
        return self.sdk.call(
            svc='dw', func='list_dbcs', ret_field='dbcs', squelch=[
                Squelch(value='NOT_FOUND', default=list()),
                Squelch(field='status_code', value='504', default=list(), warning="No Data Catalogs found in this Cluster"),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED, default=list())
            ],
            clusterId=cluster_id
        )