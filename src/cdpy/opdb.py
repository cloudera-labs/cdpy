# -*- coding: utf-8 -*-
import array

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

    def create_database(self, name: str, env: str, disable_ephemeral_storage: bool = False,
                        disable_jwt_auth: bool = False, auto_scaling_params: dict = None,
                        dns_forward_domain: str = None, dns_forward_ns_ip: str = None,
                        subnet_id: str = None, use_hdfs: bool = False, disable_multi_az: bool = False,
                        disable_kerberos: bool = False, num_edge_nodes: int = 0, image: dict = None,
                        enable_region_canary: bool = False,  master_node_type: str = None,
                        gateway_node_type: str = None, custom_user_tags: array = None,
                        attached_storage_for_workers: dict = None):
        return self.sdk.call(
            svc='opdb', func='create_database', ret_field='databaseDetails',
            squelch=[
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)  
            ],
            databaseName=name,
            environmentName=env,
            disableEphemeralStorage=disable_ephemeral_storage,
            disableJwtAuth=disable_jwt_auth,
            autoScalingParameters=auto_scaling_params,
            dnsForwardDomain=dns_forward_domain,
            dnsForwardNsIp=dns_forward_ns_ip,
            enableRegionCanary=enable_region_canary,
            useHdfs=use_hdfs,
            subnetId=subnet_id,
            disableMultiAz=disable_multi_az,
            disableKerberos=disable_kerberos,
            numEdgeNodes=num_edge_nodes,
            image=image,
            masterNodeType=master_node_type,
            gatewayNodeType=gateway_node_type,
            customUserTags=custom_user_tags,
            attachedStorageForWorkers=attached_storage_for_workers,
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