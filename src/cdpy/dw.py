# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpError


class CdpyDw(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def list_dbcs(self, cluster_id):
        return self.sdk.call(
            svc='dw', func='list_dbcs', ret_field='dbcs', squelch=[
                Squelch(value='NOT_FOUND', default=list()),
                Squelch(field='status_code', value='504', default=list(), warning="No Data Catalogs found in this Cluster"),
            ],
            clusterId=cluster_id
        )

    def list_vws(self, cluster_id):
        return self.sdk.call(
            svc='dw', func='list_vws', ret_field='vws', squelch=[
                Squelch(value='NOT_FOUND', default=list()),
                Squelch(field='status_code', value='504', default=list(), warning="No Virtual Warehouses found in this Cluster"),
            ],
            clusterId=cluster_id
        )

    def describe_cluster(self, cluster_id):
        return self.sdk.call(
            svc='dw', func='describe_cluster', ret_field='cluster', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT')
            ],
            clusterId=cluster_id
        )

    def describe_vw(self, cluster_id, vw_id):
        return self.sdk.call(
            svc='dw', func='describe_vw', ret_field='vw', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'), Squelch('UNKNOWN')
            ],
            clusterId=cluster_id,
            vwId=vw_id
        )

    def describe_dbc(self, cluster_id, dbc_id):
        return self.sdk.call(
            svc='dw', func='describe_dbc', ret_field='dbc', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'), Squelch('UNKNOWN')
            ],
            clusterId=cluster_id,
            dbcId=dbc_id
        )

    def list_clusters(self, env_crn=None):
        resp = self.sdk.call(
            svc='dw', func='list_clusters', ret_field='clusters', squelch=[
                Squelch(value='NOT_FOUND', default=list())
            ]
        )
        if env_crn:
            return [x for x in resp if env_crn == x['environmentCrn']]
        return resp

    def gather_clusters(self, env_crn=None):
        self.sdk.validate_crn(env_crn)
        clusters = self.list_clusters(env_crn=env_crn)
        out = []
        if clusters:
            for base in clusters:
                out.append({
                    'dbcs': self.list_dbcs(base['id']),
                    'vws': self.list_vws(base['id']),
                    **base
                })
        return out

    def create_cluster(self, env_crn: str, overlay: bool, aws_public_subnets: list = None,
                       aws_private_subnets: list = None, az_subnet: str = None, az_enable_az: bool = None,
                       private_load_balancer: bool = None):
        self.sdk.validate_crn(env_crn)
        if all(x is not None for x in [aws_private_subnets, aws_private_subnets]):
            aws_options = dict(publicSubnetIds=aws_public_subnets, privateSubnetIds=aws_private_subnets)
        else:
            aws_options = None
        if all(x is not None for x in [az_subnet, az_enable_az]):
            azure_options = dict(subnetId=az_subnet, enableAZ=az_enable_az)
        else:
            azure_options = None
        return self.sdk.call(
            svc='dw', func='create_cluster', ret_field='clusterId', environmentCrn=env_crn,
            useOverlayNetwork=overlay, usePrivateLoadBalancer=private_load_balancer,
            awsOptions=aws_options, azureOptions=azure_options
        )

    def delete_cluster(self, cluster_id: str, force: bool = False):
        return self.sdk.call(
            svc='dw', func='delete_cluster', squelch=[Squelch('NOT_FOUND')], clusterId=cluster_id, force=force
        )

    def create_vw(self, cluster_id:str, dbc_id:str, vw_type:str, name:str, template:str = None,
                  autoscaling_min_cluster:int = None, autoscaling_max_cluster:int = None,
                  common_configs:dict = None, application_configs:dict = None, ldap_groups:list = None,
                  enable_sso:bool = None, tags:dict = None):
        
        if any(x is not None for x in [autoscaling_min_cluster, autoscaling_max_cluster]):
            autoscaling = {}
            if autoscaling_min_cluster is not None and autoscaling_min_cluster != 0:
                autoscaling['minClusters'] = autoscaling_min_cluster
            if autoscaling_max_cluster is not None and autoscaling_max_cluster != 0:
                autoscaling['maxClusters'] = autoscaling_max_cluster
        else:
            autoscaling = None

        if tags is not None:
            tag_list = []
            for key,value in tags.items():
                tag_list.append({'key': key, 'value': value})
        else:
            tag_list = None

        if any(x is not None for x in [common_configs, application_configs, ldap_groups, enable_sso]):
            config = {}
            if common_configs is not None and common_configs:
                config['commonConfigs'] = common_configs
            if application_configs is not None and application_configs:
                config['applicationConfigs'] = application_configs
            if ldap_groups is not None and ldap_groups:
                config['ldapGroups'] = ldap_groups
            if enable_sso is not None:
                config['enableSSO'] = enable_sso
        else:
            config = None

        return self.sdk.call(
            svc='dw', func='create_vw', ret_field='vwId', clusterId=cluster_id, dbcId=dbc_id,
            vwType=vw_type, name=name, template=template, autoscaling=autoscaling, config=config,
            tags=tag_list
        )

    def delete_vw(self, cluster_id:str, vw_id:str):
        return self.sdk.call(
            svc='dw', func='delete_vw', squelch=[Squelch('NOT_FOUND')], clusterId=cluster_id, vwId=vw_id
        )
        
    def create_dbc(self, cluster_id:str, name:str, load_demo_data: bool = None):
        return self.sdk.call(
            svc='dw', func='create_dbc', ret_field='dbcId', clusterId = cluster_id, name=name,
            loadDemoData = load_demo_data
        )

    def delete_dbc(self, cluster_id:str, dbc_id:str):
        return self.sdk.call(
            svc='dw', func='delete_dbc', squelch=[Squelch('NOT_FOUND')], clusterId=cluster_id, dbcId=dbc_id
        )
