# -*- coding: utf-8 -*-

from cdpy.common import CdpError, CdpWarning, CdpSdkBase, Squelch


class CdpyDe(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def describe_vc(self, cluster_id, vc_id):
        return self.sdk.call(
            svc='de', func='describe_vc', ret_field='vc', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT')
            ],
            clusterId=cluster_id,
            vcId=vc_id
        )

    def list_vcs(self, cluster_id):
         return self.sdk.call(
            svc='de', func='list_vcs', ret_field='vcs', squelch=[
                Squelch(value='NOT_FOUND', default=list()),
                Squelch(field='status_code', value='504', default=list(), warning="No VCS in this Cluster"),
            ],
            clusterId=cluster_id
        )

    def create_vc(self, name, cluster_id):
        return self.sdk.call(
            svc='de', func='create_vc', ret_field='vc',
            vcName=name,
            clusterId=cluster_id
            # TODO: need to define cpu and mem requests
        )

    def delete_vc(self, cluster_id, vc_id):
        return self.sdk.call(
            svc='de', func='delete_vc', squelch=[Squelch('NOT_FOUND')], clusterId=cluster_id, vcId = vc_id
        )

    def describe_service(self, cluster_id):
        return self.sdk.call(
            svc='de', func='describe_service', ret_field='service', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT')
            ],
            clusterId=cluster_id,
        )

    def list_services(self):
        return self.sdk.call(
            svc='de', func='list_services', ret_field='clusters', squelch=[
                Squelch(value='NOT_FOUND', default=list())
            ]
        )

    def enable_service(self, name, env: str = None, instance_type: str = None, min_instances: str = None, max_instances: str = None):
        return self.sdk.call(
            svc='de', func='enable_service', ret_field='service', squelch=[
                Squelch(value='NOT_FOUND', default=list())
            ],
            name=name,
            env=env,
            instance-type=instance_type,
            minimum-instances=min_instances,
            maximum-instances=max_instances
        )

    def disable_service(self, cluster_id):
        return self.sdk.call(
            svc='de', func='disable_service', ret_field='status', squelch=[Squelch('NOT_FOUND')], cluster-id=cluster_id
        )

    def get_kubeconfig(self, cluster_id):
        return self.sdk.call(
            svc='de', func='get_kubeconfig', ret_field='kubeconfig', squelch=[Squelch('NOT_FOUND')], cluster-id=cluster_id
        )
