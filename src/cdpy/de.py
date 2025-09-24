# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpcliWrapper

ENTITLEMENT_DISABLED='Data Engineering not enabled on CDP Tenant'


class CdpyDe(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def describe_vc(self, cluster_id, vc_id):
        return self.sdk.call(
            svc='de', func='describe_vc', ret_field='vc', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            clusterId=cluster_id,
            vcId=vc_id
        )

    def list_vcs(self, cluster_id):
         return self.sdk.call(
            svc='de', func='list_vcs', ret_field='vcs', squelch=[
                Squelch(value='NOT_FOUND', default=list()),
                Squelch(field='status_code', value='504', default=list(),
                        warning="No VCS in this Cluster"),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            clusterId=cluster_id
        )

    def create_vc(self, name, cluster_id, cpu_requests, memory_requests, chart_value_overrides=None,
                  runtime_spot_component=None, spark_version=None, acl_users=None,
                  gpu_requests=None, guaranteed_cpu_requests=None,
                  guaranteed_memory_requests=None, guaranteed_gpu_requests=None,
                  smtp_configs=None, vc_tier='CORE' ):
        return self.sdk.call(
            svc='de', func='create_vc', ret_field='Vc', squelch=[
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)    
            ],
            name=name,
            clusterId=cluster_id,
            cpuRequests=cpu_requests,
            memoryRequests=memory_requests,
            chartValueOverrides=chart_value_overrides,
            runtimeSpotComponent=runtime_spot_component,
            sparkVersion=spark_version,
            aclUsers=acl_users,
            gpuRequests=gpu_requests,
            guaranteedCpuRequests=guaranteed_cpu_requests,
            guaranteedMemoryRequests=guaranteed_memory_requests,
            guaranteedGpuRequests=guaranteed_gpu_requests,
            smtpConfigs=smtp_configs,
            vcTier=vc_tier
        )

    def delete_vc(self, cluster_id, vc_id):
        return self.sdk.call(
            svc='de', func='delete_vc', ret_field='status', squelch=[
                Squelch('NOT_FOUND'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            clusterId=cluster_id, vcId=vc_id
        )

    def describe_service(self, cluster_id):
        return self.sdk.call(
            svc='de', func='describe_service', ret_field='service', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            clusterId=cluster_id,
        )

    def list_services(self, env=None, remove_deleted=False):
        services = self.sdk.call(
            svc='de', func='list_services', ret_field='services', squelch=[
                Squelch(value='NOT_FOUND', default=list()),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED, default=list())    
            ], removeDeleted=remove_deleted
        )
        return [s for s in services if env is None or s['environmentName'] == env]


    def enable_service(self, name, env, instance_type, minimum_instances,
            maximum_instances, initial_instances=None, 
            minimum_spot_instances=None, maximum_spot_instances=None, 
            initial_spot_instances=None, chart_value_overrides=None,
            enable_public_endpoint=False, enable_private_network=False, 
            enable_workload_analytics=False, root_volume_size=None, 
            skip_validation=False, tags=None, use_ssd=None,
            loadbalancer_allowlist=None, whitelist_ips=None, subnets=None,
            cpu_requests=None, memory_requests=None, gpu_requests=None, 
            resource_pool=None, nfs_storage_class=None ):
        return self.sdk.call(
            svc='de', func='enable_service', ret_field='service', squelch=[
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)    
            ],
            name=name,
            env=env,
            instanceType=instance_type,
            minimumInstances=minimum_instances,
            maximumInstances=maximum_instances,
            initialInstances=initial_instances,
            minimumSpotInstances=minimum_spot_instances,
            maximumSpotInstances=maximum_spot_instances,
            initialSpotInstances=initial_spot_instances,
            chartValueOverrides=chart_value_overrides,
            enablePublicEndpoint=enable_public_endpoint,
            enablePrivateNetwork=enable_private_network,
            enableWorkloadAnalytics=enable_workload_analytics,
            rootVolumeSize=root_volume_size,
            skipValidation=skip_validation,
            tags=tags,
            useSsd=use_ssd,
            whitelistIps=whitelist_ips,
            loadbalancerAllowlist=loadbalancer_allowlist
            subnets=subnets,
            cpuRequests=cpu_requests,
            memoryRequests=memory_requests,
            gpuRequests=gpu_requests,
            resourcePool=resource_pool,
            nfsStorageClass=nfs_storage_class
        )

    def disable_service(self, cluster_id, force=False):
        return self.sdk.call(
            svc='de', func='disable_service', ret_field='status', squelch=[
                Squelch('NOT_FOUND'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)    
            ], 
            clusterId=cluster_id, force=force
        )

    def get_kubeconfig(self, cluster_id):
        return self.sdk.call(
            svc='de', func='get_kubeconfig', ret_field='kubeconfig', squelch=[
                Squelch('NOT_FOUND'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            clusterId=cluster_id
        )

    def get_service_id_by_name(self, name, env):
        cluster_id = None
        for service in self.list_services(env, remove_deleted=True):
            if service['name'] == name:
                cluster_id = service['clusterId']
                break
        return cluster_id

    def get_vc_id_by_name(self, name, cluster_id, remove_deleted=True):
        vc_id = None
        for vc in self.list_vcs(cluster_id):
            vc_stopped = vc['status'] in self.sdk.STOPPED_STATES
            if vc['vcName'] == name and (not vc_stopped if remove_deleted else True):
                vc_id = vc['vcId']
                break
        return vc_id
        
    def update_service(self, cluster_id, minimum_instances=None, maximum_instances=None,
            minimum_spot_instances=None, maximum_spot_instances=None,
            whitelist_ips=None, loadbalancer_allowlist=None,
            cpu_requests=None, memory_requests=None, gpu_requests=None ):
        return self.sdk.call(
            svc='de', func='update_service', ret_field='service', squelch=[
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            clusterId=cluster_id,
            minimumInstances=minimum_instances,
            maximumInstances=maximum_instances,
            minimumSpotInstances=minimum_spot_instances,
            maximumSpotInstances=maximum_spot_instances,
            whitelistIps=whitelist_ips,
            loadbalancerAllowlist=loadbalancer_allowlist,
            cpuRequests=cpu_requests,
            memoryRequests=memory_requests,
            gpuRequests=gpu_requests
        )

    def update_vc(self, cluster_id, vc_id, acl_users=None ):
        return self.sdk.call(
            svc='de', func='update_vc', ret_field='Vc', squelch=[
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            clusterId=cluster_id,
            vcId=vc_id,
            aclUsers=acl_users
        )
