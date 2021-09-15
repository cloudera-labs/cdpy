# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpError


class CdpyDf(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def list_services(self, only_enabled=False, env_crn=None, df_crn=None, name=None):
        result = self.sdk.call(
            svc='df', func='list_services', ret_field='services', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No DataFlow Deployments found')
            ],
            pageSize=self.sdk.DEFAULT_PAGE_SIZE
        )
        if only_enabled:
            result = [x for x in result if x['status']['state'] not in ['NOT_ENABLED']]
        if name is not None:
            result = [x for x in result if x['name'] == name]
        if df_crn is not None:
            result = [x for x in result if x['crn'] == df_crn]
        if env_crn is not None:
            result = [x for x in result if x['environmentCrn'] == env_crn]
        return result

    def describe_service(self, df_crn: str = None, env_crn: str = None):
        if df_crn is not None:
            resolved_df_crn = df_crn
        elif env_crn is not None:
            services = self.list_services(env_crn=env_crn)
            if len(services) == 0:
                return None
            elif len(services) == 1:
                resolved_df_crn = services[0]['crn']
            else:
                self.sdk.throw_error(
                    CdpError('More than one DataFlow service found for env_crn, please try list instead')
                )
        else:
            self.sdk.throw_error(CdpError("Either df_crn or env_crn must be supplied to df.describe_service"))
        return self.sdk.call(
            svc='df', func='describe_service', ret_field='service', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No DataFlow Deployment with crn %s found' % df_crn),
                Squelch(value='PERMISSION_DENIED')  # DF GRPC sometimes returns 403 when finishing deletion
            ],
            serviceCrn=resolved_df_crn
        )

    def enable_service(self, env_crn: str, lb_ips: list = None, min_nodes: int = 3, max_nodes: int = 3,
                       enable_public_ip: bool = True, kube_ips: list = None, cluster_subnets: list = None,
                       lb_subnets: list = None):
        self.sdk.validate_crn(env_crn)
        return self.sdk.call(
            svc='df', func='enable_service', ret_field='service',
            environmentCrn=env_crn, minK8sNodeCount=min_nodes, maxK8sNodeCount=max_nodes,
            usePublicLoadBalancer=enable_public_ip, kubeApiAuthorizedIpRanges=kube_ips,
            loadBalancerAuthorizedIpRanges=lb_ips, clusterSubnets=cluster_subnets, loadBalancerSubnets=lb_subnets
        )

    def disable_service(self, df_crn: str, persist: bool = False, terminate=False):
        self.sdk.validate_crn(df_crn)
        return self.sdk.call(
            svc='df', func='disable_service', ret_field='status', ret_error=True,
            serviceCrn=df_crn, persist=persist, terminateDeployments=terminate
        )

    def reset_service(self, df_crn: str):
        self.sdk.validate_crn(df_crn)
        return self.sdk.call(
            svc='df', func='reset_service',
            serviceCrn=df_crn
        )
