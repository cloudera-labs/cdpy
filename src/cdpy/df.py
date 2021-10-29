# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpError
from cdpcli.extensions.df.createdeployment import CreateDeploymentOperationCaller


class CdpyDf(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        self.DEPLOYMENT_SIZES = ['EXTRA_SMALL', 'SMALL', 'MEDIUM', 'LARGE']
        super().__init__(*args, **kwargs)

    def list_services(self, only_enabled=False, env_crn=None, df_crn=None, name=None):
        result = self.sdk.call(
            svc='df', func='list_services', ret_field='services', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No DataFlow Services found')
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
                resolved_df_crn = None
                self.sdk.throw_error(
                    CdpError('More than one DataFlow service found for env_crn, please try list instead')
                )
        else:
            resolved_df_crn = None
            self.sdk.throw_error(CdpError("Either df_crn or env_crn must be supplied to df.describe_service"))
        return self.sdk.call(
            svc='df', func='describe_service', ret_field='service', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No DataFlow Service with crn %s found' % df_crn),
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

    def list_deployments(self, env_crn=None, df_crn=None, name=None):
        result = self.sdk.call(
            svc='df', func='list_deployments', ret_field='deployments', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No DataFlow Deployments found')
            ],
            pageSize=self.sdk.DEFAULT_PAGE_SIZE
        )
        if name is not None:
            result = [x for x in result if x['name'] == name]
        if df_crn is not None:
            result = [x for x in result if x['service']['crn'] == df_crn]
        if env_crn is not None:
            result = [x for x in result if x['service']['environmentCrn'] == env_crn]
        return result

    def describe_deployment(self, dep_crn):
        self.sdk.validate_crn(dep_crn)
        return self.sdk.call(
            svc='df', func='describe_deployment', ret_field='deployment', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No DataFlow Deployment with crn %s found' % dep_crn)
            ],
            deploymentCrn=dep_crn
        )

    def list_readyflows(self, name=None):
        result = self.sdk.call(
            svc='df', func='list_readyflows', ret_field='readyflows', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No ReadyFlows found within your CDP Tenant')
            ],
        )
        if name is not None:
            result = [x for x in result if x['name'] == name]
        return result

    def resolve_environment_from_dataflow(self, df_crn):
        self.sdk.validate_crn(df_crn, 'df')
        df_info = self.describe_service(df_crn=df_crn)
        if df_info:
            return df_info['environmentCrn']
        else:
            self.sdk.throw_error(CdpError("Could not resolve an Environment CRN from DataFlow CRN %s" % df_crn))

    def create_deployment(self, df_crn, flow_ver_crn, deployment_name, size_name=None, static_node_count=None,
                          autoscale_enabled=None, autoscale_nodes_min=None, autoscale_nodes_max=None, nifi_ver=None,
                          autostart_flow=None, parameters=None, kpis=None):
        # Validations
        if size_name is not None and size_name not in self.DEPLOYMENT_SIZES:
            self.sdk.throw_error(CdpError("Deployment size_name %s not in supported size list: %s"
                                          % (size_name, str(self.DEPLOYMENT_SIZES))))
        _ = [self.sdk.validate_crn(x[0], x[1]) for x in [(df_crn, 'df'), (flow_ver_crn, 'flow')]]
        if self.list_deployments(name=deployment_name):
            self.sdk.throw_error(CdpError("Deployment already exists with conflicting name %s" % deployment_name))
        # Setup
        config = dict(
            autoStartFlow=autostart_flow if autostart_flow is not None else True,
            parameterGroups=parameters,
            deploymentName=deployment_name,
            environmentCrn=self.resolve_environment_from_dataflow(df_crn),
            clusterSizeName=size_name if size_name is not None else 'EXTRA_SMALL',
            cfmNifiVersion=nifi_ver,
            kpis=kpis
        )
        if autoscale_enabled:
            config['autoScalingEnabled'] = True
            config['autoScaleMinNodes'] = autoscale_nodes_min if autoscale_nodes_min is not None else 1
            config['autoScaleMaxNodes'] = autoscale_nodes_max if autoscale_nodes_max is not None else 3
        else:
            config['staticNodeCount'] = static_node_count if static_node_count is not None else 1

        # cdpcli/extensions/df/createdeployment.py  cdpcli-beta v0.9.48+
        dep_req_crn = self.sdk.call(
            svc='df', func='initiate_deployment', ret_field='deploymentRequestCrn',
            serviceCrn=df_crn, flowVersionCrn=flow_ver_crn
        )
        df_handler = CreateDeploymentOperationCaller()
        df_handler._upload_assets(
            df_workload_client=self.sdk._client(
                service='dfworkload',
                parameters=config
            ),
            deployment_request_crn=dep_req_crn,
            parameters=config
        )
        resp = df_handler._create_deployment(
            df_workload_client=self.sdk._client(
                service='dfworkload',
                parameters=config
            ),
            deployment_request_crn=dep_req_crn,
            environment_crn=config['environmentCrn'],
            parameters=config
        )
        return resp

    def terminate_deployment(self, env_crn, dep_crn):
        _ = [self.sdk.validate_crn(x[0], x[1]) for x in [(env_crn, 'env'), (dep_crn, 'deployment')]]
        return self.sdk.call(
            svc='dfworkload', func='terminate_deployment', ret_field='deployment',
            environmentCrn=env_crn, deploymentCrn=dep_crn
        )
