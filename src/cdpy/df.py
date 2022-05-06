# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpError, CdpWarning
from cdpcli.extensions.df.createdeployment import CreateDeploymentOperationCaller

ENTITLEMENT_DISABLED = 'DataFlow not enabled on CDP Tenant'

class CdpyDf(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        self.DEPLOYMENT_SIZES = ['EXTRA_SMALL', 'SMALL', 'MEDIUM', 'LARGE']
        super().__init__(*args, **kwargs)

    def list_services(self, only_enabled=False, env_crn=None, df_crn=None, name=None):
        result = self.sdk.call(
            svc='df', func='list_services', ret_field='services', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No DataFlow Services found'),
                Squelch(value='PATH_DISABLED', default=list(),
                        warning=ENTITLEMENT_DISABLED)
            ],
            pageSize=self.sdk.DEFAULT_PAGE_SIZE
        )
        if only_enabled:
            result = [x for x in result if x['status']['state'] in self.sdk.STARTED_STATES]
        if name is not None:
            result = [x for x in result if x['name'] == name]
        if df_crn is not None:
            result = [x for x in result if x['crn'] == df_crn]
        if env_crn is not None:
            result = [x for x in result if x['environmentCrn'] == env_crn]
        return result

    def describe_service(self, df_crn: str = None, env_crn: str = None):
        resolved_df_crn = None
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
        if resolved_df_crn is not None:
            return self.sdk.call(
                svc='df', func='describe_service', ret_field='service', squelch=[
                    Squelch(value='NOT_FOUND',
                            warning='No DataFlow Service with crn %s found' % df_crn),
                    Squelch(value='PATH_DISABLED',
                            warning=ENTITLEMENT_DISABLED),
                    Squelch(value='PERMISSION_DENIED')  # DF GRPC sometimes returns 403 when finishing deletion
                ],
                serviceCrn=resolved_df_crn
            )
        else:
            return None

    def resolve_service_crn_from_name(self, name, only_enabled=True):
        listing = self.list_services(only_enabled=only_enabled, name=name)
        # More than one DF Service may exist with a given name if it was previously uncleanly deleted
        if len(listing) == 1:
            return listing[0]['crn']
        elif len(listing) == 0:
            self.sdk.throw_warning(CdpWarning("No DataFlow Service found matching name %s" % name))
            return None
        else:
            self.sdk.throw_error(CdpError("Multiple DataFlow Services found matching name %s" % name))

    def enable_service(self, env_crn: str, lb_ips: list = None, min_nodes: int = 3, max_nodes: int = 3,
                       enable_public_ip: bool = True, kube_ips: list = None, cluster_subnets: list = None,
                       lb_subnets: list = None, tags: dict = None):
        self.sdk.validate_crn(env_crn)
        return self.sdk.call(
            svc='df', func='enable_service', ret_field='service',
            environmentCrn=env_crn, minK8sNodeCount=min_nodes, maxK8sNodeCount=max_nodes,
            usePublicLoadBalancer=enable_public_ip, kubeApiAuthorizedIpRanges=kube_ips,
            loadBalancerAuthorizedIpRanges=lb_ips, clusterSubnets=cluster_subnets,
            loadBalancerSubnets=lb_subnets, tags=tags
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

    def list_deployments(self, env_crn=None, df_crn=None, name=None, dep_crn=None, described=False):
        result = self.sdk.call(
            svc='df', func='list_deployments', ret_field='deployments', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No DataFlow Deployments found'),
                Squelch(value='PATH_DISABLED', default=list(),
                        warning=ENTITLEMENT_DISABLED)
            ],
            pageSize=self.sdk.DEFAULT_PAGE_SIZE
        )
        if dep_crn is not None:
            result = [x for x in result if x['crn'] == dep_crn]
        if name is not None:
            result = [x for x in result if x['name'] == name]
        if df_crn is not None:
            result = [x for x in result if x['service']['crn'] == df_crn]
        if env_crn is not None:
            result = [x for x in result if x['service']['environmentCrn'] == env_crn]
        if described is False:
            return result
        else:
            return [self.describe_deployment(dep_crn=x['crn']) for x in result]

    def describe_deployment(self, dep_crn=None, df_crn=None, name=None):
        if dep_crn is not None:
            self.sdk.validate_crn(dep_crn, 'deployment')
        elif df_crn is not None and name is not None:
            deployments = self.list_deployments(df_crn=df_crn, name=name)
            if len(deployments) == 0:
                return None
            elif len(deployments) == 1:
                dep_crn = deployments[0]['crn']
            else:
                self.sdk.throw_error(
                    CdpError('More than one DataFlow Deployment found, please try list instead')
                )
        else:
            self.sdk.throw_error(
                CdpError(
                    "Either dep_crn or both of df_crn and name must be supplied"
                )
            )
        return self.sdk.call(
            svc='df', func='describe_deployment', ret_field='deployment', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No DataFlow Deployment with crn %s found' % dep_crn),
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
            deploymentCrn=dep_crn
        )

    def list_readyflows(self, name=None):
        # Lists readyflows that can be added to the Catalog for Deployment
        result = self.sdk.call(
            svc='df', func='list_readyflows', ret_field='readyflows', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No ReadyFlows found within your CDP Tenant'),
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
        )
        if name is not None:
            result = [x for x in result if x['name'] == name]
        return result

    def list_flow_definitions(self, name=None):
        # Lists definitions in the Catalog. May contain more than one artefactType: flows, readyFlows
        result = self.sdk.call(
            svc='df', func='list_flow_definitions', ret_field='flows', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No Flow Definitions found within your CDP Tenant Catalog'),
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
        )
        if name is not None:
            result = [x for x in result if x['name'] == name]
        return result

    def describe_readyflow(self, def_crn):
        # Describes readyFlow not added to the Catalog
        self.sdk.validate_crn(def_crn, 'readyflow')
        return self.sdk.call(
            svc='df', func='describe_readyflow', ret_field='readyflowDetail', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No ReadyFlow Definition with crn %s found' % def_crn),
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
            readyflowCrn=def_crn
        )

    def import_readyflow(self, def_crn):
        # Imports a Readyflow from the Control Plane into the Tenant Flow Catalog
        self.sdk.validate_crn(def_crn, 'readyflow')
        return self.sdk.call(
            svc='df', func='add_readyflow', ret_field='addedReadyflowDetail', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No ReadyFlow Definition with crn %s found' % def_crn),
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
            readyflowCrn=def_crn
        )

    def delete_added_readyflow(self, def_crn):
        # Deletes an added Readyflow from the Tenant Flow Catalog
        self.sdk.validate_crn(def_crn, 'readyflow')
        return self.sdk.call(
            svc='df', func='delete_added_readyflow', ret_field='readyflowDetail', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No ReadyFlow Definition with crn %s found' % def_crn),
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
            readyflowCrn=def_crn
        )

    def describe_added_readyflow(self, def_crn, sort_versions=True):
        # Describes readyFlows added to the Catalog
        self.sdk.validate_crn(def_crn, 'readyflow')
        result = self.sdk.call(
            svc='df', func='describe_added_readyflow', ret_field='addedReadyflowDetail', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No ReadyFlow Definition with crn %s found' % def_crn),
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
            readyflowCrn=def_crn
        )
        # Force renaming readyflowCrn to addedReadyflowCrn to reduce user confusion
        result['addedReadyflowCrn'] = result.pop('readyflowCrn')
        out = result
        if sort_versions and out:
            out['versions'] = sorted(result['versions'], key=lambda d: d['version'], reverse=True)
        return out

    def describe_customflow(self, def_crn, sort_versions=True):
        self.sdk.validate_crn(def_crn, 'flow')
        result = self.sdk.call(
            svc='df', func='describe_flow', ret_field='flowDetail', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No Flow Definition with crn %s found' % def_crn),
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
            flowCrn=def_crn
        )
        out = result
        if sort_versions and out:
            out['versions'] = sorted(result['versions'], key=lambda d: d['version'], reverse=True)
        return out

    def import_customflow(self, def_file, name, description=None, comments=None):
        return self.sdk.call(
            svc='df', func='import_flow_definition', ret_field='.', squelch=[
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
            file=def_file,
            name=name,
            description=description,
            comments=comments
        )

    def import_customflow_version(self, def_crn, def_file, comments=None):
        self.sdk.validate_crn(def_crn, 'flow')
        return self.sdk.call(
            svc='df', func='import_flow_definition_version', ret_field='.', squelch=[
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
            flowCrn=def_crn,
            file=def_file,
            comments=comments
        )

    def delete_customflow(self, def_crn):
        self.sdk.validate_crn(def_crn, 'flow')
        return self.sdk.call(
            svc='df', func='delete_flow', ret_field='flow', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No Flow Definition with crn %s found' % def_crn),
                Squelch(value='PATH_DISABLED',
                        warning=ENTITLEMENT_DISABLED)
            ],
            flowCrn=def_crn
        )

    def get_version_crn_from_flow_definition(self, flow_name, version=None):
        summary_list = self.list_flow_definitions(name=flow_name)
        if summary_list:
            if len(summary_list) == 1:
                flow_def = summary_list[0]
                kind = flow_def['artifactType']
                if kind == 'flow':
                    detail = self.describe_customflow(flow_def['crn'])
                elif kind == 'readyFlow':
                    detail = self.describe_added_readyflow(flow_def['crn'])
                else:
                    detail = None
                    self.sdk.throw_error(CdpError("DataFlow Definition type not supported %s" % kind))
                if version is None:
                    # versions are sorted descending by default
                    return detail['versions'][0]['crn']
                else:
                    out = [x for x in detail['versions'] if x['version'] == version]
                    if out:
                        return out[0]['crn']
                    else:
                        self.sdk.throw_error(CdpError(
                            "Could not find version %d for DataFlow Definition named %s" % (version, flow_name)
                        ))
            else:
                self.sdk.throw_error(CdpError("More than one DataFlow Definition found for name %s" % flow_name))
        else:
            self.sdk.throw_warning(CdpWarning("DataFlow Definition not found for name %s" % flow_name))

    def resolve_env_crn_from_df_crn(self, df_crn):
        if ':service:' in df_crn:
            self.sdk.validate_crn(df_crn, 'df')
            df_info = self.describe_service(df_crn=df_crn)
            if df_info:
                return df_info['environmentCrn']
        elif ':deployment:' in df_crn:
            self.sdk.validate_crn(df_crn, 'deployment')
            df_info = self.describe_deployment(df_crn)
            if df_info:
                return df_info['service']['environmentCrn']
        else:
            self.sdk.throw_error(
                CdpError(
                    "Could not resolve an Environment CRN from DataFlow CRN %s" % df_crn
                )
            )

    def create_deployment(self, df_crn, flow_ver_crn, deployment_name, size_name=None, static_node_count=None,
                          autoscale_enabled=None, autoscale_nodes_min=None, autoscale_nodes_max=None, nifi_ver=None,
                          autostart_flow=None, parameter_groups=None, kpis=None):
        # Validations
        if size_name is not None and size_name not in self.DEPLOYMENT_SIZES:
            self.sdk.throw_error(CdpError("Deployment size_name %s not in supported size list: %s"
                                          % (size_name, str(self.DEPLOYMENT_SIZES))))
        self.sdk.validate_crn(df_crn, 'df')
        if self.list_deployments(name=deployment_name):
            self.sdk.throw_error(CdpError("Deployment already exists with conflicting name %s" % deployment_name))
        # Setup
        config = dict(
            autoStartFlow=autostart_flow if autostart_flow is not None else True,
            parameterGroups=parameter_groups,
            deploymentName=deployment_name,
            environmentCrn=self.resolve_env_crn_from_df_crn(df_crn),
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

    def terminate_deployment(self, dep_crn, env_crn=None):
        if env_crn is None:
            env_crn = self.resolve_env_crn_from_df_crn(df_crn=dep_crn)
        _ = [self.sdk.validate_crn(x[0], x[1]) for x in [(env_crn, 'env'), (dep_crn, 'deployment')]]
        return self.sdk.call(
            svc='dfworkload', func='terminate_deployment', ret_field='deployment',
            environmentCrn=env_crn, deploymentCrn=dep_crn
        )
