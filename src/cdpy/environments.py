# -*- coding: utf-8 -*-

from typing import Union
from cdpy.common import CdpSdkBase, Squelch, CdpError, CdpWarning


class CdpyEnvironments(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_credential_prerequisites(self, cloud: str):
        return self.sdk.call(svc='environments', func='get_credential_prerequisites', cloudPlatform=cloud.upper())

    def describe_proxy_config(self, name):
        resp = self.list_proxy_configs(name)
        return self.sdk.first_item_if_exists(resp)

    def create_proxy_config(self, proxyConfigName, host=None, port=None, protocol=None, description=None,
                            noProxyHosts=None, user=None, password=None):

        return self.sdk.call(
            svc='environments', func='create_proxy_config', ret_field='proxyConfig',
            proxyConfigName=proxyConfigName, host=host, port=port,
            protocol=protocol, description=description, noProxyHosts=noProxyHosts, user=user, password=password
        )

    def delete_proxy_config(self, name):
        return self.sdk.call(
            svc='environments', func='delete_proxy_config', ret_field='credentials', squelch=[Squelch('NOT_FOUND')],
            proxyConfigName=name
        )

    def list_proxy_configs(self, name=None):
        return self.sdk.call(
            svc='environments', func='list_proxy_configs', ret_field='proxyConfigs', squelch=[
                Squelch('NOT_FOUND', default=list())
            ],
            proxyConfigName=name
        )

    def get_id_broker_mapping_sync(self, name):
        return self.sdk.call(
            svc='environments', func='get_id_broker_mappings_sync_status', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT')
            ],
            environmentName=name
        )

    def get_id_broker_mappings(self, name):
        return self.sdk.call(
            svc='environments', func='get_id_broker_mappings', squelch=[Squelch('NOT_FOUND')],
            environmentName=name
        )

    def describe_environment(self, name):
        resp = self.sdk.call(
            svc='environments', func='describe_environment', ret_field='environment', ret_error=True,
            environmentName=name
        )
        if isinstance(resp, CdpError):
            if resp.error_code == 'NOT_FOUND':
                # Describe will fail in certain fault scenarios early in Environment creation
                # We helpfully provide the summary listing as a backup set of information
                # If the environment truly does not exist, then this will give the same response
                self.sdk.throw_warning(CdpWarning(str(resp.violations)))
                return self.summarize_environment(name)
            self.sdk.throw_error(resp)
        return resp

    def describe_all_environments(self):
        envs_listing = self.list_environments()
        if envs_listing is not None:
            return [self.describe_environment(env['environmentName']) for env in envs_listing]
        else:
            return list()

    def summarize_environment(self, name):
        result = self.list_environments()
        if result:
            for env in result:
                if env['environmentName'] == name:
                    return env
        return None

    def gather_idbroker_mappings(self, name):
        results = dict()
        mappings = self.get_id_broker_mappings(name)
        if mappings is not None:
            results.update(**mappings)
        broker_sync_status = self.get_id_broker_mapping_sync(name)
        if broker_sync_status is not None:
            results.update(syncStatus=broker_sync_status)
        return results

    def list_environments(self):
        return self.sdk.call(
            svc='environments', func='list_environments', ret_field='environments', squelch=[
                Squelch('NOT_FOUND', default=list(), warning='No Environments found in CDP Tenant')]
        )

    def create_aws_environment(self, **kwargs):
        # TODO: Rework with named kwargs
        resp = self.sdk.call(
            svc='environments', func='create_aws_environment', ret_field='environment', ret_error=True,
            **kwargs
        )
        if isinstance(resp, CdpError):
            if resp.error_code == 'INVALID_ARGUMENT':
                if 'constraintViolations' not in str(resp.violations):
                    resp.update(message="Received violation warning:\n%s" % self.sdk.dumps(str(resp.violations)))
                    self.sdk.throw_warning(CdpWarning(str(resp.violations)))
            self.sdk.throw_error(resp)
        return resp

    def create_azure_environment(self, **kwargs):
        # TODO: Rework with named kwargs
        resp = self.sdk.call(
            svc='environments', func='create_azure_environment', ret_field='environment', ret_error=True,
            **kwargs
        )
        if isinstance(resp, CdpError):
            if resp.error_code == 'INVALID_ARGUMENT':
                if 'constraintViolations' not in str(resp.violations):
                    resp.update(message="Received violation warning:\n%s" % self.sdk.dumps(str(resp.violations)))
                    self.sdk.throw_warning(CdpWarning(str(resp.violations)))
            self.sdk.throw_error(resp)
        return resp

    def create_gcp_environment(self, **kwargs):
        # TODO: Rework with named kwargs
        resp = self.sdk.call(
            svc='environments', func='create_gcp_environment', ret_field='environment', ret_error=True,
            **kwargs
        )
        if isinstance(resp, CdpError):
            if resp.error_code == 'INVALID_ARGUMENT':
                if 'constraintViolations' not in str(resp.violations):
                    resp.update(message="Received violation warning:\n%s" % self.sdk.dumps(str(resp.violations)))
                    self.sdk.throw_warning(CdpWarning(str(resp.violations)))
            self.sdk.throw_error(resp)
        return resp

    def create_private_environment(self, env_name, address, user, authentication_token, cluster_names,
                                  kube_config=None, authentication_token_type="CLEARTEXT_PASSWORD", 
                                  namespace_prefix=None, domain=None, platform=None, docker_config_json=None, 
                                  docker_user_pass=None, description=None, storage_class=None):
        resp = self.sdk.call(
            svc='environments', func='create_private_environment', ret_field='environment', ret_error=True,
            squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No Workspaces found in Tenant'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED,
                        default=list())
            ],
            environmentName=env_name,
            address=address,
            user=user,
            authenticationToken=authentication_token,
            clusterNames=cluster_names,
            kubeConfig=kube_config,
            authenticationTokenType=authentication_token_type,
            namespacePrefix=namespace_prefix,
            domain=domain,
            platform=platform,
            dockerConfigJson=docker_config_json,
            dockerUserPass=docker_user_pass,
            description=description,
            storageClass=storage_class
        )


    def stop_environment(self, name):
        return self.sdk.call(
            svc='environments', func='stop_environment', ret_field='environment', squelch=[
                Squelch(field='error_code', value='CONFLICT', default=self.describe_environment(name),
                        warning='Environment %s is already scheduled to stop' % name)
            ],
            environmentName=name
        )

    def delete_environment(self, name, cascade=False, force=False):
        return self.sdk.call(
            svc='environments', func='delete_environment', ret_field='environment',
            environmentName=name,
            cascading=cascade,
            forced=force
        )

    def start_environment(self, name, datahub_start=True):
        return self.sdk.call(
            svc='environments', func='start_environment', ret_field='environment', squelch=[
                Squelch(field='error_code', value='CONFLICT', default=self.describe_environment(name),
                        warning='Environment %s is already scheduled to start' % name)
            ],
            environmentName=name,
            withDatahubStart=datahub_start
    )

    def set_password(self, password, environment_names=None):
        payload = dict(password=password)
        if environment_names is not None:
            if not isinstance(environment_names, list):
                environment_names = [environment_names]
            environment_crns = []
            for env in environment_names:
                r = self.describe_environment(env)
                if r is not None:
                    environment_crns.append(r['crn'])
                else:
                    self.sdk.throw_error(CdpError("Environment not found"))
            payload.update(environmentCRNs=environment_crns)
        return self.sdk.call(
            svc='environments', func='set_password', squelch=[
                Squelch(field='error_code', value='CONFLICT', default=None,
                        warning='Password Update Conflict')
            ],
            **payload
        )

    def sync_current_user(self):
        return self.sdk.call(svc='environments', func='sync_user')

    def sync_users(self, environments=None):
        if isinstance(environments, list):
            if len(environments) == 0:
                environments = None
        elif isinstance(environments, str):
            environments = [environments]
        else:
            self.sdk.throw_error(CdpError("environments must be a list of one or more strings or a string"))
        resp = self.sdk.call(
            svc='environments', func='sync_all_users', ret_error=True,
            environmentNames=environments
        )
        if isinstance(resp, CdpError):
            if resp.error_code == 'CONFLICT':
                operation_match = self.sdk.regex_search(self.sdk.OPERATION_REGEX, str(resp.violations))
                if operation_match is not None:
                    existing_op_id = operation_match.group(1)
                    if not self.sdk.strict_errors:
                        self.sdk.throw_warning(
                            CdpWarning('Sync All Users Operation already running, tracking existing job {0}'
                                       .format(existing_op_id)))
                        return self.get_sync_status(existing_op_id)
            self.sdk.throw_error(resp)
        return resp

    def get_sync_status(self, operation):
        return self.sdk.call(
            svc='environments', func='sync_status', squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=None,
                        warning='No User Sync Operation found matching %s' % operation)
            ],
            operationId=operation
        )

    def get_keytab(self, actor, environment):
        return self.sdk.call(
            svc='environments', func='get_keytab', ret_field='contents',
            actorCrn=actor,
            environmentName=environment
        )

    def list_credentials(self, name=None):
        return self.sdk.call(
            svc='environments', func='list_credentials', ret_field='credentials', squelch=[
                Squelch('NOT_FOUND', default=list())],
            credentialName=name
        )

    def describe_credential(self, name):
        resp = self.list_credentials(name)
        # Return singular None if credential not found, rather than default list()
        return self.sdk.first_item_if_exists(resp) if resp else None

    def delete_credential(self, name):
        return self.sdk.call(
            svc='environments', func='delete_credential', ret_field='credentials', squelch=[Squelch('NOT_FOUND')],
            credentialName=name
        )

    def create_aws_credential(self, name, role, description, retries=3, delay=2):
        resp = self.sdk.call(
            svc='environments', func='create_aws_credential', ret_error=True, 
            ret_field='credential', squelch=[Squelch(field='violations', value='Credential already exists with name',
                warning='Credential with this name already exists', default=None)],
            credentialName=name,
            roleArn=role,
            description=description
        )
        if isinstance(resp, CdpError):
            if retries > 0:
                consistency_violations = [
                    'Unable to verify credential', 'sts:AssumeRole', 'You are not authorized'
                ]
                if any(x in str(resp.violations) for x in consistency_violations):
                    retries = retries - 1
                    self.sdk.throw_warning(
                        CdpWarning('Got likely AWS IAM eventual consistency error [%s], %d retries left...'
                                   % (str(resp.violations), retries))
                    )
                    self.sdk.sleep(delay)
                    return self.create_aws_credential(name, role, description, retries, delay)
            else:
                self.sdk.throw_error(resp)
        return resp

    def create_azure_credential(self, name, subscription, tenant, application, secret, retries=3, delay=5):
        resp = self.sdk.call(
            svc='environments', func='create_azure_credential', ret_error=True, squelch=[
                Squelch(field='violations', value='Credential already exists with name',
                        warning='Credential with this name already exists', default=None)],
            credentialName=name,
            subscriptionId=subscription,
            tenantId=tenant,
            appBased={'applicationId': application, 'secretKey': secret}
        )
        if isinstance(resp, CdpError):
            if retries > 0:
                consistency_violations = [
                    'You may have sent your authentication request to the wrong tenant'
                ]
                if any(x in str(resp.violations) for x in consistency_violations):
                    retries = retries - 1
                    self.sdk.throw_warning(
                        CdpWarning('Got likely Azure eventual consistency error [%s], %d retries left...'
                                   % (str(resp.violations), retries))
                    )
                    self.sdk.sleep(delay)
                    return self.create_azure_credential(name, subscription, tenant, application, secret, retries, delay)
            else:
                self.sdk.throw_error(resp)
        return resp

    def create_gcp_credential(self, name, key_file):
        return self.sdk.call(
            svc='environments', func='create_gcp_credential', squelch=[
                Squelch(field='violations', value='Credential already exists with name',
                        warning='Credential with this name already exists', default=None)],
            credentialName=name,
            credentialKey=self.sdk.read_file(key_file)
        )

    def get_root_cert(self, environment):
        return self.sdk.call(
            svc='environments', func='get_root_certificate', ret_field='contents',
            environmentName=environment
        )

    def set_telemetry(self, name, workload_analytics=None, logs_collection=None):
        return self.sdk.call(
            svc='environments', func='set_telemetry_features',
            environmentName=name,
            workloadAnalytics=workload_analytics,
            reportDeploymentLogs=logs_collection
        )

    def resolve_environment_crn(self, env: Union[str, None]):
        """Ensures a given env string is either the environment crn or None"""
        if isinstance(env, str):
            if env.startswith('crn:'):
                return env
            else:
                env_desc = self.describe_environment(env)
                return env_desc['crn'] if env_desc else None
        else:
            return None
            
    def check_database_connectivity(self, host, port, name, username, password):
        return self.sdk.call(
            svc='environments', func='check_database_connectivity',
            ret_field='result',
            host=host,
            port=port,
            name=name,
            userName=username,
            password=password
        )

    def check_environment_connectivity(self, address, user, authentication_token,
                      authentication_token_type=None, cluster_names=None):
        return self.sdk.call(
            svc='environments', func='check_environment_connectivity',
            ret_field='clusters',
            address=address,
            user=user,
            authenticationToken=authentication_token,
            authenticationTokenType=authentication_token_type,
            clusterNames=cluster_names
        )

    def check_kubernetes_connectivity(self, kube_config, format=None):
        return self.sdk.call(
            svc='environments', func='check_kubernetes_connectivity',
            ret_field='status',
            kubeConfig=kube_config,
            format=format
        )    
    

