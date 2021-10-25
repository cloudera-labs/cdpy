# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpError, CdpWarning


class CdpyDatahub(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def describe_cluster(self, name):
        return self.sdk.call(
            svc='datahub', func='describe_cluster', ret_field='cluster', squelch=[Squelch('NOT_FOUND')],
            clusterName=name
        )

    def list_clusters(self, environment_name=None):
        return self.sdk.call(
            svc='datahub', func='list_clusters', ret_field='clusters', squelch=[
                Squelch(value='INVALID_ARGUMENT', default=list(),
                        warning='No Datahubs found in Tenant or provided Environment %s' % str(environment_name))
            ],
            environmentName=environment_name
        )

    def describe_all_clusters(self, environment_name=None):
        clusters_listing = self.list_clusters(environment_name)
        if clusters_listing:
            return [self.describe_cluster(cluster['clusterName']) for cluster in clusters_listing]
        return clusters_listing

    def list_cluster_templates(self, retries=3, delay=5):
        # Intermittent timeout issue in CDP 7.2.10, should be reverted to bare listing in 7.2.12
        resp = self.sdk.call(
            svc='datahub', func='list_cluster_templates', ret_field='clusterTemplates',
            ret_error=True
        )
        if isinstance(resp, CdpError):
            if retries > 0:
                if str(resp.status_code) == '500' and resp.error_code == 'UNKNOWN':
                    retries = retries - 1
                    self.sdk.throw_warning(
                        CdpWarning('Got likely CDP Control Plane eventual consistency error, %d retries left...'
                                   % (retries))
                    )
                    self.sdk.sleep(delay)
                    return self.list_cluster_templates(retries, delay)
            else:
                self.sdk.throw_error(resp)
        return resp

    def describe_cluster_template(self, name):
        return self.sdk.call(svc='datahub', func='describe_cluster_template', ret_field='clusterTemplate', squelch=[
            Squelch(value='NOT_FOUND')], clusterTemplateName=name)

    def delete_cluster(self, name):
        return self.sdk.call(svc='datahub', func='delete_cluster', clusterName=name)

    def delete_cluster_templates(self, names):
        names = names if isinstance(names, list) else [names]
        return self.sdk.call(svc='datahub', func='delete_cluster_templates', squelch=[Squelch(value='NOT_FOUND')],
                             ret_field='clusterTemplates', clusterTemplateNames=names)

    def create_cluster_template(self, name, description, content):
        return self.sdk.call(
            svc='datahub', func='create_cluster_template', ret_field='clusterTemplate',
            clusterTemplateName=name, description=description, clusterTemplateContent=content
        )

    def list_cluster_definitions(self):
        return self.sdk.call(svc='datahub', func='list_cluster_definitions', ret_field='clusterDefinitions')

    def describe_cluster_definition(self, name):
        return self.sdk.call(svc='datahub', func='describe_cluster_definition', ret_field='clusterDefinition', squelch=[
            Squelch(value='NOT_FOUND')], clusterDefinitionName=name)

    def start_cluster(self, name):
        return self.sdk.call(
            svc='datahub', func='start_cluster', squelch=[Squelch('NOT_FOUND')],
            clusterName=name
        )

    def stop_cluster(self, name):
        return self.sdk.call(
            svc='datahub', func='stop_cluster', squelch=[Squelch('NOT_FOUND')],
            clusterName=name
        )