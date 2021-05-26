# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch


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

    def list_cluster_templates(self):
        return self.sdk.call(svc='datahub', func='list_cluster_templates', ret_field='clusterTemplates')

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
