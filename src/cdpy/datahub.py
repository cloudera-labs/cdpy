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
                Squelch(value='NOT_FOUND', default=list(),
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

    def delete_cluster(self, name):
        return self.sdk.call(svc='datahub', func='delete_cluster', clusterName=name)
