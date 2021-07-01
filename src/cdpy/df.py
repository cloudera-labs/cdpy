# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpError


class CdpyDf(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def list_environments(self, only_enabled=False):
        result = self.sdk.call(
            svc='df', func='list_environments', ret_field='environments', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No Environments for DataFlow found in Tenant')
            ],
            pageSize=self.sdk.DEFAULT_PAGE_SIZE
        )
        if only_enabled:
            return [x for x in result if x['status']['state'] not in ['NOT_ENABLED']]
        return result

    def describe_environment(self, env_crn: str):
        return self.sdk.call(
            svc='df', func='get_environment', ret_field='environment', squelch=[
                Squelch(value='NOT_FOUND',
                        warning='No Environment with crn %s for DataFlow found in Tenant' % env_crn)
            ],
            crn=env_crn
        )

    def enable_environment(self, env_crn: str, authorized_ips: list = None, min_nodes: int = 3, max_nodes: int = 3,
                           enable_public_ip: bool = True):
        self.sdk.validate_crn(env_crn)
        return self.sdk.call(
            svc='df', func='enable_environment', ret_field='environment',
            crn=env_crn, minK8sNodeCount=min_nodes, maxK8sNodeCount=max_nodes,
            usePublicLoadBalancer=enable_public_ip, authorizedIpRanges=authorized_ips
        )

    def disable_environment(self, env_crn: str, persist: bool = False, force: bool = False):
        self.sdk.validate_crn(env_crn)
        resp = self.sdk.call(
            svc='df', func='disable_environment', ret_field='status', ret_error=True,
            crn=env_crn, persist=persist
        )
        if isinstance(resp, CdpError):
            if force:
                return self.sdk.call(
                    svc='df', func='delete_environment',
                    crn=env_crn
                )
            else:
                resp.update(message=resp.violations)
                self.sdk.throw_error(resp)
        return resp
