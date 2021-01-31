# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch


class CdpyMl(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def describe_workspace(self, name=None, crn=None, env=None):
        return self.sdk.call(
            svc='ml', func='describe_workspace', ret_field='workspace', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'), Squelch('UNKNOWN')
            ],
            workspaceName=name,
            environmentName=env,
            workspaceCrn=crn
        )

    def list_workspaces(self, env=None):
        resp = self.sdk.call(
            svc='ml', func='list_workspaces', ret_field='workspaces', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No Workspaces found in Tenant')
            ]
        )
        # TODO: Replace with Filters
        if env:
            return [x for x in resp if env == x['environmentName']]
        return resp

    def describe_all_workspaces(self, env=None):
        ws_list = self.list_workspaces(env)
        resp = []
        for ws in ws_list:
            ws_desc = self.describe_workspace(crn=ws['crn'])
            if ws_desc is not None:
                resp.append(ws_desc)
        return resp
