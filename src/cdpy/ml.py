# -*- coding: utf-8 -*-

from cdpy.common import CdpError, CdpWarning, CdpSdkBase, Squelch

ENTITLEMENT_DISABLED = "Machine Learning not enabled on CDP Tenant"


class CdpyMl(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def describe_workspace(self, name=None, crn=None, env=None):
        return self.sdk.call(
            svc="ml",
            func="describe_workspace",
            ret_field="workspace",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch("INVALID_ARGUMENT"),
                Squelch("UNKNOWN"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            workspaceName=name,
            environmentName=env,
            workspaceCrn=crn,
        )

    def list_workspaces(self, env=None):
        resp = self.sdk.call(
            svc="ml",
            func="list_workspaces",
            ret_field="workspaces",
            squelch=[
                Squelch(
                    value="NOT_FOUND",
                    default=list(),
                    warning="No Workspaces found in Tenant",
                ),
                Squelch(
                    value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED, default=list()
                ),
            ],
        )
        # TODO: Replace with Filters
        if env:
            return [x for x in resp if env == x["environmentName"]]
        return resp

    def describe_all_workspaces(self, env=None):
        ws_list = self.list_workspaces(env)
        resp = []
        for ws in ws_list:
            ws_desc = self.describe_workspace(crn=ws["crn"])
            if ws_desc is not None:
                resp.append(ws_desc)
        return resp

    def list_workspace_access(self, name: str = None, crn: str = None, env: str = None):
        resp = self.sdk.call(
            svc="ml",
            func="list_workspace_access",
            ret_field="users",
            ret_error=True,
            squelch=[
                Squelch(value="UNKNOWN", default=list()),
                Squelch(
                    value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED, default=list()
                ),
            ],
            workspaceName=name,
            environmentName=env,
            workspaceCrn=crn,
        )
        if isinstance(resp, CdpError) and resp.error_code == "INVALID_ARGUMENT":
            resp.update(message=resp.violations)
            self.sdk.throw_error(resp)
        return resp

    def grant_workspace_access(
        self, identifier: str, name: str = None, crn: str = None, env: str = None
    ):
        resp = self.sdk.call(
            svc="ml",
            func="grant_workspace_access",
            ret_error=True,
            squelch=[
                Squelch(value="UNKNOWN"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            workspaceName=name,
            environmentName=env,
            workspaceCrn=crn,
            identifier=identifier,
        )
        if isinstance(resp, CdpError) and resp.error_code == "INVALID_ARGUMENT":
            resp.update(message=resp.violations)
            self.sdk.throw_error(resp)
        return resp

    def revoke_workspace_access(
        self, identifier: str, name: str = None, crn: str = None, env: str = None
    ):
        resp = self.sdk.call(
            svc="ml",
            func="revoke_workspace_access",
            ret_error=True,
            squelch=[
                Squelch(value="UNKNOWN"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            workspaceName=name,
            environmentName=env,
            workspaceCrn=crn,
            identifier=identifier,
        )
        if isinstance(resp, CdpError) and resp.error_code == "INVALID_ARGUMENT":
            resp.update(message=resp.violations)
            self.sdk.throw_error(resp)
        return resp
