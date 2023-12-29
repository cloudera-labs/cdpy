# -*- coding: utf-8 -*-

from cdpy.common import CdpError, CdpWarning, CdpSdkBase, Squelch

ENTITLEMENT_DISABLED='Machine Learning not enabled on CDP Tenant'


class CdpyMl(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def backup_workspace(self, workspace_crn, backup_name, backup_job_timeout_minutes=None, skip_validation:bool=False ):
        return self.sdk.call(
            svc='ml', func='backup_workspace', ret_field='backupCrn', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            workspaceCrn=workspace_crn,
            backupName=backup_name,
            backupJobTimeoutMinutes=backup_job_timeout_minutes,
            skipValidation=skip_validation
        )

    def delete_backup(self, backup_crn, skip_validation:bool=False ):
        return self.sdk.call(
            svc='ml', func='delete_backup', ret_field='workflowId', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            backupCrn=backup_crn,
            skipValidation=skip_validation
        )

    def describe_workspace(self, name=None, crn=None, env=None):
        return self.sdk.call(
            svc='ml', func='describe_workspace', ret_field='workspace', squelch=[
                Squelch('NOT_FOUND'), 
                Squelch('INVALID_ARGUMENT'), 
                Squelch('UNKNOWN'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            workspaceName=name,
            environmentName=env,
            workspaceCrn=crn
        )

    def list_workspaces(self, env=None):
        resp = self.sdk.call(
            svc='ml', func='list_workspaces', ret_field='workspaces', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No Workspaces found in Tenant'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED,
                        default=list())
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
    
    def get_audit_events(self, resource_crn=None):
        resp = self.sdk.call(
            svc='ml', func='get_audit_events', ret_field='auditEvents', squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No Workspaces found in Tenant'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED,
                        default=list())
            ],
            resourceCrn=resource_crn
        )       
    
    def list_workspace_access(self, name: str = None, crn: str = None, env: str = None):
        resp = self.sdk.call(
            svc='ml', func='list_workspace_access', ret_field='users', ret_error=True,
            squelch=[
                Squelch(value='UNKNOWN', default=list()),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED, default=list())
            ],
            workspaceName=name,
            environmentName=env,
            workspaceCrn=crn
        )
        if isinstance(resp, CdpError) and resp.error_code == 'INVALID_ARGUMENT':
            resp.update(message=resp.violations)
            self.sdk.throw_error(resp)
        return resp

    def list_workspace_backups(self, env_name=None, wksp_name=None, wksp_crn=None, query_options=None):
        resp = self.sdk.call(
            svc='ml', func='list_workspace_backups', ret_field='backups', ret_error=True,
            squelch=[
                Squelch(value='UNKNOWN', default=list()),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED, default=list())
            ],
            environmentName=env_name,
            workspaceName=wksp_name,
            workspaceCrn=wksp_crn,
            queryOptions=query_options
        )            

    def grant_workspace_access(self, identifier: str, name: str = None, crn: str = None, env: str = None):
        resp = self.sdk.call(
            svc='ml', func='grant_workspace_access', ret_error=True,
            squelch=[
                Squelch(value='UNKNOWN'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            workspaceName=name,
            environmentName=env,
            workspaceCrn=crn,
            identifier=identifier
        )
        if isinstance(resp, CdpError) and resp.error_code == 'INVALID_ARGUMENT':
            resp.update(message=resp.violations)
            self.sdk.throw_error(resp)
        return resp
        
    def revoke_workspace_access(self, identifier: str, name: str = None, crn: str = None, env: str = None):
        resp = self.sdk.call(
            svc='ml', func='revoke_workspace_access', ret_error=True,
            squelch=[
                Squelch(value='UNKNOWN'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            workspaceName=name,
            environmentName=env,
            workspaceCrn=crn,
            identifier=identifier
        )
        if isinstance(resp, CdpError) and resp.error_code == 'INVALID_ARGUMENT':
            resp.update(message=resp.violations)
            self.sdk.throw_error(resp)
        return resp
    
    def restore_workspace(self, new_workspace_parameters=None, backup_crn=None, use_static_subdomain:bool=False, restore_job_timeout_minutes=5 ):
        resp = self.sdk.call(
            svc='ml', func='restore_workspace', ret_field='workspaceCrn', ret_error=True,
            squelch=[
                Squelch(value='NOT_FOUND', default=list(),
                        warning='No Workspaces found in Tenant'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED,
                        default=list())
            ],
            newWorkspaceParameters=new_workspace_parameters,
            backupCrn=backup_crn,
            useStaticSubdomain=use_static_subdomain, 
            restoreJobTimeoutMinutes=restore_job_timeout_minutes

        )        