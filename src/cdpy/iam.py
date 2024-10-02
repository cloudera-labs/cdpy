# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch


class CdpyIam(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_user(self, name=None):
        return self.sdk.call(
            # Describe base function calls
            svc='iam',  # Name of the client service
            func='get_user',  # Name of the Function within the service to call
            ret_field='user',  # Optional child field to return, often CDP CLI responses are wrapped like this
            squelch=[  # List of below Client Error Handlers
                # Describe any Client Error responses using the provided Squelch class
                Squelch(
                    field='error_code',  # CdpError Field to test
                    value='NOT_FOUND',  # String to check for in Field
                    warning='CDP User could not be retrieved',  # Warning to throw if encountered
                    default=None  # Value to return instead of Error
                )
            ],
            # Include any keyword args that may be used in the function call, None/'' args will be ignored
            userId=name  # As name is None by default, it will be ignored unless provided
        )

    def set_cloudera_sso(self, enabled: bool):
        func = 'enable_cloudera_sso_login' if enabled is True else 'disable_cloudera_sso_login'
        return self.sdk.call(svc='iam', func=func)

    def set_password_lifetime(self, lifetime: int):
        return self.sdk.call(svc='iam', func='set_workload_password_policy', maxPasswordLifetimeDays=lifetime)

    def create_group(self, name: str, sync: bool = True):
        return self.sdk.call(
            svc='iam', func='create_group', ret_field='group',
            groupName=name,
            syncMembershipOnUserLogin=sync
        )

    def update_group(self, name, sync=True):
        return self.sdk.call(
            svc='iam', func='update_group', ret_field='group',
            groupName=name,
            syncMembershipOnUserLogin=sync
        )

    def delete_group(self, name):
        return self.sdk.call(
            svc='iam', func='delete_group', ret_field='group',
            groupName=name
        )

    def add_group_user(self, group, user):
        return self.sdk.call(
            svc='iam', func='add_user_to_group', ret_field='group',
            groupName=group, userId=user
        )

    def remove_group_user(self, group, user):
        return self.sdk.call(
            svc='iam', func='remove_user_from_group', ret_field='group',
            groupName=group, userId=user
        )

    def assign_group_role(self, group, role):
        return self.sdk.call(
            svc='iam', func='assign_group_role', ret_field='group',
            groupName=group, role=role
        )

    def unassign_group_role(self, group, role):
        return self.sdk.call(
            svc='iam', func='unassign_group_role', ret_field='group',
            groupName=group, role=role
        )

    def assign_group_resource_role(self, group, resource, role):
        return self.sdk.call(
            svc='iam', func='assign_group_resource_role', ret_field='group',
            groupName=group, resourceCrn=resource, resourceRoleCrn=role
        )

    def unassign_group_resource_role(self, group, resource, role):
        return self.sdk.call(
            svc='iam', func='unassign_group_resource_role', ret_field='group',
            groupName=group, resourceCrn=resource, resourceRoleCrn=role
        )

    def assign_user_role(self, user, role):
        return self.sdk.call(
            svc='iam', func='assign_user_role', ret_field='user',
            user=user, role=role
        )

    def unassign_user_role(self, user, role):
        return self.sdk.call(
            svc='iam', func='unassign_user_role', ret_field='user',
            user=user, role=role
        )
    def assign_user_resource_role(self, user, resource, role):
        return self.sdk.call(
            svc='iam', func='assign_user_resource_role', ret_field='user',
            user=user, resourceCrn=resource, resourceRoleCrn=role
        )

    def unassign_user_resource_role(self, user, resource, role):
        return self.sdk.call(
            svc='iam', func='unassign_user_resource_role', ret_field='user',
            user=user, resourceCrn=resource, resourceRoleCrn=role
        )

    def gather_groups(self, group_names=None):
        # TODO: Needs tests
        resp = self.list_groups(group_names=group_names)
        if resp:
            list(map(lambda grp: grp.update(users=self.list_group_membership(grp['crn'])), resp))
            list(map(lambda grp: grp.update(roles=self.list_group_assigned_roles(grp['crn'])), resp))
            list(map(lambda grp: grp.update(resource_roles=self.list_group_assigned_resource_roles(grp['crn'])), resp))
        return resp

    def list_groups(self, group_names=None):
        group_names = group_names if group_names is None or isinstance(group_names, list) else [group_names]
        return self.sdk.call(
            svc='iam', func='list_groups', ret_field='groups', squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No Groups found for Group Names, %s' % str(group_names))
            ],
            groupNames=group_names
        )

    def gather_users(self, users=None):
        resp = self.list_users(users=users)
        return resp if resp is None else self.sdk.filter_by_key(resp, 'crn')

    def list_users(self, users=None):
        users = users if users is None or isinstance(users, list) else [users]
        return self.sdk.call(
            svc='iam', func='list_users', ret_field='users', squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No Users found for UserIds, %s' % str(users))
            ],
            userIds=users
        )

    def list_group_membership(self, group_name):
        return self.sdk.call(
            svc='iam', func='list_group_members', ret_field='memberCrns', squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No Group Members found for Group, %s' % group_name)
            ],
            groupName=group_name
        )

    def list_group_assigned_roles(self, group_name):
        return self.sdk.call(
            svc='iam', func='list_group_assigned_roles', ret_field='roleCrns', squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No Roles found for Group, %s' % group_name)
            ],
            groupName=group_name
        )

    def list_group_assigned_resource_roles(self, group_name):
        return self.sdk.call(
            svc='iam', func='list_group_assigned_resource_roles', ret_field='resourceAssignments', squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No Group Assigned Resource Roles found for Group, %s' % group_name)
            ],
            groupName=group_name
        )

    def list_resource_roles(self, roles=None):
        return self.sdk.call(
            svc='iam', func='list_resource_roles', ret_field='resourceRoles', squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No Resource Roles found for Names, %s' % str(roles))
            ],
            resourceRoleNames=roles
        )

    def list_roles(self, roles=None):
        return self.sdk.call(
            svc='iam', func='list_roles', ret_field='roles', squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No Roles found for Names, %s' % str(roles))
            ],
            roleNames=roles
        )
    
    def get_account(self):
        return self.sdk.call(
            svc='iam', func='get_account', ret_field='account', squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=None,
                        warning='CDP Account could not be retrieved')
            ]
        )

    def list_groups_for_user(self, user_id=None):
        return self.sdk.call(
            svc='iam', func='list_groups_for_user', ret_field='groupCrns', 
            squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No users, %s' % str(user_id))
            ],
            userId=user_id
        )

    def list_user_assigned_roles(self, user=None):
        return self.sdk.call(
            svc='iam', func='list_user_assigned_roles', ret_field='roleCrns', 
            squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No users, %s' % str(user))
            ],
            user=user
        )

    def list_user_assigned_resource_roles(self, user=None):
        return self.sdk.call(
            svc='iam', func='list_user_assigned_resource_roles', ret_field='resourceAssignments', 
            squelch=[
                Squelch(field='error_code', value='NOT_FOUND', default=list(),
                        warning='No users, %s' % str(user))
            ],
            user=user
        )