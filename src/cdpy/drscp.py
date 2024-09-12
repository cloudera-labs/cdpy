# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpcliWrapper

ENTITLEMENT_DISABLED='DRS not enabled on CDP Tenant'


class CdpyDrscp(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create_backup(self, backup_name=None, item_name=None ):
        return self.sdk.call(
            svc='drscp', func='create_backup', ret_field='backupCrn', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            backupName=backup_name,
            itemName=item_name
        )

    def delete_backup(self, backup_crn):
        return self.sdk.call(
            svc='drscp', func='delete_backup', ret_field='deleteBackupCrn', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            backupCrn=backup_crn
        )

    def describe_backup(self, backup_crn):
        return self.sdk.call(
            svc='drscp', func='describe_backup', ret_field='backup', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            backupCrn=backup_crn
        )

    def describe_restore(self, restore_crn):
        return self.sdk.call(
            svc='drscp', func='describe_restore', ret_field='restore', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            restoreCrn=restore_crn
        )

    def get_logs(self, crn ):
        return self.sdk.call(
            svc='drscp', func='get_logs', ret_field='logs', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            crn=crn
        )

    def list_backup_entities(self):
        return self.sdk.call(
            svc='drscp', func='list_backup_entities', ret_field='items', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ]
        )

    def list_backups(self, backup_name=None, job_states=None):
        return self.sdk.call(
            svc='drscp', func='list_backups', ret_field='backup', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            backupName=backup_name,
            jobStates=job_states
        )

    def list_restores(self, job_states=None, backup_crn=None):
        return self.sdk.call(
            svc='drscp', func='list_restores', ret_field='restores', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            jobStates=job_states,
            backupCrn=backup_crn
        )

    def restore_backup(self, backup_crn ):
        return self.sdk.call(
            svc='drscp', func='restore_backup', ret_field='restoreCrn', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            backupCrn=backup_crn
        )