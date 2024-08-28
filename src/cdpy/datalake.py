# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch


class CdpyDatalake(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def list_datalakes(self, name=None):
        return self.sdk.call(
            svc='datalake', func='list_datalakes', ret_field='datalakes', squelch=[Squelch('NOT_FOUND')],
            environmentName=name
        )

    def is_datalake_running(self, environment_name):
        resp = self.list_datalakes(environment_name)
        if resp and len(resp) == 1:
            if resp[0]['status'] in self.sdk.STARTED_STATES:
                return True
        return False

    def describe_datalake(self, name):
        return self.sdk.call(
            svc='datalake', func='describe_datalake', ret_field='datalake',
            squelch=[Squelch('NOT_FOUND'), Squelch('UNKNOWN')
                     ],
            datalakeName=name
        )

    def delete_datalake(self, name, force=False):
        return self.sdk.call(
            svc='datalake', func='delete_datalake', squelch=[Squelch('NOT_FOUND')],
            datalakeName=name, force=force
        )

    def describe_all_datalakes(self, environment_name=None):
        datalakes_listing = self.list_datalakes(environment_name)
        if datalakes_listing:
            return [self.describe_datalake(datalake['datalakeName']) for datalake in datalakes_listing]
        return datalakes_listing

    def create_datalake_backup(self, datalake_name, backup_name=None, backup_location=None,
                               close_db_connections=None, skip_atlas_indexes=None, skip_atlas_metadata=None,
                               skip_ranger_audits=None, skip_ranger_hms_metadata=None,
                               skip_validation=None, validation_only=None):
        return self.sdk.call(
            svc='datalake', func='backup_datalake', squelch=[],
            datalakeName=datalake_name, backupName=backup_name,
            backupLocation=backup_location, closeDbConnections=close_db_connections,
            skipAtlasIndexes=skip_atlas_indexes, skipAtlasMetadata=skip_atlas_metadata,
            skipRangerAudits=skip_ranger_audits, skipRangerHmsMetadata=skip_ranger_hms_metadata,
            skipValidation=skip_validation, validationOnly=validation_only
        )

    def check_datalake_backup_status(self, datalake_name, backup_id=None, backup_name=None):
        return self.sdk.call(
            svc='datalake', func='backup_datalake_status',
            squelch=[Squelch('NOT_FOUND'), Squelch('UNKNOWN')
                     ],
            datalakeName=datalake_name, backupId=backup_id, backupName=backup_name
        )

    def list_datalake_backups(self, datalake_name):
        return self.sdk.call(
            svc='datalake', func='list_datalake_backups',
            squelch=[Squelch('NOT_FOUND'), Squelch('UNKNOWN')
                     ],
            datalakeName=datalake_name
        )

    def restore_datalake_backup(self, datalake_name, backup_name=None, backup_id=None, backup_location_override=None, skip_atlas_indexes=None, skip_atlas_metadata=None, skip_ranger_audits=None, skip_ranger_hms_metadata=None, skip_validation=None, validation_only=None):
        return self.sdk.call(
            svc='datalake', func='restore_datalake', squelch=[],
            datalakeName=datalake_name, backupName=backup_name,
            backupId=backup_id, backupLocationOverride=backup_location_override,
            skipAtlasIndexes=skip_atlas_indexes, skipAtlasMetadata=skip_atlas_metadata,
            skipRangerAudits=skip_ranger_audits, skipRangerHmsMetadata=skip_ranger_hms_metadata,
            skipValidation=skip_validation, validationOnly=validation_only
        )        

    def check_datalake_restore_status(self, datalake_name, restore_id=None):
        return self.sdk.call(
            svc='datalake', func='restore_datalake_status',
            squelch=[Squelch('NOT_FOUND'), Squelch('UNKNOWN')
                     ],
            datalakeName=datalake_name, restoreId=restore_id
        )