from cdpy.common import CdpSdkBase, Squelch


class CdpyMetrics(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def create_remote_write_config(self, remote_write_config=None):
        return self.sdk.call(
            svc='metrics', func='create_remote_write_config', ret_field='remoteWriteConfig', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            remoteWriteConfig=remote_write_config
        )

    def delete_remote_write_config(self, id=None):
        return self.sdk.call(
            svc='metrics', func='delete_remote_write_config', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            id=id
        )

    def describe_remote_write_config(self, id=None):
        return self.sdk.call(
            svc='metrics', func='describe_remote_write_config',
                ret_field='remoteWriteConfig', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            id=id
        )

    def list_remote_write_configs(self):
        return self.sdk.call(
            svc='metrics', func='list_remote_write_configs',
                ret_field='remoteWriteConfigs', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ]
        )

    def update_remote_write_config(self, remote_write_config=None):
        return self.sdk.call(
            svc='metrics', func='update_remote_write_config', ret_field='remoteWriteConfig', squelch=[
                Squelch('NOT_FOUND'), Squelch('INVALID_ARGUMENT'),
                Squelch(value='PATH_DISABLED', warning=ENTITLEMENT_DISABLED)
            ],
            remoteWriteConfig=remote_write_config
        )