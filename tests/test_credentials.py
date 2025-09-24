from cdpy.common import StaticCredentials
from cdpy.cdpy import Cdpy
from cdpy.iam import CdpyIam


def test_static_credentials():
    ACCESS_KEY_ID = "thekey"
    PRIVATE_KEY = "theanswer"

    iam = Cdpy(cdp_credentials=StaticCredentials(ACCESS_KEY_ID, PRIVATE_KEY)).iam

    assert iam.sdk.cdp_credentials.access_key_id == ACCESS_KEY_ID
    assert iam.sdk.cdp_credentials.private_key == PRIVATE_KEY


def test_static_credentials_submodule():
    ACCESS_KEY_ID = "thekey"
    PRIVATE_KEY = "theanswer"

    iam = CdpyIam(cdp_credentials=StaticCredentials(ACCESS_KEY_ID, PRIVATE_KEY))

    assert iam.sdk.cdp_credentials.access_key_id == ACCESS_KEY_ID
    assert iam.sdk.cdp_credentials.private_key == PRIVATE_KEY
