import pytest
from cdpy.iam import CdpyIam
from tests import conftest

sdk = CdpyIam()


def test_get_user():
    r1 = sdk.get_user()
    assert isinstance(r1, dict)
    assert isinstance(r1["workloadUsername"], str)

    r2 = sdk.get_user(name=r1["userId"])
    assert r2["userId"] == r1["userId"]

    with pytest.warns(UserWarning, match="Removing empty string arg"):
        r3 = sdk.get_user(name="")  # Invalid submission
        assert isinstance(r3["workloadUsername"], str)

    with pytest.warns(UserWarning, match="User could not be retrieved"):
        r4 = sdk.get_user(name="obviouslyfakeomgwtfbbq")
        assert r4 is None

    # Example test function
