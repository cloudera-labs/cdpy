# -*- coding: utf-8 -*-

# Copyright 2026 Cloudera, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function

__metaclass__ = type

import pytest

from cdpy.environments import CdpyEnvironments

@pytest.fixture()
def env_sdk(cdpy_fixture) -> CdpyEnvironments:
    """Fixture for environment SDK tests."""
    return cdpy_fixture.environments


@pytest.fixture()
def env_name(env_sdk) -> str:
    """Fixture for a single environment name."""
    envs = env_sdk.list_environments()
    if not envs:
        pytest.skip("No environments available for testing.")
    return envs[0]["environmentName"]


def test_list_all_environments(env_sdk):
    
    """Test listing all environments."""
    envs = env_sdk.list_environments()

    assert isinstance(envs, list)
    for env in envs:
        assert "crn" in env
        assert "environmentName" in env


def test_get_environment_details(env_sdk, env_name):
    """Test getting environment details."""
    details = env_sdk.describe_environment(env_name)

    assert isinstance(details, dict)
    assert details["environmentName"] == env_name
    assert "crn" in details
