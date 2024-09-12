# -*- coding: utf-8 -*-

"""
For Wrapping interactions with the Cloudera CDP CLI
"""

from cdpy.common import CdpSdkBase
from cdpy.iam import CdpyIam
from cdpy.environments import CdpyEnvironments
from cdpy.datahub import CdpyDatahub
from cdpy.datalake import CdpyDatalake
from cdpy.ml import CdpyMl
from cdpy.de import CdpyDe
from cdpy.opdb import CdpyOpdb
from cdpy.dw import CdpyDw
from cdpy.df import CdpyDf
from cdpy.dc import CdpyDc


class Cdpy(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.iam = CdpyIam(*args, **kwargs)
        self.environments = CdpyEnvironments(*args, **kwargs)
        self.datahub = CdpyDatahub(*args, **kwargs)
        self.datalake = CdpyDatalake(*args, **kwargs)
        self.ml = CdpyMl(*args, **kwargs)
        self.de = CdpyDe(*args, **kwargs)
        self.opdb = CdpyOpdb(*args, **kwargs)
        self.dw = CdpyDw(*args, **kwargs)
        self.df = CdpyDf(*args, **kwargs)
        self.de = CdpyDe(*args, **kwargs)
        self.dc = CdpyDc(*args, **kwargs)
