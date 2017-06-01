# !usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2016-06-10 13:00:58
# @Last modified by:   Brian
# @Last Modified time: 2016-06-10 13:28:53

from __future__ import print_function, division, absolute_import

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr

from sopActor.Commands import SopCmd


class SopCmd_LOCAL(SopCmd.SopCmd):

    def __init__(self, actor):
        # initialize from the superclass
        super(SopCmd_LOCAL, self).__init__(actor)

        # Define some new command keywords

        # Define new commands for Local
        pass

