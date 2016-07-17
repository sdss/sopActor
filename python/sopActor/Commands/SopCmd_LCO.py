# !usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2016-06-10 13:00:58
# @Last modified by:   Brian
# @Last Modified time: 2016-06-10 13:05:43

from __future__ import print_function, division, absolute_import

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr

from sopActor.Commands import SopCmd


class SopCmd_LCO(SopCmd.SopCmd):

    def __init__(self, actor):

        # initialize from the superclass
        super(SopCmd_LCO, self).__init__(actor)

        # Define APO specific keys.
        self.keys.extend([
            keys.Key('lco', types.Int(), help='Test key for LCO.')])

        # Define new commands for APO
        self.vocab = [
            ('doLCOThing', '[<lco>]', self.doLCOThing)]

    def doLCOThing(self, cmd):
        """Test for LCO."""

        if 'lco' in cmd.cmd.keywords:
            text = cmd.cmd.keywords['lco'].values[0]
            cmd.inform('text={0}'.format(
                qstr('got a text!: {0}'.format(text))))
        else:
            cmd.informat('text="no text was passed :("')
