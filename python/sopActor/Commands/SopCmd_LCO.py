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

from sopActor import CmdState
import sopActor.myGlobals as myGlobals
from sopActor.Commands import SopCmd


class SopCmd_LCO(SopCmd.SopCmd):

    def __init__(self, actor):

        # initialize from the superclass
        super(SopCmd_LCO, self).__init__(actor)

        # Define APO specific keys.
        self.keys.extend([
            keys.Key('lco', types.String(), help='Test key for LCO.')])

        # Define new commands for APO
        self.vocab = [
            ('doLCOThing', '[<lco>]', self.doLCOThing),
            ('gotoField', '[abort]', self.gotoField)]

    def doLCOThing(self, cmd):
        """Test for LCO."""

        if 'lco' in cmd.cmd.keywords:
            text = cmd.cmd.keywords['lco'].values[0]
            cmd.warn('text={0}'.format(
                qstr('got a text!: {0}'.format(text))))
        else:
            cmd.warn('text="no text was passed :("')

    def gotoField(self, cmd):
        """Slew to the current cartridge/pointing.

        Slew to the position of the currently loaded cartridge. Eventually
        this command may also do callibrations.

        """

        sopState = myGlobals.actorState
        cmdState = sopState.gotoField
        keywords = cmd.cmd.keywords

        if self.doing_science(sopState):
            cmd.fail('text=\"A science exposure sequence is running -- '
                     'will not go to field!\"')
            return

        if 'abort' in keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'gotoField')
            return

    def initCommands(self):
        """Recreate the objects that hold the state of the various commands."""

        sopState = myGlobals.actorState

        sopState.gotoField = CmdState.GotoFieldCmd()

        super(SopCmd_LCO, self).initCommands()

    def _status_commands(self, cmd, sopState, oneCommand=None):
        """Status of LCO specific commands.

        """

        super(SopCmd_LCO, self)._status_commands(cmd, sopState,
                                                 oneCommand=oneCommand)

        sopState.gotoField.genKeys(cmd=cmd, trimKeys=oneCommand)
