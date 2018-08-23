#!/usr/bin/env python
# encoding: utf-8
"""

slewThread.py

Created by José Sánchez-Gallego on 25 Nov 2015.
Licensed under a 3-clause BSD license.

Revision history:
    25 Nov 2015 J. Sánchez-Gallego
      Initial version

This thread contains the classes related to telescope slews, namely
gotoPosition and gotoGangChange. doApogeeCals is also included here as
APOGEE cals are most frequently taken when gotoGangChange is called.

This thread exists so that the telescope while other tasks are being executed.
For instance, this allows to execute gotoGangChange while a BOSS exposure is
being read. The slew can only happen when the isSlewDisable is False.

"""

from __future__ import division, print_function

import Queue
import threading

import sopActor
import sopActor.myGlobals as myGlobals
from sopActor import Msg
from sopActor import masterThread as master
from sopActor.multiCommand import MultiCommand


def goto_position(cmd, cmdState, actorState):
    """Goes to a certain (az, alt, rot) position."""

    finishMsg = 'On position.'

    # TODO: I think this line does not do anything. It should be removed and
    # then tested.
    multiCmd = master.SopMultiCommand(cmd, actorState.timeout + 100, cmdState.name + '.slew')
    cmdState.setStageState('slew', 'running')

    # Heading towards the instrument change pos.
    az = cmdState.az
    alt = cmdState.alt
    rot = cmdState.rot

    slewDuration = 120
    multiCmd = MultiCommand(cmd, slewDuration + actorState.timeout, None)

    # Start with an axis init, in case the axes are not clear.
    multiCmd.append(master.SopPrecondition(sopActor.TCC, Msg.AXIS_INIT))

    multiCmd.append(sopActor.TCC, Msg.SLEW, actorState=actorState, az=az, alt=alt, rot=rot)

    multiCmd.append(sopActor.TCC, Msg.AXIS_STOP, actorState=actorState)

    if not master.handle_multiCmd(
            multiCmd, cmd, cmdState, 'slew', 'Failed to slew to position az={0}, alt={1},'
            ' rot={2}'.format(az, alt, rot)):
        return

    master.finish_command(cmd, cmdState, actorState, finishMsg)


def apogee_dome_flat(cmd, cmdState, actorState, multiCmd, failMsg='failed to take APOGEE flat'):
    """Takes an APOGEE dome flat.

    Take an APOGEE dome flat: shutter open, FFS closed, FFlamp on very briefly.
    Doesn't "finish", because it is used in the middle of goto_gang_change;
    Return True if success, fail the cmd and return False if not.
    """

    if not master.is_gang_at_cart(cmd, cmdState, actorState):
        return False

    master.prep_apogee_shutter(multiCmd, open=True)

    multiCmd.append(master.SopPrecondition(sopActor.FFS, Msg.FFS_MOVE, open=False))

    multiCmd.append(sopActor.APOGEE_SCRIPT, Msg.POST_FLAT, cmdState=cmdState)

    doMultiCmd = master.handle_multiCmd(multiCmd, cmd, cmdState, 'domeFlat', failMsg, finish=True)

    if not doMultiCmd:
        return False

    # Per ticket #2379, we always want to close the APOGEE shuter
    # after dome flats.
    multiCmd = master.SopMultiCommand(cmd, actorState.timeout, '')
    multiCmd.append(sopActor.APOGEE, Msg.APOGEE_SHUTTER, open=False)
    if not multiCmd.run():
        cmdState.setStageState('domeFlat', 'failed')
        failMsgComplete = ': '.join((failMsg, 'error closing apogee shutter'))
        return master.fail_command(cmd, cmdState, failMsgComplete, finish=True)

    return True


def goto_gang_change(cmd, cmdState, actorState, failMsg=None):
    """Goes to the gang change positions.

    Goto gang change position at requested altitude, taking a dome flat on
    the way if we have an APOGEE cartridge, and the gang is plugged into it.
    """

    finishMsg = 'at gang change position'
    failMsg = 'failed to take flat before gang change' \
        if failMsg is None else failMsg

    # Behavior varies depending on where the gang connector is.
    gangCart = actorState.apogeeGang.atCartridge()

    if cmdState.doDomeFlat:
        multiCmd = master.SopMultiCommand(cmd, actorState.timeout + 100,
                                          cmdState.name + '.domeFlat')
        cmdState.setStageState('domeFlat', 'running')

        # If the gang connector is at the cartridge, we should do cals.
        if gangCart and actorState.survey != sopActor.BOSS:
            cmd.inform('text="scheduling dome flat: {0}"'.format(actorState.survey))
            doCals = apogee_dome_flat(cmd, cmdState, actorState, multiCmd, failMsg)
            if not doCals:
                return
        else:
            gangAt = actorState.apogeeGang.getPos()
            cmd.inform('text="Skipping flat with {0} and {1}"'.format(gangAt, actorState.survey))
            cmdState.setStageState('domeFlat', 'idle')

    if cmdState.doSlew:
        multiCmd = master.SopMultiCommand(cmd, actorState.timeout + 100, cmdState.name + '.slew')
        cmdState.setStageState('slew', 'running')

        # Close the FFS, to prevent excess light
        # incase of slews past the moon, etc.
        multiCmd.append(master.SopPrecondition(sopActor.FFS, Msg.FFS_MOVE, open=False))

        tccDict = actorState.models['tcc'].keyVarDict
        if gangCart:
            # Heading towards the instrument change pos.
            az = 121
            alt = cmdState.alt
            rot = 0

            # Try to move the rotator as far as we can while the altitude
            # is moving.
            thisAlt = tccDict['axePos'][1]
            thisRot = tccDict['axePos'][2]
            dRot = rot - thisRot
            if dRot != 0:
                dAlt = alt - thisAlt
                dAltTime = abs(dAlt) / 1.5  # deg/sec
                dRotTime = abs(dRot) / 2.0  # deg/sec
                dCanRot = dRot * min(1.0, dAltTime / dRotTime)
                rot = thisRot + dCanRot
        else:
            # Nod up: going to the commanded altitude,
            # leaving az and rot where they are.
            az = tccDict['axePos'][0]
            alt = cmdState.alt
            rot = tccDict['axePos'][2]

        slewDuration = 60
        multiCmd = MultiCommand(cmd, slewDuration + actorState.timeout, None)

        # Start with an axis init, in case the axes are not clear.
        multiCmd.append(master.SopPrecondition(sopActor.TCC, Msg.AXIS_INIT))

        # If the gang is at the cart, we need to close the apogee shutter
        # during the slew, no matter which survey is leading.
        if actorState.apogeeGang.atCartridge():
            master.prep_apogee_shutter(multiCmd, open=False)
            # used to do darks on the way to the field.
            # multiCmd.append(sopActor.APOGEE_SCRIPT, Msg.APOGEE_PARK_DARKS)

        multiCmd.append(sopActor.TCC, Msg.SLEW, actorState=actorState, az=az, alt=alt, rot=rot)

        multiCmd.append(sopActor.TCC, Msg.AXIS_STOP, actorState=actorState)

        doMultiCmd = master.handle_multiCmd(multiCmd, cmd, cmdState, 'slew',
                                            'Failed to slew to gang change')
        if not doMultiCmd:
            return

    master.finish_command(cmd, cmdState, actorState, finishMsg)


def main(actor, queues):
    """Main loop for SOP selw thread."""

    threadName = 'slew'
    timeout = myGlobals.actorState.timeout

    while True:
        try:
            msg = queues[sopActor.SLEW].get(timeout=timeout)

            if msg.type == Msg.EXIT:
                if msg.cmd:
                    msg.cmd.inform('text=\"Exiting thread {0}\"'
                                   .format(threading.current_thread().name))

                return

            elif msg.type == Msg.GOTO_POSITION:
                cmd, cmdState, actorState = master.preprocess_msg(msg)
                goto_position(cmd, cmdState, actorState)

            elif msg.type == Msg.DO_APOGEE_DOME_FLAT:
                cmd, cmdState, actorState = master.preprocess_msg(msg)
                name = 'apogeeDomeFlat'
                finishMsg = 'Dome flat done.'
                # 50 seconds is the read time for this exposure.
                multiCmd = MultiCommand(cmd, actorState.timeout + 50, name)
                # the dome flat command sends a fail msg if it fails.
                if apogee_dome_flat(cmd, cmdState, actorState, multiCmd):
                    master.finish_command(cmd, cmdState, actorState, finishMsg)

            elif msg.type == Msg.GOTO_GANG_CHANGE:
                cmd, cmdState, actorState = master.preprocess_msg(msg)
                goto_gang_change(cmd, cmdState, actorState)

            else:
                raise ValueError('Unknown message type {0}'.format(msg.type))

        except Queue.Empty:
            actor.bcast.diag('text="%s alive"' % threadName)

        except Exception, ee:
            sopActor.handle_bad_exception(actor, ee, threadName, msg)
