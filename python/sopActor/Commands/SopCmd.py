#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Filename: SopCmd.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
#
# @Last modified by: José Sánchez-Gallego (gallegoj@uw.edu)
# @Last modified time: 2018-12-11 17:33:24

from __future__ import absolute_import, division, print_function

import glob
import os
import threading

import opscore.protocols.keys as keys
import opscore.protocols.types as types
import sopActor
import sopActor.myGlobals as myGlobals
from opscore.utility.qstr import qstr
from sopActor import CmdState, Msg
from sopActor.multiCommand import MultiCommand


""" Wrap top-level ICC functions. """

# SDSS-IV plates should all be "APOGEE-2;MaNGA", but we need both,
# for test plates drilled as part of SDSS-III.
survey_dict = {
    'UNKNOWN': sopActor.UNKNOWN,
    'ecamera': sopActor.ECAMERA,
    'BOSS': sopActor.BOSS,
    'eBOSS': sopActor.BOSS,
    'APOGEE': sopActor.APOGEE,
    'APOGEE-2': sopActor.APOGEE,
    'MaNGA': sopActor.MANGA,
    'APOGEE-2&MaNGA': sopActor.APOGEEMANGA,
    'APOGEE&MaNGA': sopActor.APOGEEMANGA
}
surveyMode_dict = {
    'None': None,
    None: None,
    'APOGEE lead': sopActor.APOGEELEAD,
    'MaNGA dither': sopActor.MANGADITHER,
    'MaNGA 10min': sopActor.MANGA10,
    'MaNGA stare': sopActor.MANGASTARE,
    'MaStar': sopActor.MASTAR
}

# And the inverses of the above.
# Can't directly make an inverse, since it's not one-to-one.
survey_inv_dict = {
    sopActor.UNKNOWN: 'UNKNOWN',
    sopActor.ECAMERA: 'ecamera',
    sopActor.BOSS: 'eBOSS',
    sopActor.APOGEE: 'APOGEE-2',
    sopActor.MANGA: 'MaNGA',
    sopActor.APOGEEMANGA: 'APOGEE-2&MaNGA'
}
surveyMode_inv_dict = {
    None: 'None',
    sopActor.MANGADITHER: 'MaNGA dither',
    sopActor.MANGA10: 'MaNGA 10min',
    sopActor.MANGASTARE: 'MaNGA stare',
    sopActor.APOGEELEAD: 'APOGEE lead',
    sopActor.MASTAR: 'MaStar'
}


class SopCmd(object):
    """ Wrap commands to the sop actor"""

    def __init__(self, actor):

        self.actor = actor
        self.replyQueue = sopActor.Queue('(replyQueue)', 0)

        # Declare keys that we're going to use
        self.keys = keys.KeysDictionary(
            'sop_sop', (1, 2),
            keys.Key('abort', help='Abort a command'),
            keys.Key('clear', help='Clear a flag'),
            keys.Key('narc', types.Int(), help='Number of arcs to take'),
            keys.Key('nbias', types.Int(), help='Number of biases to take'),
            keys.Key('ndark', types.Int(), help='Number of darks to take'),
            keys.Key('nexp', types.Int(), help='Number of exposures to take'),
            keys.Key('nflat', types.Int(), help='Number of flats to take'),
            keys.Key('nStep', types.Int(), help='Number of dithered exposures to take'),
            keys.Key('nTick', types.Int(), help='Number of ticks to move collimator'),
            keys.Key('arcTime', types.Float(), help='Exposure time for arcs'),
            keys.Key('darkTime', types.Float(), help='Exposure time for flats'),
            keys.Key('expTime', types.Float(), help='Exposure time'),
            keys.Key('guiderTime', types.Float(), help='Exposure time for guider'),
            keys.Key('offset', types.Float(), help='Offset before calibrations (arcsecs in Dec)'),
            keys.Key('fiberId', types.Int(), help='A fiber ID'),
            keys.Key('flatTime', types.Float(), help='Exposure time for flats'),
            keys.Key('guiderFlatTime', types.Float(), help='Exposure time for guider flats'),
            keys.Key('keepQueues', help='Restart thread queues'),
            keys.Key('noSlew', help="Don't slew to field"),
            keys.Key('noHartmann', help="Don't make Hartmann corrections"),
            keys.Key('noGuider', help="Don't start the guider"),
            keys.Key('noCalibs', help="Don't run the calibration step"),
            keys.Key('noDomeFlat', help="Don't run the dome flat step"),
            keys.Key('sp1', help='Select SP1'),
            keys.Key('sp2', help='Select SP2'),
            keys.Key('geek', help='Show things that only some of us love'),
            keys.Key('subSystem', types.String() * (1, ), help='The sub-systems to bypass'),
            keys.Key('threads', types.String() * (1, ), help='Threads to restart; default: all'),
            keys.Key('scale', types.Float(), help="Current scale from \"tcc show scale\""),
            keys.Key('delta', types.Float(), help='Delta scale (percent)'),
            keys.Key('absolute', help='Set scale to provided value'),
            keys.Key('test', help='Assert that the exposures are not expected to be meaningful'),
            keys.Key('keepOffsets', help='When slewing, do not clear accumulated offsets'),
            keys.Key('ditherPairs', types.Int(),
                     help='Number of dither pairs (AB or BA) to observe'),
            keys.Key('ditherSeq', types.String(),
                     help='dither positions for each sequence, e.g. AB'),
            keys.Key('comment', types.String(), help='comment for headers'),
            keys.Key('dither', types.String(), help='MaNGA dither position for a single dither.'),
            keys.Key('dithers', types.String(),
                     help='MaNGA dither positions for a dither sequence.'),
            keys.Key('mangaDithers', types.String(),
                     help='MaNGA dither positions for a dither sequence.'),
            keys.Key('mangaDither', types.String(),
                     help='MaNGA dither position for a single dither.'),
            keys.Key('count', types.Int(), help='Number of MaNGA dither sets to perform.'),
            keys.Key('scriptName', types.String(), help='name of script to run'),
            keys.Key('az', types.Float(), help='what azimuth to slew to'),
            keys.Key('rotOffset', types.Float(), help='what rotator offset to add'),
            keys.Key('alt', types.Float(), help='what altitude to slew to'),
        )

        # Declare commands
        self.vocab = [
            ('bypass', '<subSystem> [clear]', self.bypass),
            ('doBossCalibs', '[<narc>] [<nbias>] [<ndark>] [<nflat>] [<arcTime>] '
                             '[<darkTime>] [<flatTime>] [<guiderFlatTime>] [<offset>] [abort]',
                             self.doBossCalibs),
            ('doBossScience', '[<expTime>] [<nexp>] [abort] [stop] [test]',
                              self.doBossScience),
            ('doApogeeScience', '[<expTime>] [<ditherPairs>] [stop] [<abort>] [<comment>]',
                                self.doApogeeScience),
            ('doApogeeSkyFlats', '[<expTime>] [<ditherPairs>] [stop] [abort]',
                                 self.doApogeeSkyFlats),
            ('doMangaDither', '[<expTime>] [<dither>] [stop] [abort]',
                              self.doMangaDither),
            ('doMangaSequence', '[<expTime>] [<dithers>] [<count>] [stop] [abort]',
                                self.doMangaSequence),
            ('doApogeeMangaDither', '[<mangaDither>] [<comment>] [stop] [abort]',
                                    self.doApogeeMangaDither),
            ('doApogeeMangaSequence', '[<mangaDithers>] [<count>] [<comment>] [stop] [abort]',
                                      self.doApogeeMangaSequence),
            ('ditheredFlat', '[sp1] [sp2] [<expTime>] [<nStep>] [<nTick>]',
                             self.ditheredFlat),
            ('hartmann', '[<expTime>]', self.hartmann),
            ('collimateBoss', '', self.collimateBoss),
            ('lampsOff', '', self.lampsOff),
            ('ping', '', self.ping),
            ('restart', '[keepQueues]', self.restart),
            ('gotoField', '[<arcTime>] [<flatTime>] [<guiderFlatTime>] [<guiderTime>] [noSlew] '
                          '[noHartmann] [noCalibs] [noGuider] [abort] [keepOffsets]',
                          self.gotoField),
            ('gotoInstrumentChange', '[abort] [stop]', self.gotoInstrumentChange),
            ('gotoStow', '[abort] [stop]', self.gotoStow),
            ('gotoAll60', '[abort] [stop]', self.gotoAll60),
            ('gotoStow60', '[abort] [stop]', self.gotoStow60),
            ('gotoGangChange', '[<alt>] [abort] [stop] [noDomeFlat] [noSlew]',
                               self.gotoGangChange),
            ('doApogeeDomeFlat', '[stop] [abort]', self.doApogeeDomeFlat),
            ('setFakeField', '[<az>] [<alt>] [<rotOffset>]', self.setFakeField),
            ('status', '[geek]', self.status),
            ('reinit', '', self.reinit),
            ('runScript', '<scriptName>', self.runScript),
            ('listScripts', '', self.listScripts),
            ('stopScript', '', self.stopScript)
        ]

    def stop_cmd(self, cmd, cmdState, sopState, name):
        """Stop an active cmdState, failing if there's nothing to stop."""
        if self.modifiable(cmd, cmdState):
            cmdState.abort()
            self.status(cmd, threads=False, finish=True, oneCommand=name)
        else:
            cmd.fail('text="No %s command is active"' % (name))

    def modifiable(self, cmd, cmdState):
        return cmdState.cmd and cmdState.cmd.isAlive()

    def doBossCalibs(self, cmd):
        """ Take a set of calibration frames.

        CmdArgs:
          nbias=N     - the number of biases to take. Taken first. [0]
          ndark=N     - the number of darks to take. Taken after any biases. [0]
          nflat=N     - the number of flats to take. Taken after any darks or biases. [0]
          narc=N      - the number of arcs to take. Taken after any flats, darks, or biases. [0]

          darkTime=S  - override the default dark exposure time. Default depends on survey.
          flatTime=S  - override the default flat exposure time. Default depends on survey.
          arcTime=S   - override the default arc exposure time. Default depends on survey.
          guiderFlatTime=S   - override the default guider flat exposure time.
          """

        sopState = myGlobals.actorState
        cmdState = sopState.doBossCalibs
        keywords = cmd.cmd.keywords
        if self.doing_science(sopState):
            cmd.fail("text='A science exposure sequence is running "
                     '-- will not take calibration frames!')
            return

        if 'abort' in keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'doBossCalibs')
            return

        # Modify running doBossCalibs command
        if self.modifiable(cmd, cmdState):
            if 'nbias' in keywords:
                cmdState.nBias = int(keywords['nbias'].values[0])
            if 'ndark' in keywords:
                cmdState.nDark = int(keywords['ndark'].values[0])
            if 'nflat' in keywords:
                cmdState.nFlat = int(keywords['nflat'].values[0])
            if 'narc' in keywords:
                cmdState.nArc = int(keywords['narc'].values[0])

            if 'darkTime' in keywords:
                cmdState.darkTime = float(keywords['darkTime'].values[0])
            if 'flatTime' in keywords:
                cmdState.flatTime = float(keywords['flatTime'].values[0])
            if 'guiderFlatTime' in keywords:
                cmdState.guiderFlatTime = float(keywords['guiderFlatTime'].values[0])
            if 'arcTime' in keywords:
                cmdState.arcTime = float(keywords['arcTime'].values[0])

            self.status(cmd, threads=False, finish=True, oneCommand='doBossCalibs')
            return

        # Lookup the current cartridge
        survey = sopState.survey
        if survey == sopActor.APOGEE:
            cmd.fail('text="current cartridge is not for BOSS or MaNGA; use bypass '
                     'if you want to force calibrations"')
            return

        cmdState.reinitialize(cmd)
        if 'nbias' in keywords:
            cmdState.nBias = keywords['nbias'].values[0]
        if 'ndark' in keywords:
            cmdState.nDark = keywords['ndark'].values[0]
        if 'nflat' in keywords:
            cmdState.nFlat = keywords['nflat'].values[0]
        if 'narc' in keywords:
            cmdState.nArc = keywords['narc'].values[0]

        cmdState.arcTime = keywords['arcTime'].values[0] \
            if 'arcTime' in keywords else CmdState.getDefaultArcTime(survey)
        if 'darkTime' in keywords:
            cmdState.darkTime = keywords['darkTime'].values[0]
        cmdState.flatTime = keywords['flatTime'].values[0] \
            if 'flatTime' in keywords else CmdState.getDefaultFlatTime(survey)
        if 'guiderFlatTime' in keywords:
            cmdState.guiderFlatTime = keywords['guiderFlatTime'].values[0]

        if cmdState.nArc + cmdState.nBias + cmdState.nDark + cmdState.nFlat == 0:
            cmd.fail('text="You must take at least one arc, bias, dark, or flat exposure"')
            return

        if cmdState.nDark and cmdState.darkTime <= 0:
            cmd.fail('text="darkTime must have a non-zero length"')
            return

        if cmdState.nFlat > 0 and cmdState.guiderFlatTime > 0:
            if sopState.cartridge < 0:
                cmd.warn('text="No cartridge is known to be loaded; not taking guider flats"')
                cmdState.guiderFlatTime = 0

        if 'offset' in keywords:
            cmdState.offset = float(keywords['offset'].values[0])

        activeStages = []
        if cmdState.nBias:
            activeStages.append('bias')
        if cmdState.nDark:
            activeStages.append('dark')
        if cmdState.nFlat:
            activeStages.append('flat')
        if cmdState.nArc:
            activeStages.append('arc')
        activeStages.append('cleanup')  # we always may have to cleanup...
        cmdState.setupCommand(cmd, activeStages)

        sopState.queues[sopActor.MASTER].put(
            Msg.DO_BOSS_CALIBS,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def doBossScience(self, cmd):
        """Take a set of BOSS science frames"""

        sopState = myGlobals.actorState
        cmdState = sopState.doBossScience
        keywords = cmd.cmd.keywords

        if 'abort' in keywords or 'stop' in keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'doBossScience')
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doBossScience command
            if 'nexp' in keywords:
                cmdState.nExp = int(keywords['nexp'].values[0])

            if 'expTime' in keywords:
                cmdState.expTime = float(keywords['expTime'].values[0])

            self.status(cmd, threads=False, finish=True, oneCommand='doBossScience')
            return

        cmdState.cmd = None
        cmdState.reinitialize(cmd)

        # NOTE: TBD: would have to sync with STUI to make nExp have defaults
        # and behave like one would expect it to (see doBossScience_nExp
        # actorkeys value)
        cmdState.nExp = int(keywords['nexp'].values[0]) if 'nexp' in keywords else 1
        expTime = float(keywords['expTime'].values[0]) if 'expTime' in keywords else None
        cmdState.set('expTime', expTime)

        if cmdState.nExp == 0:
            cmd.fail('text="You must take at least one exposure"')
            return
        if cmdState.expTime == 0:
            cmd.fail('text="Exposure time must be greater than 0 seconds."')
            return

        sopState.queues[sopActor.MASTER].put(
            Msg.DO_BOSS_SCIENCE,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def doApogeeScience(self, cmd):
        """Take a sequence of dithered APOGEE science frames, or stop or modify
        a running sequence."""

        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeScience
        keywords = cmd.cmd.keywords
        name = 'doApogeeScience'

        if 'stop' in keywords or 'abort' in keywords:
            self.stop_cmd(cmd, cmdState, sopState, name)
            return

        # Modify running doApogeeScience command
        if self.modifiable(cmd, cmdState):
            if 'ditherPairs' in keywords:
                cmdState.set('ditherPairs', int(keywords['ditherPairs'].values[0]))

            if 'expTime' in keywords:
                cmdState.set('expTime', int(keywords['expTime'].values[0]))

            # update the etr
            cmdState.update_etr()

            self.status(cmd, threads=False, finish=True, oneCommand=name)
            return

        cmdState.reinitialize(cmd)
        ditherPairs = int(keywords['ditherPairs'].values[0]) if 'ditherPairs' in keywords else None
        cmdState.set('ditherPairs', ditherPairs)
        comment = keywords['comment'].values[0] if 'comment' in keywords else None
        cmdState.comment = comment
        expTime = float(keywords['expTime'].values[0]) if 'expTime' in keywords else None
        cmdState.set('expTime', expTime)

        if cmdState.ditherPairs == 0:
            cmd.fail('text="You must take at least one exposure"')
            return

        sopState.queues[sopActor.MASTER].put(
            Msg.DO_APOGEE_EXPOSURES,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def doApogeeSkyFlats(self, cmd):
        """Take a set of APOGEE sky flats, offsetting by 0.01 degree in RA."""

        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeSkyFlats
        keywords = cmd.cmd.keywords
        name = 'doApogeeSkyFlats'

        blocked = self.isSlewingDisabled(cmd)
        if blocked:
            cmd.fail('text=%s' % (qstr('will not take APOGEE sky flats: %s' % (blocked))))
            return

        if 'stop' in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stop_cmd(cmd, cmdState, sopState, name)
            return

        if self.modifiable(cmd, cmdState):
            if 'ditherPairs' in keywords:
                cmdState.set('ditherPairs', int(keywords['ditherPairs'].values[0]))

            if 'expTime' in keywords:
                cmdState.set('expTime', int(keywords['expTime'].values[0]))

            self.status(cmd, threads=False, finish=True, oneCommand=name)
            return
        cmdState.reinitialize(cmd)

        expTime = float(keywords['expTime'].values[0]) if 'expTime' in keywords else None
        cmdState.set('expTime', expTime)
        ditherPairs = int(keywords['ditherPairs'].values[0]) if 'ditherPairs' in keywords else None
        cmdState.set('ditherPairs', ditherPairs)

        if cmdState.ditherPairs == 0:
            cmd.fail('text="You must take at least one exposure"')
            return

        cmdState.setCommandState('running')

        sopState.queues[sopActor.MASTER].put(
            Msg.DO_APOGEE_SKY_FLATS,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def doMangaDither(self, cmd):
        """Take an exposure at a single manga dither position."""
        sopState = myGlobals.actorState
        cmdState = sopState.doMangaDither

        if 'stop' in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'doMangaDither')
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doMangaDither command
            cmd.fail(
                'text="Cannot modify MaNGA dither. If you need to change the dither position, '
                'abort and resubmit."')
            return

        cmdState.reinitialize(cmd)
        dither = cmd.cmd.keywords['dither'].values[0] \
            if 'dither' in cmd.cmd.keywords else None
        cmdState.set('dither', dither)
        expTime = cmd.cmd.keywords['expTime'].values[0] \
            if 'expTime' in cmd.cmd.keywords else None
        cmdState.set('expTime', expTime)

        sopState.queues[sopActor.MASTER].put(
            Msg.DO_MANGA_DITHER,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def doMangaSequence(self, cmd):
        """Take an exposure at a sequence of dither positions, including calibrations."""

        name = 'doMangaSequence'
        sopState = myGlobals.actorState
        cmdState = sopState.doMangaSequence
        keywords = cmd.cmd.keywords

        if 'stop' in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stop_cmd(cmd, cmdState, sopState, name)
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doMangaDither command
            if 'dithers' in keywords:
                newDithers = keywords['dithers'].values[0]
                # if (newDithers != cmdState.dithers):
                #     cmd.fail('text="Cannot modify MaNGA dither pattern, only counts."')
                #     return
                dithers = newDithers

            if 'count' in keywords:
                count = int(keywords['count'].values[0])

            # Updating the dithers, count, and/or ditherSeq with new values
            cmdState.dithers = dithers
            cmdState.count = count
            cmdState.update_ditherSeq()
            cmdState.update_etr()

            if cmdState.index >= len(cmdState.ditherSeq):
                cmd.warn('text="Modified exposure sequence is shorter than position in '
                         'current sequence."')
                cmd.warn('text="Truncating previous exposure sequence, but NOT trying to '
                         'stop current exposure."')
                cmdState.index = len(cmdState.ditherSeq)

            self.status(cmd, threads=False, finish=True, oneCommand=name)
            return

        cmdState.reinitialize(cmd)
        expTime = keywords['expTime'].values[0] if 'expTime' in keywords else None
        cmdState.set('expTime', expTime)
        dither = keywords['dithers'].values[0] if 'dithers' in keywords else None
        cmdState.set('dithers', dither)
        count = keywords['count'].values[0] if 'count' in keywords else None
        cmdState.set('count', count)
        cmdState.reset_ditherSeq()

        sopState.queues[sopActor.MASTER].put(
            Msg.DO_MANGA_SEQUENCE,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def doApogeeMangaDither(self, cmd):
        """Take an exposure at a single manga dither position."""
        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeMangaDither

        if 'stop' in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'doApogeeMangaDither')
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doApogeeMangaDither command
            cmd.fail('text="Cannot modify ApogeeManga dither. '
                     'If you need to change the dither position, abort and resubmit."')
            return

        cmdState.reinitialize(cmd)

        mangaDither = cmd.cmd.keywords['mangaDither'].values[0] \
            if 'mangaDither' in cmd.cmd.keywords else None
        cmdState.set('mangaDither', mangaDither)

        sopState.queues[sopActor.MASTER].put(
            Msg.DO_APOGEEMANGA_DITHER,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def doApogeeMangaSequence(self, cmd):
        """Take an exposure at a sequence of dither positions, including calibrations."""

        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeMangaSequence
        keywords = cmd.cmd.keywords
        name = 'doApogeeMangaSequence'

        if 'stop' in keywords or 'abort' in keywords:
            self.stop_cmd(cmd, cmdState, sopState, name)
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doMangaDither command
            if 'mangaDithers' in keywords:
                newMangaDithers = keywords['mangaDithers'].values[0]
                # if (newMangaDithers != cmdState.mangaDithers):
                #     cmd.fail('text="Cannot modify APOGEE/MaNGA dither pattern, only counts."')
                #     return
                mangaDithers = newMangaDithers

            if 'count' in keywords:
                count = int(keywords['count'].values[0])

            cmdState.mangaDithers = mangaDithers
            cmdState.count = count
            cmdState.update_ditherSeq()
            cmdState.update_etr()

            if cmdState.index >= len(cmdState.mangaDitherSeq):
                cmd.warn('text="Modified exposure sequence is shorter than position '
                         'in current sequence."')
                cmd.warn('text="Truncating previous exposure sequence, '
                         'but NOT trying to stop current exposure."')
                cmdState.index = len(cmdState.mangaDitherSeq)

            self.status(cmd, threads=False, finish=True, oneCommand=name)
            return

        cmdState.reinitialize(cmd)

        mangaDithers = keywords['mangaDithers'].values[0] if 'mangaDithers' in keywords else None
        cmdState.set('mangaDithers', mangaDithers)

        count = keywords['count'].values[0] if 'count' in keywords else None
        cmdState.set('count', count)

        cmdState.reset_ditherSeq()

        sopState.queues[sopActor.MASTER].put(
            Msg.DO_APOGEEMANGA_SEQUENCE,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def lampsOff(self, cmd, finish=True):
        """Turn all the lamps off"""

        sopState = myGlobals.actorState
        sopState.aborting = False

        multiCmd = MultiCommand(cmd, sopState.timeout, None)

        multiCmd.append(sopActor.FF_LAMP, Msg.LAMP_ON, on=False)
        multiCmd.append(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False)
        multiCmd.append(sopActor.NE_LAMP, Msg.LAMP_ON, on=False)
        multiCmd.append(sopActor.WHT_LAMP, Msg.LAMP_ON, on=False)
        multiCmd.append(sopActor.UV_LAMP, Msg.LAMP_ON, on=False)

        if multiCmd.run():
            if finish:
                cmd.finish('text="Turned lamps off"')
        else:
            if finish:
                cmd.fail('text="Some lamps failed to turn off"')

    def bypass(self, cmd):
        """Ignore errors in a subsystem, or force a system to be in a given state."""
        subSystems = cmd.cmd.keywords['subSystem'].values
        doBypass = False if 'clear' in cmd.cmd.keywords else True

        sopState = myGlobals.actorState
        bypass = myGlobals.bypass

        for subSystem in subSystems:
            if bypass.set(subSystem, doBypass) is None:
                cmd.fail(
                    'text="{} is not a recognised and bypassable subSystem"'.format(subSystem))
                return
            if bypass.is_cart_bypass(subSystem):
                self.updateCartridge(
                    sopState.cartridge,
                    sopState.plateType,
                    sopState.surveyModeName,
                    status=False,
                    bypassed=True)
                cmdStr = 'setRefractionBalance plateType="{0}" surveyMode="{1}"'.format(
                    *sopState.surveyText)
                cmdVar = sopState.actor.cmdr.call(actor='guider', forUserCmd=cmd, cmdStr=cmdStr)
                if cmdVar.didFail:
                    cmd.fail(
                        'text="Failed to set guider refraction balance for bypass {0} {1}'.format(
                            subSystem, doBypass))
                    return
            if bypass.is_gang_bypass(subSystem):
                cmd.warn('text="gang bypassed: {}"'.format(sopState.apogeeGang.getPos()))

        self.status(cmd, threads=False)

    def setFakeField(self, cmd):
        """ (Re)set the position gotoField slews to if the slewToField bypass is set.

        The az and alt are used directly. RotOffset is added to whatever obj offset is calculated
        for the az and alt.

        Leaving any of the az, alt, and rotOffset arguments off will set them to the default,
        which is 'here'.
        """

        sopState = myGlobals.actorState

        cmd.warn('text="cmd=%s"' % (cmd.cmd.keywords))

        sopState.gotoField.fakeAz = float(
            cmd.cmd.keywords['az'].values[0]) if 'az' in cmd.cmd.keywords else None
        sopState.gotoField.fakeAlt = float(
            cmd.cmd.keywords['alt'].values[0]) if 'alt' in cmd.cmd.keywords else None
        sopState.gotoField.fakeRotOffset = float(
            cmd.cmd.keywords['rotOffset'].values[0]) if 'rotOffset' in cmd.cmd.keywords else 0.0

        cmd.finish('text="set fake slew position to az=%s alt=%s rotOffset=%s"' %
                   (sopState.gotoField.fakeAz, sopState.gotoField.fakeAlt,
                    sopState.gotoField.fakeRotOffset))

    def ditheredFlat(self, cmd, finish=True):
        """Take a set of nStep dithered flats, moving the collimator by nTick between exposures"""

        sopState = myGlobals.actorState

        if self.doing_science():
            cmd.fail(
                "text='A science exposure sequence is running -- will not start dithered flats!")
            return

        sopState.aborting = False

        spN = []
        all = [
            'sp1',
            'sp2',
        ]
        for s in all:
            if s in cmd.cmd.keywords:
                spN += [s]

        if not spN:
            spN = all

        nStep = int(cmd.cmd.keywords['nStep'].values[0]) if 'nStep' in cmd.cmd.keywords else 22
        nTick = int(cmd.cmd.keywords['nTick'].values[0]) if 'nTick' in cmd.cmd.keywords else 62
        expTime = float(
            cmd.cmd.keywords['expTime'].values[0]) if 'expTime' in cmd.cmd.keywords else 30

        sopState.queues[sopActor.MASTER].put(
            Msg.DITHERED_FLAT,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            expTime=expTime,
            spN=spN,
            nStep=nStep,
            nTick=nTick)

    def hartmann(self, cmd, finish=True):
        """
        Take two arc exposures, one with the Hartmann left screen in
        and one with the right one in.

        If the flat field screens are initially open they are closed,
        and the Ne/HgCd lamps are turned on. You may specify using
        only one spectrograph with sp1 or sp2; the default is both.
        The exposure time is set by expTime

        When the sequence is finished the Hartmann screens are moved
        out of the beam, the lamps turned off, and the flat field
        screens returned to their initial state.
        """
        sopState = myGlobals.actorState
        cmdState = sopState.hartmann

        if self.doing_science(sopState):
            cmd.fail("text='A science exposure sequence is running -- "
                     'will not start a hartmann sequence!')
            return

        cmdState.reinitialize(cmd, output=False)

        expTime = float(cmd.cmd.keywords['expTime'].values[0]) \
            if 'expTime' in cmd.cmd.keywords else CmdState.getDefaultArcTime(sopActor.BOSS)
        cmdState.expTime = expTime

        sopState.queues[sopActor.MASTER].put(
            Msg.HARTMANN, cmd, replyQueue=self.replyQueue, actorState=sopState, cmdState=cmdState)

    def collimateBoss(self, cmd):
        """
        Warm up Ne/HgCd lamps, and take left/right full hartmanns to collimate
        the BOSS spectrographs, ignoring any remaining blue residuals.
        """
        sopState = myGlobals.actorState
        cmdState = sopState.collimateBoss

        if self.doing_science(sopState):
            cmd.fail("text='A science exposure sequence is running -- "
                     'will not start a hartmann sequence!')
            return

        cmdState.reinitialize(cmd, output=False)

        sopState.queues[sopActor.MASTER].put(
            Msg.COLLIMATE_BOSS,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def gotoField(self, cmd):
        """Slew to the current cartridge/pointing

        Slew to the position of the currently loaded cartridge. At the
        beginning of the slew all the lamps are turned on and the flat
        field screen petals are closed.  When you arrive at the field,
        all the lamps are turned off again and the flat field petals
        are opened if you specified openFFS.
        """

        sopState = myGlobals.actorState
        survey = sopState.survey
        cmdState = sopState.gotoField
        keywords = cmd.cmd.keywords

        if self.doing_science(sopState):
            cmd.fail("text='A science exposure sequence is running -- will not go to field!")
            return

        if 'abort' in keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'gotoField')
            return

        # Modify running gotoField command
        if self.modifiable(cmd, cmdState):
            cmdState.doSlew = True if 'noSlew' not in keywords else False
            cmdState.doGuider = True if 'noGuider' not in keywords else False
            cmdState.doHartmann = True if 'noHartmann' not in keywords else False

            # NOTE: TBD: Need a full set of test cases for this...

            dropCalibs = False
            if 'noCalibs' in keywords:
                if cmdState.didFlat or cmdState.didArc:
                    cmd.warn('text="Some cals have been taken; it\'s too late to disable them."')
                else:
                    dropCalibs = True
            if 'arcTime' in keywords:
                if cmdState.didArc:
                    cmd.warn('text="Arcs are taken; it\'s too late to modify arcTime"')
                else:
                    cmdState.arcTime = float(keywords['arcTime'].values[0])
            if 'flatTime' in keywords:
                if cmdState.didFlat:
                    cmd.warn('text="Flats are taken; it\'s too late to modify flatTime"')
                else:
                    cmdState.flatTime = float(keywords['flatTime'].values[0])
            if 'guiderFlatTime' in keywords:
                cmdState.guiderFlatTime = float(keywords['guiderFlatTime'].values[0])
            if 'guiderTime' in keywords:
                cmdState.guiderTime = float(keywords['guiderTime'].values[0])

            # * TBD: WARNING! This does not keep track of what's already been done,
            # * except for the dropCalibs bit above.
            cmdState.doCalibs = not dropCalibs
            cmdState.setStageState('slew', 'pending' if cmdState.doSlew else 'off')
            cmdState.setStageState('hartmann', 'pending' if cmdState.doHartmann else 'off')
            cmdState.setStageState('calibs', 'pending' if not dropCalibs else 'off')
            cmdState.setStageState('guider', 'pending' if cmdState.doGuider else 'off')

            self.status(cmd, threads=False, finish=True, oneCommand='gotoField')
            return

        cmdState.reinitialize(cmd, output=False)

        cmdState.doSlew = 'noSlew' not in keywords
        cmdState.doGuider = 'noGuider' not in keywords
        cmdState.doCalibs = ('noCalibs' not in keywords and survey != sopActor.APOGEE)
        cmdState.doHartmann = ('noHartmann' not in keywords and survey != sopActor.APOGEE)
        if cmdState.doCalibs:
            if 'arcTime' in keywords:
                cmdState.arcTime = float(keywords['arcTime'].values[0])
            else:
                cmdState.arcTime = CmdState.getDefaultArcTime(survey)
            if 'flatTime' in keywords:
                cmdState.flatTime = float(keywords['flatTime'].values[0])
            else:
                cmdState.flatTime = CmdState.getDefaultFlatTime(survey)
            if cmdState.arcTime <= 0:
                cmd.warn('text="GotoField arcTime is not a positive number: '
                         'are you sure you want that?"')
            if cmdState.flatTime <= 0:
                cmd.warn('text="GotoField flatTime is not a positive number: '
                         'are you sure you want that?"')
        if cmdState.doGuider:
            cmdState.guiderFlatTime = float(
                keywords['guiderFlatTime'].values[0]) if 'guiderFlatTime' in keywords else 0.5
            cmdState.guiderTime = float(keywords['guiderTime'].values[0]) \
                if 'guiderTime' in keywords else 5
            cmdState.doGuiderFlat = cmdState.guiderFlatTime > 0
            cmdState.keepOffsets = 'keepOffsets' in keywords
        else:
            cmdState.doGuiderFlat = False

        if survey == sopActor.UNKNOWN:
            cmd.warn('text="No cartridge is known to be loaded; disabling guider"')
            cmdState.doGuider = False
            cmdState.doGuiderFlat = False

        if cmdState.doSlew:
            pointingInfo = sopState.models['platedb'].keyVarDict['pointingInfo']
            cmdState.ra = pointingInfo[3]
            cmdState.dec = pointingInfo[4]
            cmdState.rotang = 0.0  # Rotator angle; should always be 0.0

        if myGlobals.bypass.get(name='slewToField'):
            fakeSkyPos = obs2Sky(cmd, cmdState.fakeAz, cmdState.fakeAlt, cmdState.fakeRotOffset)
            cmdState.ra = fakeSkyPos[0]
            cmdState.dec = fakeSkyPos[1]
            cmdState.rotang = fakeSkyPos[2]
            cmd.warn('text="Bypass slewToField is FAKING RA DEC:  %g, %g /rotang=%g"' %
                     (cmdState.ra, cmdState.dec, cmdState.rotang))

        # Junk!! Must keep this in one place! Adjustment will be ugly
        # otherwise.
        activeStages = []
        if cmdState.doSlew:
            activeStages.append('slew')
        if cmdState.doHartmann:
            activeStages.append('hartmann')
        if cmdState.doCalibs:
            activeStages.append('calibs')
        if cmdState.doGuider:
            activeStages.append('guider')
        activeStages.append('cleanup')  # we always may have to cleanup...
        cmdState.setupCommand(cmd, activeStages)

        sopState.queues[sopActor.MASTER].put(
            Msg.GOTO_FIELD,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def gotoPosition(self, cmd, cmdState, name, az=None, alt=None, rot=None):
        """Goto a specified alt/az/[rot] position, named 'name'."""

        sopState = myGlobals.actorState
        cmdState = cmdState or sopState.gotoPosition
        keywords = cmd.cmd.keywords

        blocked = self.isSlewingDisabled(cmd)
        if blocked:
            cmd.fail('text=%s' % (qstr('will not {0}: {1}'.format(name, blocked))))
            return

        if 'stop' in keywords or 'abort' in keywords:
            self.stop_cmd(cmd, cmdState, sopState, name)
            return

        if self.modifiable(cmd, cmdState):
            # Modify running gotoPosition command
            cmd.fail('text="Cannot modify {0}."'.format(name))
            return

        cmdState.reinitialize(cmd, output=False)
        cmdState.set('alt', alt or cmdState.alt)
        cmdState.set('az', az or cmdState.az)
        cmdState.set('rot', rot or cmdState.rot)

        activeStages = ['slew']
        cmdState.setupCommand(cmd, activeStages)

        sopState.queues[sopActor.SLEW].put(
            Msg.GOTO_POSITION,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def gotoInstrumentChange(self, cmd):
        """Go to the instrument change position: alt=90 az=121 rot=0"""

        cmdState = myGlobals.actorState.gotoInstrumentChange
        self.gotoPosition(cmd, cmdState, 'gotoInstrumentChange')

    def gotoStow(self, cmd):
        """Go to the stow position: alt=30, az=121, rot=0"""

        cmdState = myGlobals.actorState.gotoStow
        self.gotoPosition(cmd, cmdState, 'gotoStow')

    def gotoAll60(self, cmd):
        """Go to the startup check position: alt=60, az=60, rot=60"""

        self.gotoPosition(cmd, None, 'stow', 60, 60, 60)

    def gotoStow60(self, cmd):
        """Go to the resting position: alt=60, az=121, rot=0"""

        self.gotoPosition(cmd, None, 'stow', 121, 60, 0)

    def gotoGangChange(self, cmd):
        """Go to the gang connector change position"""

        sopState = myGlobals.actorState
        cmdState = sopState.gotoGangChange
        keywords = cmd.cmd.keywords

        blocked = self.isSlewingDisabled(cmd)
        if blocked:
            cmd.fail('text=%s' % (qstr('will not go to gang change: %s' % (blocked))))
            return

        if 'stop' in keywords or 'abort' in keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'gotoGangChange')
            return

        if self.modifiable(cmd, cmdState):
            # Modify running gotoGangChange command
            cmd.fail('text="Cannot modify gotoGangChange."')
            return

        cmdState.reinitialize(cmd, output=False)
        alt = keywords['alt'].values[0] if 'alt' in keywords else None
        cmdState.set('alt', alt)
        cmdState.doSlew = 'noSlew' not in keywords
        cmdState.doDomeFlat = 'noDomeFlat' not in keywords

        activeStages = []
        if cmdState.doSlew:
            activeStages.append('slew')
        if cmdState.doDomeFlat:
            activeStages.append('domeFlat')
        cmdState.setupCommand(cmd, activeStages)

        sopState.queues[sopActor.SLEW].put(
            Msg.GOTO_GANG_CHANGE,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState)

    def doApogeeDomeFlat(self, cmd):
        """Take an APOGEE dome flat, with FFS closed and FFlamps on."""
        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeDomeFlat

        if self.doing_science(sopState):
            cmd.fail("text='A science exposure sequence is running -- will not take a dome flat!")
            return

        if 'stop' in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'doApogeeDomeFlat')
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doApogeeDomeFlat command
            cmd.fail('text="Cannot modify doApogeeDomeFlat."')
            return

        cmdState.reinitialize(cmd)

        sopState.queues[sopActor.SLEW].put(
            Msg.DO_APOGEE_DOME_FLAT,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            cmdState=cmdState,
            survey=sopState.survey)

    def runScript(self, cmd):
        """ Run the named script from the SOPACTOR_DIR/scripts directory. """
        sopState = myGlobals.actorState

        sopState.queues[sopActor.SCRIPT].put(
            Msg.NEW_SCRIPT,
            cmd,
            replyQueue=self.replyQueue,
            actorState=sopState,
            survey=sopState.survey,
            scriptName=cmd.cmd.keywords['scriptName'].values[0])

    def listScripts(self, cmd, finish=True):
        """ List available script names for the runScript command."""

        path = os.path.join(os.environ['SOPACTOR_DIR'], 'scripts', '*.inp')
        scripts = glob.glob(path)
        scripts = ','.join(os.path.splitext(os.path.basename(s))[0] for s in scripts)
        cmd.inform('availableScripts="%s"' % scripts)

        if finish:
            cmd.finish('')

    def stopScript(self, cmd):
        """Stops any running script."""

        sopState = myGlobals.actorState
        sopState.queues[sopActor.SCRIPT].put(Msg.STOP_SCRIPT, cmd, replyQueue=self.replyQueue)

    def ping(self, cmd):
        """ Query sop for liveness/happiness. """

        cmd.finish('text="Yawn; how soporific"')

    def restart(self, cmd):
        """Restart the worker threads"""

        sopState = myGlobals.actorState

        keepQueues = True if 'keepQueues' in cmd.cmd.keywords else False

        if sopState.restartCmd:
            sopState.restartCmd.finish("text=\"secundum verbum tuum in pace\"")
            sopState.restartCmd = None
        #
        # We can't finish this command now as the threads may not have died yet,
        # but we can remember to clean up _next_ time we restart
        #
        cmd.inform("text=\"Restarting threads\"")
        sopState.restartCmd = cmd

        sopState.actor.startThreads(sopState, cmd, restart=True, restartQueues=not keepQueues)

    def reinit(self, cmd):
        """ (engineering command) Recreate the objects which hold the state of
        the various top-level commands. """

        cmd.inform('text="recreating command objects"')
        try:
            self.initCommands()
        except Exception as e:
            cmd.fail('text="failed to re-initialize command state: %s"' % e)
            return

        cmd.finish('')

    def isSlewingDisabled(self, cmd):
        """Return False if we can slew, otherwise return a string describing why we cannot."""
        sopState = myGlobals.actorState

        bossCalibs_disabled = sopState.doBossCalibs.isSlewingDisabled()
        if bossCalibs_disabled:
            return bossCalibs_disabled

        if sopState.survey == sopActor.BOSS:
            return sopState.doBossScience.isSlewingDisabled()

        elif sopState.survey == sopActor.MANGA:
            disabled1 = sopState.doMangaDither.isSlewingDisabled()
            disabled2 = sopState.doMangaSequence.isSlewingDisabled()
            return disabled1 if disabled1 else disabled2

        elif sopState.survey == sopActor.APOGEE:
            disabled1 = sopState.doApogeeScience.isSlewingDisabled()
            disabled2 = sopState.doApogeeSkyFlats.isSlewingDisabled()
            return disabled1 if disabled1 else disabled2

        elif sopState.survey == sopActor.APOGEEMANGA:
            disabled1 = sopState.doApogeeMangaDither.isSlewingDisabled()
            disabled2 = sopState.doApogeeMangaSequence.isSlewingDisabled()
            return disabled1 if disabled1 else disabled2

        return False

    def status(self, cmd, threads=False, finish=True, oneCommand=None):
        """Return sop status.

        If threads is true report on SOP's threads; (also if geek in cmd.keywords)
        If finish complete the command.
        Trim output to contain just keys relevant to oneCommand.
        """

        sopState = myGlobals.actorState
        bypass = myGlobals.bypass

        self.actor.sendVersionKey(cmd)

        if hasattr(cmd, 'cmd') and cmd.cmd is not None and 'geek' in cmd.cmd.keywords:
            threads = True
            for t in threading.enumerate():
                cmd.inform('text="%s"' % t)

        bypassNames, bypassStates = bypass.get_bypass_list()
        cmd.inform('bypassNames=' + ', '.join(bypassNames))
        bypassed = bypass.get_bypassedNames()
        txt = 'bypassedNames=' + ', '.join(bypassed)
        # output non-empty bypassedNames as a warning, per #2187.
        if bypassed == []:
            cmd.inform(txt)
        else:
            cmd.warn(txt)
        cmd.inform('text="apogeeGang: %s"' % (sopState.apogeeGang.getPos()))

        cmd.inform('surveyCommands=' + ', '.join(sopState.validCommands))

        # major commands
        sopState.gotoField.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doBossCalibs.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doBossScience.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doMangaDither.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doMangaSequence.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doApogeeMangaDither.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doApogeeMangaSequence.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doApogeeScience.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doApogeeSkyFlats.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.gotoGangChange.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doApogeeDomeFlat.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.hartmann.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.collimateBoss.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.gotoPosition.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.gotoInstrumentChange.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.gotoStow.genKeys(cmd=cmd, trimKeys=oneCommand)

        # TBD: threads arg is only used with "geek" option, apparently?
        # TBD: I guess its useful for live debugging of the threads.
        if threads:
            try:
                sopState.ignoreAborting = True
                getStatus = MultiCommand(cmd, 5.0, None)

                for tid in sopState.threads.keys():
                    getStatus.append(tid, Msg.STATUS)

                if not getStatus.run():
                    if finish:
                        cmd.fail('')
                        return
                    else:
                        cmd.warn('')
            finally:
                sopState.ignoreAborting = False

        # Outputs available scripts
        self.listScripts(cmd, finish=False)

        if finish:
            cmd.finish('')

    def initCommands(self):
        """Recreate the objects that hold the state of the various commands."""
        sopState = myGlobals.actorState

        sopState.gotoField = CmdState.GotoFieldCmd()
        sopState.doBossCalibs = CmdState.DoBossCalibsCmd()
        sopState.doBossScience = CmdState.DoBossScienceCmd()
        sopState.doMangaDither = CmdState.DoMangaDitherCmd()
        sopState.doMangaSequence = CmdState.DoMangaSequenceCmd()
        sopState.doApogeeMangaDither = CmdState.DoApogeeMangaDitherCmd()
        sopState.doApogeeMangaSequence = CmdState.DoApogeeMangaSequenceCmd()
        sopState.doApogeeScience = CmdState.DoApogeeScienceCmd()
        sopState.doApogeeSkyFlats = CmdState.DoApogeeSkyFlatsCmd()
        sopState.gotoGangChange = CmdState.GotoGangChangeCmd()
        sopState.gotoPosition = CmdState.GotoPositionCmd()
        sopState.gotoInstrumentChange = CmdState.GotoInstrumentChangeCmd()
        sopState.gotoStow = CmdState.GotoStowCmd()
        sopState.doApogeeDomeFlat = CmdState.DoApogeeDomeFlatCmd()
        sopState.hartmann = CmdState.HartmannCmd()
        sopState.collimateBoss = CmdState.CollimateBossCmd()

        self.updateCartridge(-1, 'UNKNOWN', 'None')
        sopState.guiderState.setLoadedNewCartridgeCallback(self.updateCartridge)

    def updateCartridge(self, cartridge, plateType, surveyModeName, status=True, bypassed=False):
        """
        Read the guider's notion of the loaded cartridge and configure ourselves appropriately.

        Args:
            cartridge (int): Cartridge ID number
            plateType (str): plateType keyword from the guider, used as a lookup into survey_dict
            surveyModeName (str): surveyMode keyword from the guider, used as a lookup into
                                  surveyMode_dict

        Kwargs:
            status (bool): Output status when done?
            bypassed (bool): Were we set via a bypass? If not, clear cart bypasses before
                             doing anything else.
        """

        # clear cart bypasses on load cartridge, per #2284
        if not bypassed:
            myGlobals.bypass.clear_cart_bypasses()

        sopState = myGlobals.actorState
        cmd = sopState.actor.bcast

        sopState.cartridge = cartridge
        # save these for when someone sets a bypass.
        sopState.plateType = plateType
        sopState.surveyModeName = surveyModeName

        self.classifyCartridge(cmd, cartridge, plateType, surveyModeName)
        surveyMode = sopState.surveyMode
        survey = sopState.survey

        cmd.warn('text="loadCartridge fired cart=%s survey=%s surveyMode=%s"' % (cartridge, survey,
                                                                                 surveyMode))
        cmd.inform('survey={0},{1}'.format(*[qstr(x) for x in sopState.surveyText]))

        sopState.validCommands = [
            'gotoField', 'gotoStow', 'gotoInstrumentChange', 'gotoAll60', 'gotoStow60'
        ]
        if survey is sopActor.BOSS:
            sopState.gotoField.setStages(['slew', 'hartmann', 'calibs', 'guider', 'cleanup'])
            sopState.validCommands += [
                'doBossCalibs',
                'doBossScience',
            ]
        elif survey is sopActor.APOGEE:
            apogeeDesign, __ = self.update_designs(sopState)
            sopState.doApogeeScience.set_apogee_expTime(apogeeDesign[1])
            sopState.gotoField.setStages(['slew', 'guider', 'cleanup'])
            sopState.validCommands += [
                'doApogeeScience', 'doApogeeSkyFlats', 'gotoGangChange', 'doApogeeDomeFlat'
            ]
        elif survey is sopActor.MANGA:
            sopState.gotoField.setStages(['slew', 'hartmann', 'calibs', 'guider', 'cleanup'])
            sopState.validCommands += [
                'doBossCalibs',
                'doMangaDither',
                'doMangaSequence',
            ]
            if surveyMode is sopActor.MANGADITHER:
                sopState.doMangaSequence.set_mangaDither()
            if surveyMode is sopActor.MANGA10:
                sopState.doMangaDither.set_manga10()
                sopState.doMangaSequence.set_manga10()
            if surveyMode is sopActor.MANGASTARE or surveyMode is sopActor.MASTAR:
                __, mangaExpTime = self.update_designs(sopState)
                sopState.doMangaDither.set_mangaStare(expTime=mangaExpTime)
                sopState.doMangaSequence.set_mangaStare(expTime=mangaExpTime)
        elif survey is sopActor.APOGEEMANGA:
            sopState.gotoField.setStages(['slew', 'hartmann', 'calibs', 'guider', 'cleanup'])
            sopState.validCommands += [
                'doBossCalibs', 'doApogeeMangaDither', 'doApogeeMangaSequence', 'doApogeeSkyFlats',
                'gotoGangChange', 'doApogeeDomeFlat'
            ]
            if surveyMode is sopActor.APOGEELEAD or surveyMode is None:
                apogeeDesign, mangaExpTime = self.update_designs(sopState)
                sopState.doApogeeMangaDither.set_apogeeLead(apogeeExpTime=apogeeDesign[1],
                                                            mangaExpTime=mangaExpTime)
                sopState.doApogeeMangaSequence.set_apogeeLead(apogeeExpTime=apogeeDesign[1],
                                                              mangaExpTime=mangaExpTime)
                sopState.doApogeeScience.set_apogee_expTime(apogeeDesign[1])
            if surveyMode is sopActor.MANGADITHER:
                sopState.doApogeeMangaDither.set_manga()
                sopState.doApogeeMangaSequence.set_mangaDither()
            if surveyMode is sopActor.MANGA10:
                sopState.doApogeeMangaDither.set_manga10()
                sopState.doApogeeMangaSequence.set_manga10()
            if surveyMode is sopActor.MANGASTARE or surveyMode is sopActor.MASTAR:
                __, mangaExpTime = self.update_designs(sopState)
                sopState.doApogeeMangaDither.set_manga(mangaExpTime=mangaExpTime)
                sopState.doApogeeMangaSequence.set_mangaStare(mangaExpTime=mangaExpTime)
        else:
            sopState.gotoField.setStages(['slew', 'guider', 'cleanup'])

        # If MaStar, sets doBossCalibs for post-cals.
        if surveyMode is sopActor.MASTAR:
            sopState.doBossCalibs.nFlat = 1
            sopState.doBossCalibs.nArc = 1
            sopState.doBossCalibs.set('offset', 16)

        if status:
            self.status(cmd, threads=False, finish=False)

    def update_designs(self, sopState):
        """Update the APOGEE design parameters, including expTime, from the platedb keyword."""

        apogeeDesign = sopState.models['platedb'].keyVarDict['apogeeDesign']
        mangaExpTime = sopState.models['platedb'].keyVarDict['mangaExposureTime'][0]

        return (apogeeDesign, None if mangaExpTime == -1 else mangaExpTime)

    def update_plugged_instruments(self, sopState):
        ''' Update the plugged instrument from the platedb keyword '''
        pluggedInstruments = sopState.models['platedb'].keyVarDict['pluggedInstruments']
        sopState.pluggedInstruments = pluggedInstruments.getValue()

    def survey_bypasses(self, cmd, sopState):
        """Set survey/surveyMode if a bypass is set and return True if so."""

        bypass = myGlobals.bypass
        sopState.survey = None
        sopState.surveyMode = None

        if bypass.get('isBoss'):
            cmd.warn('text="We are lying about this being a BOSS cartridge"')
            sopState.survey = sopActor.BOSS
            sopState.surveyMode = None
        elif bypass.get('isApogee'):
            cmd.warn('text="We are lying about this being an APOGEE cartridge"')
            sopState.survey = sopActor.APOGEE
            sopState.surveyMode = None
        elif bypass.get('isMangaStare'):
            cmd.warn('text="We are lying about this being a MaNGA Stare cartridge"')
            sopState.survey = sopActor.MANGA
            sopState.surveyMode = sopActor.MANGASTARE
        elif bypass.get('isMaStar'):
            cmd.warn('text="We are lying about this being a MaNGA MaStar cartridge"')
            sopState.survey = sopActor.MANGA
            sopState.surveyMode = sopActor.MASTAR
        elif bypass.get('isMangaDither'):
            cmd.warn('text="We are lying about this being a MaNGA Dither cartridge"')
            sopState.survey = sopActor.MANGA
            sopState.surveyMode = sopActor.MANGADITHER
        elif bypass.get('isManga10'):
            cmd.warn('text="We are lying about this being a MaNGA 10min cartridge"')
            sopState.survey = sopActor.MANGA
            sopState.surveyMode = sopActor.MANGA10
        elif bypass.get('isApogeeMangaStare'):
            cmd.warn('text="We are lying about this being an APOGEE&MaNGA Stare cartridge"')
            sopState.survey = sopActor.APOGEEMANGA
            sopState.surveyMode = sopActor.MANGASTARE
        elif bypass.get('isApogeeMangaMaStar'):
            cmd.warn('text="We are lying about this being a MaStar cartridge"')
            sopState.survey = sopActor.APOGEEMANGA
            sopState.surveyMode = sopActor.MASTAR
        elif bypass.get('isApogeeMangaDither'):
            cmd.warn('text="We are lying about this being a APOGEE&MaNGA Dither cartridge"')
            sopState.survey = sopActor.APOGEEMANGA
            sopState.surveyMode = sopActor.MANGADITHER
        elif bypass.get('isApogeeManga10'):
            cmd.warn('text="We are lying about this being a APOGEE&MaNGA 10min cartridge"')
            sopState.survey = sopActor.APOGEEMANGA
            sopState.surveyMode = sopActor.MANGA10
        elif bypass.get('isApogeeLead'):
            cmd.warn('text="We are lying about this being an APOGEE&MaNGA, APOGEE Lead cartridge"')
            sopState.survey = sopActor.APOGEEMANGA
            sopState.surveyMode = sopActor.APOGEELEAD

        return sopState.survey is not None

    def classifyCartridge(self, cmd, cartridge, plateType, surveyMode):
        """
        Set the survey and surveyMode for this cartridge in actorState.

        Args:
            cmd (Cmdr): Cmdr to send output to.
            cartridge (int): Cartridge ID number
            plateType (str): plateType keyword from the guider, used as a lookup into survey_dict
            surveyModeName (str): surveyMode keyword from the guider, used as a lookup into
                                  surveyMode_dict
        """

        def update_surveyText():
            sopState.surveyText = [
                survey_inv_dict[sopState.survey], surveyMode_inv_dict[sopState.surveyMode]
            ]

        sopState = myGlobals.actorState
        sopState.surveyText = ['', '']

        if self.survey_bypasses(cmd, sopState):
            update_surveyText()
            return

        if cartridge <= 0:
            cmd.warn('text="We do not have a valid cartridge (id=%s)"' % (cartridge))
            sopState.survey = sopActor.UNKNOWN
            sopState.surveyMode = None
            update_surveyText()
            return

        # NOTE: don't use .get() here to send an error message if the key lookup fails.
        # Set surveyText explicitly to exactly match the guider output.
        try:
            sopState.survey = survey_dict[plateType]
            sopState.surveyText[0] = plateType
        except KeyError:
            cmd.error('text=%s' % qstr('Do not understand plateType: %s.' % plateType))
            sopState.survey = sopActor.UNKNOWN
            sopState.surveyText[0] = survey_inv_dict[sopState.survey]

        try:
            sopState.surveyMode = surveyMode_dict[surveyMode]
            sopState.surveyText[1] = surveyMode
        except KeyError:
            cmd.error('text=%s' % qstr('Do not understand surveyMode: %s.' % surveyMode))
            sopState.surveyMode = None
            sopState.surveyText[1] = surveyMode_inv_dict[sopState.surveyMode]

        # #2453 Check which instruments were plugged for APOGEE-MaNGA plates
        self.update_plugged_instruments(sopState)
        if sopState.survey == sopActor.APOGEEMANGA:
            if sopState.pluggedInstruments == ('APOGEE', ):
                sopState.survey = sopActor.APOGEE
                sopState.surveyMode = None
                update_surveyText()
            elif sopState.pluggedInstruments == ('BOSS', ):
                sopState.survey = sopActor.MANGA
                sopState.surveyMode = sopActor.MANGADITHER
                update_surveyText()
            elif not sopState.pluggedInstruments:
                sopState.survey = sopActor.UNKNOWN
                sopState.surveyMode = None
                update_surveyText()

    def doing_science(self, sopState):
        """Return True if any sort of science command is currently running."""
        return (
            (sopState.doBossScience.cmd and sopState.doBossScience.cmd.isAlive()) or
            (sopState.doApogeeScience.cmd and sopState.doApogeeScience.cmd.isAlive()) or
            (sopState.doMangaDither.cmd and sopState.doMangaDither.cmd.isAlive()) or
            (sopState.doMangaSequence.cmd and sopState.doMangaSequence.cmd.isAlive()) or
            (sopState.doApogeeMangaDither.cmd and sopState.doApogeeMangaDither.cmd.isAlive()) or
            (sopState.doApogeeMangaSequence.cmd and sopState.doApogeeMangaSequence.cmd.isAlive()))


def obs2Sky(cmd, az=None, alt=None, rotOffset=0.0):
    """Return ra, dec, rot for the current telescope position, for fake slews."""

    tccDict = myGlobals.actorState.models['tcc'].keyVarDict
    axePos = tccDict['axePos']
    gotoAz = az if az is not None else axePos[0]
    gotoAlt = alt if alt is not None else axePos[1]

    cmd.warn('text="FAKING slew position from az, alt, and rotator offset: %0.1f %0.1f %0.1f"' %
             (gotoAz, gotoAlt, rotOffset))
    cmdVar = myGlobals.actorState.actor.cmdr.call(
        actor='tcc', forUserCmd=cmd, cmdStr=('convert %0.5f,%0.6f obs icrs' % (gotoAz, gotoAlt)))
    if cmdVar.didFail:
        return 0, 0, 0
    else:
        convPos = tccDict['convPos']
        convAng = tccDict['convAng']

        rotPos = 180.0 - convAng[0].getPos()
        rotPos += rotOffset

        # I think I need to do _something with the axePos rotation angle.
        return convPos[0].getPos(), convPos[1].getPos(), rotPos


def _set_mastar_postcals(cmd):
    """Sets doBossCalibs for MaStar post-cals."""

    sopState = myGlobals.actorState

    # Resets command state
    sopState.doBossCalibs = CmdState.DoBossCalibsCmd()

    sopState.doBossCalibs.offset = 16
    sopState.doBossCalibs.nFlat = 1
    sopState.doBossCalibs.nArc = 1
