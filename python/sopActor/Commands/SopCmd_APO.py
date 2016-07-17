# !usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2016-06-10 13:00:58
# @Last modified by:   Brian
# @Last Modified time: 2016-06-10 13:04:25

from __future__ import print_function, division, absolute_import

import opscore.protocols.keys as keys
import opscore.protocols.types as types

import sopActor
from sopActor import CmdState, Msg
from sopActor.Commands import SopCmd
import sopActor.myGlobals as myGlobals
from sopActor.multiCommand import MultiCommand


class SopCmd_APO(SopCmd.SopCmd):

    def __init__(self, actor):

        # initialize from the superclass
        super(SopCmd_APO, self).__init__(actor)

        # Define APO specific keys.
        self.keys.extend([
            keys.Key('narc', types.Int(), help='Number of arcs to take'),
            keys.Key('nbias', types.Int(), help='Number of biases to take'),
            keys.Key('ndark', types.Int(), help='Number of darks to take'),
            keys.Key('nexp', types.Int(), help='Number of exposures to take'),
            keys.Key('nflat', types.Int(), help='Number of flats to take'),
            keys.Key('arcTime', types.Float(), help='Exposure time for arcs'),
            keys.Key('darkTime', types.Float(), help='Exposure time for flats'),
            keys.Key('flatTime', types.Float(), help='Exposure time for flats'),
            keys.Key('test', help='Assert that the exposures are '
                                  'not expected to be meaningful'),
            keys.Key('guiderFlatTime', types.Float(), help='Exposure time '
                                                           'for guider flats'),
            keys.Key('sp1', help='Select SP1'),
            keys.Key('sp2', help='Select SP2'),
            keys.Key('nStep', types.Int(), help='Number of dithered '
                                                'exposures to take'),
            keys.Key('nTick', types.Int(), help='Number of ticks '
                                                'to move collimator'),
            keys.Key('dither', types.String(), help='MaNGA dither position '
                                                    'for a single dither.'),
            keys.Key('dithers', types.String(), help='MaNGA dither positions '
                                                     'for a dither sequence.'),
            keys.Key('mangaDithers', types.String(), help='MaNGA dither '
                                                          'positions for a '
                                                          'dither sequence.'),
            keys.Key('mangaDither', types.String(), help='MaNGA dither '
                                                         'position for a '
                                                         'single dither.'),
            keys.Key('count', types.Int(), help='Number of MaNGA dither '
                                                'sets to perform.'),
            keys.Key('guiderTime', types.Float(), help='Exposure time '
                                                       'for guider'),
            keys.Key('noHartmann', help='Don\'t make Hartmann corrections'),
            keys.Key('noGuider', help='Don\'t start the guider'),
            keys.Key('noCalibs', help='Don\'t run the calibration step'),
            keys.Key('keepOffsets', help='When slewing, do not clear '
                                         'accumulated offsets'),
            keys.Key('ditherSeq', types.String(), help='dither positions for '
                                                       'each sequence. '
                                                       'e.g. AB')])

        # Define new commands for APO
        self.vocab = [
            ('doBossCalibs', '[<narc>] [<nbias>] [<ndark>] [<nflat>] '
                             '[<arcTime>] [<darkTime>] [<flatTime>] '
                             '[<guiderFlatTime>] [abort]',
                             self.doBossCalibs),

            ('doBossScience', '[<expTime>] [<nexp>] [abort] [stop] [test]',
                              self.doBossScience),

            ('doMangaDither', '[<expTime>] [<dither>] [stop] [abort]',
                              self.doMangaDither),

            ('doMangaSequence', '[<expTime>] [<dithers>] [<count>] [stop] '
                                '[abort]',
                                self.doMangaSequence),

            ('doApogeeMangaDither', '[<mangaDither>] [<comment>] '
                                    '[stop] [abort]',
                                    self.doApogeeMangaDither),

            ('doApogeeMangaSequence', '[<mangaDithers>] [<count>] [<comment>] '
                                      '[stop] [abort]',
                                      self.doApogeeMangaSequence),

            ('gotoField', '[<arcTime>] [<flatTime>] [<guiderFlatTime>] '
                          '[<guiderTime>] [noSlew] [noHartmann] [noCalibs] '
                          '[noGuider] [abort] [keepOffsets]',
                          self.gotoField),

            ('ditheredFlat', '[sp1] [sp2] [<expTime>] [<nStep>] [<nTick>]',
                             self.ditheredFlat),

            ('hartmann', '[<expTime>]', self.hartmann),

            ('collimateBoss', '', self.collimateBoss),

            ('lampsOff', '', self.lampsOff)]

    def doBossCalibs(self, cmd):
        """Take a set of calibration frames.

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
            cmd.fail("text='A science exposure sequence is running -- will not take calibration frames!")
            return

        if "abort" in keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'doBossCalibs')
            return

        # Modify running doBossCalibs command
        if self.modifiable(cmd, cmdState):
            if "nbias" in keywords:
                cmdState.nBias = int(keywords["nbias"].values[0])
            if "ndark" in keywords:
                cmdState.nDark = int(keywords["ndark"].values[0])
            if "nflat" in keywords:
                cmdState.nFlat = int(keywords["nflat"].values[0])
            if "narc" in keywords:
                cmdState.nArc = int(keywords["narc"].values[0])

            if "darkTime" in keywords:
                cmdState.darkTime = float(keywords["darkTime"].values[0])
            if "flatTime" in keywords:
                cmdState.flatTime = float(keywords["flatTime"].values[0])
            if "guiderFlatTime" in keywords:
                cmdState.guiderFlatTime = float(keywords["guiderFlatTime"].values[0])
            if "arcTime" in keywords:
                cmdState.arcTime = float(keywords["arcTime"].values[0])

            self.status(cmd, threads=False, finish=True, oneCommand='doBossCalibs')
            return

        # Lookup the current cartridge
        survey = sopState.survey
        if survey == sopActor.APOGEE:
            cmd.fail('text="current cartridge is not for BOSS or MaNGA; use bypass if you want to force calibrations"')
            return

        cmdState.reinitialize(cmd)
        if 'nbias' in keywords:
            cmdState.nBias = keywords["nbias"].values[0]
        if 'ndark' in keywords:
            cmdState.nDark = keywords["ndark"].values[0]
        if 'nflat' in keywords:
            cmdState.nFlat = keywords["nflat"].values[0]
        if 'narc' in keywords:
            cmdState.nArc = keywords["narc"].values[0]

        cmdState.arcTime = keywords["arcTime"].values[0] \
                                    if "arcTime" in keywords else CmdState.getDefaultArcTime(survey)
        if 'darkTime' in keywords:
            cmdState.darkTime = keywords["darkTime"].values[0]
        cmdState.flatTime = keywords["flatTime"].values[0] \
                                     if "flatTime" in keywords else CmdState.getDefaultFlatTime(survey)
        if 'guiderFlatTime' in keywords:
            cmdState.guiderFlatTime = keywords["guiderFlatTime"].values[0]

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

        activeStages = []
        if cmdState.nBias: activeStages.append('bias')
        if cmdState.nDark: activeStages.append('dark')
        if cmdState.nFlat: activeStages.append('flat')
        if cmdState.nArc: activeStages.append('arc')
        activeStages.append('cleanup') # we always may have to cleanup...
        cmdState.setupCommand(cmd, activeStages)

        sopState.queues[sopActor.MASTER].put(Msg.DO_BOSS_CALIBS, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)

    def doBossScience(self, cmd):
        """Take a set of BOSS science frames"""

        sopState = myGlobals.actorState
        cmdState = sopState.doBossScience
        keywords = cmd.cmd.keywords

        if "abort" in keywords or "stop" in keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'doBossScience')
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doBossScience command
            if "nexp" in keywords:
                cmdState.nExp = int(keywords["nexp"].values[0])

            if "expTime" in keywords:
                cmdState.expTime = float(keywords["expTime"].values[0])

            self.status(cmd, threads=False, finish=True, oneCommand='doBossScience')
            return

        cmdState.cmd = None
        cmdState.reinitialize(cmd)

        # NOTE: TBD: would have to sync with STUI to make nExp have defaults
        # and behave like one would expect it to (see doBossScience_nExp actorkeys value)
        cmdState.nExp = int(keywords["nexp"].values[0]) if "nexp" in keywords else 1
        expTime = float(keywords["expTime"].values[0]) if "expTime" in keywords else None
        cmdState.set('expTime',expTime)

        if cmdState.nExp == 0:
            cmd.fail('text="You must take at least one exposure"')
            return
        if cmdState.expTime == 0:
            cmd.fail('text="Exposure time must be greater than 0 seconds."')
            return

        sopState.queues[sopActor.MASTER].put(Msg.DO_BOSS_SCIENCE, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)

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

        if "abort" in keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'gotoField')
            return

        # Modify running gotoField command
        if self.modifiable(cmd, cmdState):
            cmdState.doSlew = True if "noSlew" not in keywords else False
            cmdState.doGuider = True if "noGuider" not in keywords else False
            cmdState.doHartmann = True if "noHartmann" not in keywords else False

            # NOTE: TBD: Need a full set of test cases for this...

            dropCalibs = False
            if "noCalibs" in keywords:
                if cmdState.didFlat or cmdState.didArc:
                    cmd.warn('text="Some cals have been taken; it\'s too late to disable them."')
                else:
                    dropCalibs = True
            if "arcTime" in keywords:
                if cmdState.didArc:
                    cmd.warn('text="Arcs are taken; it\'s too late to modify arcTime"')
                else:
                    cmdState.arcTime = float(keywords["arcTime"].values[0])
            if "flatTime" in keywords:
                if cmdState.didFlat:
                    cmd.warn('text="Flats are taken; it\'s too late to modify flatTime"')
                else:
                    cmdState.flatTime = float(keywords["flatTime"].values[0])
            if "guiderFlatTime" in keywords:
                cmdState.guiderFlatTime = float(keywords["guiderFlatTime"].values[0])
            if "guiderTime" in keywords:
                cmdState.guiderTime = float(keywords["guiderTime"].values[0])

            # * TBD: WARNING! This does not keep track of what's already been done,
            # * except for the dropCalibs bit above.
            cmdState.doCalibs = not dropCalibs
            cmdState.setStageState("slew", "pending" if cmdState.doSlew else "off")
            cmdState.setStageState("hartmann", "pending" if cmdState.doHartmann else "off")
            cmdState.setStageState("calibs", "pending" if not dropCalibs else "off")
            cmdState.setStageState("guider", "pending" if cmdState.doGuider else "off")

            self.status(cmd, threads=False, finish=True, oneCommand="gotoField")
            return

        cmdState.reinitialize(cmd, output=False)

        cmdState.doSlew = "noSlew" not in keywords
        cmdState.doGuider = "noGuider" not in keywords
        cmdState.doCalibs = ("noCalibs" not in keywords and survey != sopActor.APOGEE)
        cmdState.doHartmann = ("noHartmann" not in keywords and survey != sopActor.APOGEE)
        if cmdState.doCalibs:
            if "arcTime" in keywords:
                cmdState.arcTime = float(keywords["arcTime"].values[0])
            else:
                cmdState.arcTime = CmdState.getDefaultArcTime(survey)
            if "flatTime" in keywords:
                cmdState.flatTime = float(keywords["flatTime"].values[0])
            else:
                cmdState.flatTime = CmdState.getDefaultFlatTime(survey)
            if cmdState.arcTime <= 0:
                cmd.warn('text="GotoField arcTime is not a positive number: are you sure you want that?"')
            if cmdState.flatTime <= 0:
                cmd.warn('text="GotoField flatTime is not a positive number: are you sure you want that?"')
        if cmdState.doGuider:
            cmdState.guiderFlatTime = float(keywords["guiderFlatTime"].values[0]) \
                                      if "guiderFlatTime" in keywords else 0.5
            cmdState.guiderTime = float(keywords["guiderTime"].values[0]) \
                                  if "guiderTime" in keywords else 5
            cmdState.doGuiderFlat = cmdState.guiderFlatTime > 0
            cmdState.keepOffsets = "keepOffsets" in keywords
        else:
            cmdState.doGuiderFlat = False

        if survey == sopActor.UNKNOWN:
            cmd.warn('text="No cartridge is known to be loaded; disabling guider"')
            cmdState.doGuider = False
            cmdState.doGuiderFlat = False

        if cmdState.doSlew:
            pointingInfo = sopState.models["platedb"].keyVarDict["pointingInfo"]
            cmdState.ra = pointingInfo[3]
            cmdState.dec = pointingInfo[4]
            cmdState.rotang = 0.0  # Rotator angle; should always be 0.0

        if myGlobals.bypass.get(name='slewToField'):
            fakeSkyPos = SopCmd.obs2Sky(cmd, cmdState.fakeAz, cmdState.fakeAlt, cmdState.fakeRotOffset)
            cmdState.ra = fakeSkyPos[0]
            cmdState.dec = fakeSkyPos[1]
            cmdState.rotang = fakeSkyPos[2]
            cmd.warn('text="Bypass slewToField is FAKING RA DEC:  %g, %g /rotang=%g"' % (cmdState.ra,
                                                                   cmdState.dec,
                                                                   cmdState.rotang))

        # Junk!! Must keep this in one place! Adjustment will be ugly otherwise.
        activeStages = []
        if cmdState.doSlew: activeStages.append("slew")
        if cmdState.doHartmann: activeStages.append("hartmann")
        if cmdState.doCalibs: activeStages.append("calibs")
        if cmdState.doGuider: activeStages.append("guider")
        activeStages.append('cleanup') # we always may have to cleanup...
        cmdState.setupCommand(cmd, activeStages)

        sopState.queues[sopActor.MASTER].put(Msg.GOTO_FIELD, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)

    def ditheredFlat(self, cmd, finish=True):
        """Take a set of nStep dithered flats, moving the collimator by nTick between exposures"""

        sopState = myGlobals.actorState

        if self.doing_science():
            cmd.fail("text='A science exposure sequence is running -- will not start dithered flats!")
            return

        sopState.aborting = False

        spN = []
        all = ["sp1", "sp2",]
        for s in all:
            if s in cmd.cmd.keywords:
                spN += [s]

        if not spN:
            spN = all

        nStep = int(cmd.cmd.keywords["nStep"].values[0]) if "nStep" in cmd.cmd.keywords else 22
        nTick = int(cmd.cmd.keywords["nTick"].values[0]) if "nTick" in cmd.cmd.keywords else 62
        expTime = float(cmd.cmd.keywords["expTime"].values[0]) if "expTime" in cmd.cmd.keywords else 30

        sopState.queues[sopActor.MASTER].put(Msg.DITHERED_FLAT, cmd, replyQueue=self.replyQueue,
                                               actorState=sopState,
                                               expTime=expTime, spN=spN, nStep=nStep, nTick=nTick)

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
            cmd.fail("text='A science exposure sequence is running -- will not start a hartmann sequence!")
            return

        cmdState.reinitialize(cmd, output=False)

        expTime = float(cmd.cmd.keywords["expTime"].values[0]) \
                  if "expTime" in cmd.cmd.keywords else CmdState.getDefaultArcTime(sopActor.BOSS)
        cmdState.expTime = expTime

        sopState.queues[sopActor.MASTER].put(Msg.HARTMANN, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)

    def collimateBoss(self, cmd):
        """
        Warm up Ne/HgCd lamps, and take left/right full hartmanns to collimate
        the BOSS spectrographs, ignoring any remaining blue residuals.
        """
        sopState = myGlobals.actorState
        cmdState = sopState.collimateBoss

        if self.doing_science(sopState):
            cmd.fail("text='A science exposure sequence is running -- will not start a hartmann sequence!")
            return

        cmdState.reinitialize(cmd, output=False)

        sopState.queues[sopActor.MASTER].put(Msg.COLLIMATE_BOSS, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)

    def lampsOff(self, cmd, finish=True):
        """Turn all the lamps off"""

        sopState = myGlobals.actorState
        sopState.aborting = False

        multiCmd = MultiCommand(cmd, sopState.timeout, None)

        multiCmd.append(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False)
        multiCmd.append(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False)
        multiCmd.append(sopActor.NE_LAMP  , Msg.LAMP_ON, on=False)
        multiCmd.append(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False)
        multiCmd.append(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False)

        if multiCmd.run():
            if finish:
                cmd.finish('text="Turned lamps off"')
        else:
            if finish:
                cmd.fail('text="Some lamps failed to turn off"')

    def doMangaDither(self, cmd):
        """Take an exposure at a single manga dither position."""
        sopState = myGlobals.actorState
        cmdState = sopState.doMangaDither

        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'doMangaDither')
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doMangaDither command
            cmd.fail('text="Cannot modify MaNGA dither. If you need to change the dither position, abort and resubmit."')
            return

        cmdState.reinitialize(cmd)
        dither = cmd.cmd.keywords['dither'].values[0] \
                    if "dither" in cmd.cmd.keywords else None
        cmdState.set('dither',dither)
        expTime = cmd.cmd.keywords["expTime"].values[0] \
                    if "expTime" in cmd.cmd.keywords else None
        cmdState.set('expTime',expTime)

        sopState.queues[sopActor.MASTER].put(Msg.DO_MANGA_DITHER, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)

    def doMangaSequence(self, cmd):
        """Take an exposure at a sequence of dither positions, including calibrations."""

        name = 'doMangaSequence'
        sopState = myGlobals.actorState
        cmdState = sopState.doMangaSequence
        keywords = cmd.cmd.keywords

        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stop_cmd(cmd, cmdState, sopState, name)
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doMangaDither command
            if "dithers" in keywords:
                newDithers = keywords['dithers'].values[0]
                if (newDithers != cmdState.dithers):
                    cmd.fail('text="Cannot modify MaNGA dither pattern, only counts."')
                    return
                dithers = newDithers

            if "count" in keywords:
                count = int(keywords["count"].values[0])

            # Updating the dithers, count, and/or ditherSeq with new values
            cmdState.dithers = dithers
            cmdState.count = count
            cmdState.reset_ditherSeq()

            if cmdState.index >= len(cmdState.ditherSeq):
                cmd.warn('text="Modified exposure sequence is shorter than position in current sequence."')
                cmd.warn('text="Truncating previous exposure sequence, but NOT trying to stop current exposure."')
                cmdState.index = len(cmdState.ditherSeq)

            self.status(cmd, threads=False, finish=True, oneCommand=name)
            return

        cmdState.reinitialize(cmd)
        expTime = keywords["expTime"].values[0] if "expTime" in keywords else None
        cmdState.set('expTime', expTime)
        dither = keywords['dithers'].values[0] if "dithers" in keywords else None
        cmdState.set('dithers', dither)
        count = keywords['count'].values[0] if "count" in keywords else None
        cmdState.set('count', count)
        cmdState.reset_ditherSeq()

        sopState.queues[sopActor.MASTER].put(Msg.DO_MANGA_SEQUENCE, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)

    def doApogeeMangaDither(self, cmd):
        """Take an exposure at a single manga dither position."""
        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeMangaDither

        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stop_cmd(cmd, cmdState, sopState, 'doApogeeMangaDither')
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doApogeeMangaDither command
            cmd.fail('text="Cannot modify ApogeeManga dither. If you need to change the dither position, abort and resubmit."')
            return

        cmdState.reinitialize(cmd)

        mangaDither = cmd.cmd.keywords['mangaDither'].values[0] \
                    if "mangaDither" in cmd.cmd.keywords else None
        cmdState.set('mangaDither',mangaDither)

        sopState.queues[sopActor.MASTER].put(Msg.DO_APOGEEMANGA_DITHER, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)

    def doApogeeMangaSequence(self, cmd):
        """Take an exposure at a sequence of dither positions, including calibrations."""

        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeMangaSequence
        keywords = cmd.cmd.keywords
        name = 'doApogeeMangaSequence'

        if "stop" in keywords or 'abort' in keywords:
            self.stop_cmd(cmd, cmdState, sopState, name)
            return

        if self.modifiable(cmd, cmdState):
            # Modify running doMangaDither command
            if "mangaDithers" in keywords:
                newMangaDithers = keywords['mangaDithers'].values[0]
                if (newMangaDithers != cmdState.mangaDithers):
                    cmd.fail('text="Cannot modify APOGEE/MaNGA dither pattern, only counts."')
                    return
                mangaDithers = newMangaDithers

            if "count" in keywords:
                count = int(keywords["count"].values[0])

            cmdState.mangaDithers = mangaDithers
            cmdState.count = count
            cmdState.reset_ditherSeq()

            if cmdState.index >= len(cmdState.mangaDitherSeq):
                cmd.warn('text="Modified exposure sequence is shorter than position in current sequence."')
                cmd.warn('text="Truncating previous exposure sequence, but NOT trying to stop current exposure."')
                cmdState.index = len(cmdState.mangaDitherSeq)

            self.status(cmd, threads=False, finish=True, oneCommand=name)
            return

        cmdState.reinitialize(cmd)

        mangaDithers = keywords['mangaDithers'].values[0] if "mangaDithers" in keywords else None
        cmdState.set('mangaDithers', mangaDithers)

        count = keywords["count"].values[0] if "count" in keywords else None
        cmdState.set('count', count)

        cmdState.reset_ditherSeq()

        sopState.queues[sopActor.MASTER].put(Msg.DO_APOGEEMANGA_SEQUENCE, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)

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
        sopState.hartmann = CmdState.HartmannCmd()
        sopState.collimateBoss = CmdState.CollimateBossCmd()

        super(SopCmd_APO, self).initCommands()
