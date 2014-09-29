#!/usr/bin/env python

""" Wrap top-level ICC functions. """

import threading

import opscore.protocols.keys as keys
import opscore.protocols.types as types

from opscore.utility.qstr import qstr

import glob, os

from sopActor import *
import sopActor
from sopActor.CmdState import *
import sopActor.myGlobals as myGlobals
from sopActor import MultiCommand
# The below is useful if you are reloading this file for debugging.
# Normally, reloading SopCmd doesn't reload the rest of sopActor
if not 'debugging':
    oldPrecondition = sopActor.Precondition
    oldMultiCommand = sopActor.MultiCommand
    print "Reloading sopActor"
    reload(sopActor)
    sopActor.Precondition = oldPrecondition
    sopActor.MultiCommand = oldMultiCommand

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


# SDSS-IV plates should all be "APOGEE-2;MaNGA", but we need both,
# for test plates drilled as part of SDSS-III.
survey_dict = {'None':None, None:None,
               'BOSS':sopActor.BOSS, 'eBOSS':sopActor.BOSS,
               'APOGEE':sopActor.APOGEE,'APOGEE-2':sopActor.APOGEE,
               'MaNGA':sopActor.MANGA,
               'APOGEE-2&MaNGA':sopActor.APOGEEMANGA,
               'APOGEE&MaNGA':sopActor.APOGEEMANGA}
surveyMode_dict = {'None':None, None:None,
                   'APOGEE lead':sopActor.APOGEELEAD,
                   'MaNGA dither':sopActor.MANGADITHER,
                   'MaNGA stare':sopActor.MANGASTARE}

# And the inverses of the above.
# Can't directly make an inverse, since it's not one-to-one.
survey_inv_dict = {sopActor.UNKNOWN:'UNKNOWN',
                   sopActor.BOSS:'eBOSS', sopActor.APOGEE:'APOGEE-2',
                   sopActor.MANGA:'MaNGA',
                   sopActor.APOGEEMANGA:'APOGEE-2&MaNGA'}
surveyMode_inv_dict = {None:'None',
                       sopActor.MANGADITHER:'MaNGA dither',
                       sopActor.MANGASTARE:'MaNGA stare',
                       sopActor.APOGEELEAD:'APOGEE lead'}


class SopCmd(object):
    """ Wrap commands to the sop actor"""

    def __init__(self, actor):
        self.actor = actor
        self.replyQueue = sopActor.Queue('(replyQueue)',0)
        #
        # Declare keys that we're going to use
        #
        self.keys = keys.KeysDictionary("sop_sop", (1, 2),
                                        keys.Key("abort", help="Abort a command"),
                                        keys.Key("clear", help="Clear a flag"),
                                        keys.Key("narc", types.Int(), help="Number of arcs to take"),
                                        keys.Key("nbias", types.Int(), help="Number of biases to take"),
                                        keys.Key("ndark", types.Int(), help="Number of darks to take"),
                                        keys.Key("nexp", types.Int(), help="Number of exposures to take"),
                                        keys.Key("nflat", types.Int(), help="Number of flats to take"),
                                        keys.Key("nStep", types.Int(), help="Number of dithered exposures to take"),
                                        keys.Key("nTick", types.Int(), help="Number of ticks to move collimator"),
                                        keys.Key("arcTime", types.Float(), help="Exposure time for arcs"),
                                        keys.Key("darkTime", types.Float(), help="Exposure time for flats"),
                                        keys.Key("expTime", types.Float(), help="Exposure time"),
                                        keys.Key("guiderTime", types.Float(), help="Exposure time for guider"),
                                        keys.Key("fiberId", types.Int(), help="A fiber ID"),
                                        keys.Key("flatTime", types.Float(), help="Exposure time for flats"),
                                        keys.Key("guiderFlatTime", types.Float(), help="Exposure time for guider flats"),
                                        keys.Key("keepQueues", help="Restart thread queues"),
                                        keys.Key("noSlew", help="Don't slew to field"),
                                        keys.Key("noHartmann", help="Don't make Hartmann corrections"),
                                        keys.Key("noGuider", help="Don't start the guider"),
                                        keys.Key("noCalibs", help="Don't run the calibration step"),
                                        keys.Key("sp1", help="Select SP1"),
                                        keys.Key("sp2", help="Select SP2"),
                                        keys.Key("geek", help="Show things that only some of us love"),
                                        keys.Key("subSystem", types.String()*(1,), help="The sub-systems to bypass"),
                                        keys.Key("threads", types.String()*(1,), help="Threads to restart; default: all"),
                                        keys.Key("scale", types.Float(), help="Current scale from \"tcc show scale\""),
                                        keys.Key("delta", types.Float(), help="Delta scale (percent)"),
                                        keys.Key("absolute", help="Set scale to provided value"),
                                        keys.Key("test", help="Assert that the exposures are not expected to be meaningful"),
                                        keys.Key("keepOffsets", help="When slewing, do not clear accumulated offsets"),
                                        keys.Key("ditherSeq", types.String(),
                                                 help="dither positions for each sequence. e.g. AB"),
                                        keys.Key("seqCount", types.Int(),
                                                 help="number of times to launch sequence"),
                                        keys.Key("comment", types.String(), help="comment for headers"),
                                        keys.Key("dither", types.String(), help="MaNGA dither position for a single dither."),
                                        keys.Key("dithers", types.String(), help="MaNGA dither positions for a dither sequence."),
                                        keys.Key("apogeeExpTime", types.Float(), help="APOGEE exposure time per apogeeDither"),
                                        keys.Key("mangaExpTime", types.Float(), help="MaNGA exposure time per mangaDither"),
                                        keys.Key("apogeeDithers", types.String(), help="APOGEE dither positions for a single MaNGA dither."),
                                        keys.Key("mangaDithers", types.String(), help="MaNGA dither positions for a dither sequence."),
                                        keys.Key("mangaDither", types.String(), help="MaNGA dither position for a single dither."),
                                        keys.Key("count", types.Int(), help="Number of MaNGA dither sets to perform."),
                                        keys.Key("scriptName", types.String(), help="name of script to run"),
                                        keys.Key("az", types.Float(), help="what azimuth to slew to"),
                                        keys.Key("rotOffset", types.Float(), help="what rotator offset to add"),
                                        keys.Key("alt", types.Float(), help="what altitude to slew to"),
                                        )
        #
        # Declare commands
        #
        self.vocab = [
            ("bypass", "<subSystem> [clear]", self.bypass),
            ("doBossCalibs",
             "[<narc>] [<nbias>] [<ndark>] [<nflat>] [<arcTime>] [<darkTime>] [<flatTime>] [<guiderFlatTime>] [abort]",
             self.doBossCalibs),
            ("doBossScience", "[<expTime>] [<nexp>] [abort] [stop] [test]", self.doBossScience),
            ("doApogeeScience", "[<expTime>] [<ditherSeq>] [<seqCount>] [stop] [<abort>] [<comment>]", self.doApogeeScience),
            ("doApogeeSkyFlats", "[<expTime>] [<ditherSeq>] [stop] [abort]", self.doApogeeSkyFlats),
            ("doMangaDither", "[<expTime>] [<dither>] [stop] [abort]", self.doMangaDither),
            ("doMangaSequence", "[<expTime>] [<dithers>] [<count>] [stop] [abort]", self.doMangaSequence),
            ("doApogeeMangaDither", "[<apogeeExpTime>] [<mangaExpTime>] [<apogeeDithers>] [<mangaDither>] [<comment>] [stop] [abort]", self.doApogeeMangaDither),
            ("doApogeeMangaSequence", "[<apogeeExpTime>] [<mangaExpTime>] [<apogeeDithers>] [<mangaDithers>] [<comment>] [<count>] [stop] [abort]", self.doApogeeMangaSequence),
            ("ditheredFlat", "[sp1] [sp2] [<expTime>] [<nStep>] [<nTick>]", self.ditheredFlat),
            ("hartmann", "[<expTime>]", self.hartmann),
            ("lampsOff", "", self.lampsOff),
            ("ping", "", self.ping),
            ("restart", "[<threads>] [keepQueues]", self.restart),
            ("gotoField", "[<arcTime>] [<flatTime>] [<guiderFlatTime>] [<guiderTime>] [noSlew] [noHartmann] [noCalibs] [noGuider] [abort] [keepOffsets]",
             self.gotoField),
            ("gotoInstrumentChange", "", self.gotoInstrumentChange),
            ("gotoStow", "", self.gotoStow),
            ("gotoGangChange", "[<alt>] [abort] [stop]", self.gotoGangChange),
            ("doApogeeDomeFlat", "[stop] [abort]", self.doApogeeDomeFlat),
            ("setFakeField", "[<az>] [<alt>] [<rotOffset>]", self.setFakeField),
            ("status", "[geek]", self.status),
            ("reinit", "", self.reinit),
            ("runScript", "<scriptName>", self.runScript),
            ("listScripts", "", self.listScripts),
            ]

    # Define commands' callbacks
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
            cmd.fail("text='A science exposure sequence is running -- will not take calibration frames!")
            return

        sopState.aborting = False

        if "abort" in keywords:
            if cmdState.cmd and cmdState.cmd.isAlive():
                sopState.aborting = True
                cmd.warn('text="doBossCalibs will abort when it finishes its current activities; be patient"')

                cmdState.nArc = cmdState.nArcDone
                cmdState.nBias = cmdState.nBiasDone
                cmdState.nDark = cmdState.nDarkDone
                cmdState.nFlat = cmdState.nFlatDone

                cmdState.abortStages()
                self.status(cmd, threads=False, finish=True, oneCommand='doBossCalibs')
            else:
                cmd.fail('text="No doBossCalibs command is active"')

            return
        
        # Modify running doBossCalibs command
        if cmdState.cmd and cmdState.cmd.isAlive():
            if "nbias" in keywords:
                nBiasDoneOld = cmdState.nBiasDone
                cmdState.nBias = int(keywords["nbias"].values[0])
            if "ndark" in keywords:
                nDarkDoneOld = cmdState.nDarkDone
                cmdState.nDark = int(keywords["ndark"].values[0])
            if "nflat" in keywords:
                nFlatDoneOld = cmdState.nFlatDone
                cmdState.nFlat = int(keywords["nflat"].values[0])
            if "narc" in keywords:
                nArcDoneOld = cmdState.nArcDone
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
        
        cmdState.reinitialize(output=False)
        if 'nbias' in keywords:
            cmdState.nBias = keywords["nbias"].values[0]
        if 'ndark' in keywords:
            cmdState.nDark = keywords["ndark"].values[0]
        if 'nflat' in keywords:
            cmdState.nFlat = keywords["nflat"].values[0]
        if 'narc' in keywords:
            cmdState.nArc = keywords["narc"].values[0]

        cmdState.arcTime = keywords["arcTime"].values[0] \
                                    if "arcTime" in keywords else getDefaultArcTime(survey)
        if 'darkTime' in keywords:
            cmdState.darkTime = keywords["darkTime"].values[0]
        cmdState.flatTime = keywords["flatTime"].values[0] \
                                     if "flatTime" in keywords else getDefaultFlatTime(survey)
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
        sopState.aborting = False

        if "abort" in cmd.cmd.keywords or "stop" in cmd.cmd.keywords:
            if sopState.doBossScience.cmd and sopState.doBossScience.cmd.isAlive():
                sopState.aborting = True
                cmd.warn('text="doBossScience will cancel pending exposures and stop and readout any running one."')

                sopState.doBossScience.nExp = sopState.doBossScience.nExpDone
                sopState.doBossScience.nExpLeft = 0
                cmdVar = sopState.actor.cmdr.call(actor="boss", forUserCmd=cmd, cmdStr="exposure stop")
                if cmdVar.didFail:
                    cmd.warn('text="Failed to stop running exposure"')

                sopState.doBossScience.abortStages()
                self.status(cmd, threads=False, finish=True, oneCommand='doBossScience')
            else:
                cmd.fail('text="No doBossScience command is active"')
            return

        if sopState.doBossScience.cmd and sopState.doBossScience.cmd.isAlive():
            # Modify running doBossScience command
            if "nexp" in cmd.cmd.keywords:
                nExpDoneOld = sopState.doBossScience.nExpDone
                sopState.doBossScience.nExp = int(cmd.cmd.keywords["nexp"].values[0])
                sopState.doBossScience.nExpLeft = sopState.doBossScience.nExp - nExpDoneOld

            if "expTime" in cmd.cmd.keywords:
                sopState.doBossScience.expTime = float(cmd.cmd.keywords["expTime"].values[0])

            self.status(cmd, threads=False, finish=True, oneCommand='doBossScience')
            return

        sopState.doBossScience.cmd = None
        sopState.doBossScience.reinitialize(cmd)

        sopState.doBossScience.nExp = int(cmd.cmd.keywords["nexp"].values[0])   \
                                 if "nexp" in cmd.cmd.keywords else 1
        sopState.doBossScience.expTime = float(cmd.cmd.keywords["expTime"].values[0]) \
                                 if "expTime" in cmd.cmd.keywords else 900

        if sopState.doBossScience.nExp == 0:
            cmd.fail('text="You must take at least one exposure"')
            return
        
        # How many exposures we have left/have done
        sopState.doBossScience.nExpLeft = sopState.doBossScience.nExp; sopState.doBossScience.nExpDone = 0

        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.DO_BOSS_SCIENCE, actorState=sopState,
                            cartridge=sopState.cartridge,
                            survey=sopState.survey, cmdState=sopState.doBossScience).run():
            cmd.fail('text="Failed to issue doBossScience"')

    def stopApogeeSequence(self,cmd,cmdState,sopState,name):
        """Stop a currently running APOGEE science/skyflat sequence."""
        if cmdState.cmd and cmdState.cmd.isAlive():
            cmd.warn('text="%s will cancel pending exposures and stop any running one."'%(name))
            cmdState.exposureSeq = cmdState.exposureSeq[:cmdState.index]
            # Need to work out seqCount/seqDone -- CPL
            cmdVar = sopState.actor.cmdr.call(actor="apogee", forUserCmd=cmd, cmdStr="expose stop")
            if cmdVar.didFail:
                cmd.warn('text="Failed to stop running exposure"')
                
            cmdState.abortStages()
            self.status(cmd, threads=False, finish=True, oneCommand=name)
        else:
            cmd.fail('text="No %s command is active"'%(name))

    def doApogeeScience(self, cmd):
        """Take a sequence of dithered APOGEE science frames, or stop or modify a running sequence."""

        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeScience
        sopState.aborting = False

        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stopApogeeSequence(cmd,cmdState,sopState,"doApogeeScience")
            return

        cmdState.expTime = float(cmd.cmd.keywords["expTime"].values[0]) if \
                                               "expTime" in cmd.cmd.keywords else 10*60.0
        
        # Modify running doApogeeScience command
        if cmdState.cmd and cmdState.cmd.isAlive():
            ditherSeq = cmdState.ditherSeq
            seqCount = cmdState.seqCount
            if "ditherSeq" in cmd.cmd.keywords:
                newDitherSeq = cmd.cmd.keywords['ditherSeq'].values[0]
                if (cmdState.seqCount > 1 and newDitherSeq != cmdState.ditherSeq):
                    cmd.fail('text="Cannot modify dither sequence if current sequence count is > 1."')
                    cmd.fail('text="If you are certain it makes sense to change the dither sequence, change Seq Count to 1 first."')
                    return
                ditherSeq = newDitherSeq
            
            if "seqCount" in cmd.cmd.keywords:
                seqCount = int(cmd.cmd.keywords["seqCount"].values[0])

            exposureSeq = ditherSeq * seqCount
            cmdState.ditherSeq = ditherSeq
            cmdState.seqCount = seqCount
            cmdState.exposureSeq = exposureSeq

            if cmdState.index >= len(cmdState.exposureSeq):
                cmd.warn('text="Modified exposure sequence is shorter than position in current sequence."')
                cmd.warn('text="Truncating previous exposure sequence, but NOT trying to stop current exposure."')
                cmdState.index = len(cmdState.exposureSeq)

            self.status(cmd, threads=False, finish=True, oneCommand='doApogeeScience')
            return

        seqCount = int(cmd.cmd.keywords["seqCount"].values[0]) \
                   if "seqCount" in cmd.cmd.keywords else 2
        ditherSeq = cmd.cmd.keywords["ditherSeq"].values[0] \
                    if "ditherSeq" in cmd.cmd.keywords else "ABBA"
        comment = cmd.cmd.keywords["comment"].values[0] \
                    if "comment" in cmd.cmd.keywords else ""

        cmdState.reinitialize(cmd)
        cmdState.ditherSeq = ditherSeq
        cmdState.seqCount = seqCount
        cmdState.comment = comment

        exposureSeq = ditherSeq * seqCount
        cmdState.exposureSeq = exposureSeq
        cmdState.index = 0
        cmdState.expType = 'object'

        if len(cmdState.exposureSeq) == 0:
            cmd.fail('text="You must take at least one exposure"')
            return

        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.DO_APOGEE_EXPOSURES, actorState=sopState,
                            cartridge=sopState.cartridge,
                            survey=sopState.survey, cmdState=cmdState).run():
            cmd.fail('text="Failed to issue doApogeeScience"')

    def doApogeeSkyFlats(self, cmd):
        """ Take sky flats. """

        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeSkyFlats

        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            self.stopApogeeSequence(cmd,cmdState,sopState,"doApogeeSkyFlats")
            return
        
        cmdState.expTime = float(cmd.cmd.keywords["expTime"].values[0]) if \
                           "expTime" in cmd.cmd.keywords else 150.0
        seqCount = 1
        ditherSeq = cmd.cmd.keywords["ditherSeq"].values[0] \
                    if "ditherSeq" in cmd.cmd.keywords else "ABBA"

        cmdState.reinitialize(cmd)
        cmdState.ditherSeq = ditherSeq
        cmdState.seqCount = seqCount
        cmdState.comment = "sky flat, offset 0.01 degree in RA"

        exposureSeq = ditherSeq * seqCount
        cmdState.exposureSeq = exposureSeq
        cmdState.index = 0

        if len(cmdState.exposureSeq) == 0:
            cmd.fail('text="You must take at least one exposure"')
            return
        
        # Turn off the guider, if it's on.
        guideState = myGlobals.actorState.models["guider"].keyVarDict["guideState"]
        if guideState == 'on' or guideState == 'starting':
            cmdVar = sopState.actor.cmdr.call(actor="guider", forUserCmd=cmd,
                                              cmdStr="off",
                                              timeLim=sopState.timeout)
            if cmdVar.didFail:
                cmd.fail('text="Failed to turn off guiding for sky flats."')
                return

        # Offset
        cmdVar = sopState.actor.cmdr.call(actor="tcc", forUserCmd=cmd,
                                            cmdStr="offset arc 0.01,0.0",
                                            timeLim=sopState.timeout)
        if cmdVar.didFail:
            cmd.fail('text="Failed to make tcc offset for sky flats."')
            return

        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.DO_APOGEE_EXPOSURES,
                            actorState=sopState,
                            expType='object',
                            cartridge=sopState.cartridge,
                            survey=sopState.survey, cmdState=cmdState).run():
            cmd.fail('text="Failed to issue doApogeeSkyFlats"')
    
    def doMangaDither(self, cmd):
        """Take an exposure at a single manga dither position."""
        sopState = myGlobals.actorState
        cmdState = sopState.doMangaDither
        
        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            cmd.fail('text="Sorry, I cannot stop or abort a doMangaDither command. (yet)"')
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
        
        sopState = myGlobals.actorState
        cmdState = sopState.doMangaSequence
        
        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            cmd.fail('text="Sorry, I cannot stop or abort a doMangaSequence command. (yet)"')
            return

        cmdState.reinitialize(cmd)
        expTime = cmd.cmd.keywords["expTime"].values[0] \
                    if "expTime" in cmd.cmd.keywords else None
        cmdState.set('expTime',expTime)
        dither = cmd.cmd.keywords['dithers'].values[0] \
                    if "dithers" in cmd.cmd.keywords else None
        cmdState.set('dithers',dither)
        count = cmd.cmd.keywords['count'].values[0] \
                    if "count" in cmd.cmd.keywords else None
        cmdState.set('count',count)
        cmdState.reset_ditherSeq()
        
        sopState.queues[sopActor.MASTER].put(Msg.DO_MANGA_SEQUENCE, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)
        
    def doApogeeMangaDither(self, cmd):
        """Take an exposure at a single manga dither position."""
        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeMangaDither
        
        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            cmd.fail('text="Sorry, I cannot stop or abort a doMangaDither command. (yet)"')
            return
        
        cmdState.reinitialize(cmd)

        apogeeExpTime = cmd.cmd.keywords["apogeeExpTime"].values[0] \
                    if "apogeeExpTime" in cmd.cmd.keywords else None
        cmdState.set('apogeeExpTime',apogeeExpTime)

        mangaDither = cmd.cmd.keywords['mangaDither'].values[0] \
                    if "mangaDither" in cmd.cmd.keywords else None
        cmdState.set('mangaDither',mangaDither)
        mangaExpTime = cmd.cmd.keywords["mangaExpTime"].values[0] \
                    if "mangaExpTime" in cmd.cmd.keywords else None
        cmdState.set('mangaExpTime',mangaExpTime)

        sopState.queues[sopActor.MASTER].put(Msg.DO_APOGEEMANGA_DITHER, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)
    
    def doApogeeMangaSequence(self, cmd):
        """Take an exposure at a sequence of dither positions, including calibrations."""
        
        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeMangaSequence
        
        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            cmd.fail('text="Sorry, I cannot stop or abort a doMangaSequence command. (yet)"')
            return

        cmdState.reinitialize(cmd)

        apogeeExpTime = cmd.cmd.keywords["apogeeExpTime"].values[0] \
                    if "apogeeExpTime" in cmd.cmd.keywords else None
        cmdState.set('apogeeExpTime',apogeeExpTime)

        mangaDithers = cmd.cmd.keywords['mangaDithers'].values[0] \
                    if "mangaDithers" in cmd.cmd.keywords else None
        cmdState.set('mangaDithers',mangaDithers)
        mangaExpTime = cmd.cmd.keywords["mangaExpTime"].values[0] \
                    if "mangaExpTime" in cmd.cmd.keywords else None
        cmdState.set('mangaExpTime',mangaExpTime)

        count = cmd.cmd.keywords["count"].values[0] \
                    if "count" in cmd.cmd.keywords else None
        cmdState.set('count',count)

        cmdState.reset_ditherSeq()
        
        sopState.queues[sopActor.MASTER].put(Msg.DO_APOGEEMANGA_SEQUENCE, cmd, replyQueue=self.replyQueue,
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

    def bypass(self, cmd):
        """Ignore errors in a subsystem, or force a system to be in a given state."""
        subSystems = cmd.cmd.keywords["subSystem"].values
        doBypass = False if "clear" in cmd.cmd.keywords else True

        sopState = myGlobals.actorState
        bypass = myGlobals.bypass

        for subSystem in subSystems:
            if bypass.set(subSystem, doBypass) is None:
                cmd.fail('text="%s is not a recognised and bypassable subSystem"' % subSystem)
                return
            if bypass.is_cart_bypass(subSystem):
                self.updateCartridge(sopState.cartridge, sopState.plateType, sopState.surveyModeName, status=False)
                cmdStr = 'setRefractionBalance plateType="{0}" surveyMode="{1}"'.format(*sopState.surveyText)
                cmdVar = sopState.actor.cmdr.call(actor="guider", forUserCmd=cmd, cmdStr=cmdStr)
                if cmdVar.didFail:
                    cmd.fail('text="Failed to set guider refraction balance for bypass {0} {1}'.format(subSystem, doBypass))
                    return
            if bypass.is_gang_bypass(subSystem):
                cmd.warn('text="gang bypassed: %s"' % (sopState.apogeeGang.getPos()))

        self.status(cmd, threads=False)

    def setFakeField(self, cmd):
        """ (Re)set the position gotoField slews to if the slewToField bypass is set.

        The az and alt are used directly. RotOffset is added to whatever obj offset is calculated
        for the az and alt.

        Leaving any of the az, alt, and rotOffset arguments off will set them to the default, which is 'here'.
        """

        sopState = myGlobals.actorState

        cmd.warn('text="cmd=%s"' % (cmd.cmd.keywords))

        sopState.gotoField.fakeAz = float(cmd.cmd.keywords["az"].values[0]) if "az" in cmd.cmd.keywords else None
        sopState.gotoField.fakeAlt = float(cmd.cmd.keywords["alt"].values[0]) if "alt" in cmd.cmd.keywords else None
        sopState.gotoField.fakeRotOffset = float(cmd.cmd.keywords["rotOffset"].values[0]) if "rotOffset" in cmd.cmd.keywords else 0.0

        cmd.finish('text="set fake slew position to az=%s alt=%s rotOffset=%s"'
                   % (sopState.gotoField.fakeAz,
                      sopState.gotoField.fakeAlt,
                      sopState.gotoField.fakeRotOffset))

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

        sopState.queues[sopActor.MASTER].put(Msg.DITHERED_FLAT, cmd, replyQueue=sopState.queues[sopActor.MASTER],
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
        if self.doing_science(sopState):
            cmd.fail("text='A science exposure sequence is running -- will not start a hartmann sequence!")
            return
        
        sopState.aborting = False
        cmdState = sopState.hartmann
        
        expTime = float(cmd.cmd.keywords["expTime"].values[0]) \
                  if "expTime" in cmd.cmd.keywords else getDefaultArcTime(sopActor.BOSS)
        cmdState.expTime = expTime
        
        sopState.queues[sopActor.MASTER].put(Msg.HARTMANN, cmd, replyQueue=self.replyQueue,
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

        if self.doing_science(sopState):
            cmd.fail("text='A science exposure sequence is running -- will not go to field!")
            return

        sopState.aborting = False

        if "abort" in cmd.cmd.keywords:
            if cmdState.cmd and cmdState.cmd.isAlive():
                sopState.aborting = True

                cmdVar = sopState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="axis stop")
                if cmdVar.didFail:
                    cmd.warn('text="Failed to abort slew"')

                cmdState.doSlew = False
                cmdState.doHartmann = False
                cmdState.doGuider = False

                cmdState.abortStages()

                cmd.warn('text="gotoField will abort when it finishes its current activities; be patient"')
                self.status(cmd, threads=False, finish=True, oneCommand='gotoField')
            else:
                cmd.fail('text="No gotoField command is active"')

            return
        
        # Modify running gotoField command
        if cmdState.cmd and cmdState.cmd.isAlive():
            cmdState.doSlew = True if "noSlew" not in cmd.cmd.keywords else False
            cmdState.doGuider = True if "noGuider" not in cmd.cmd.keywords else False
            cmdState.doHartmann = True if "noHartmann" not in cmd.cmd.keywords else False

            dropCalibs = False
            if "noCalibs" in cmd.cmd.keywords:
                if cmdState.didFlat or cmdState.didArc:
                    cmd.warn('text="Some cals have been taken; it\'s too late to disable them."')
                else:
                    dropCalibs = True
            if "arcTime" in cmd.cmd.keywords or dropCalibs:
                if cmdState.didArc:
                    cmd.warn('text="Arcs are taken; it\'s too late to modify arcTime"')
                else:
                    cmdState.arcTime = float(cmd.cmd.keywords["arcTime"].values[0])
            if "flatTime" in cmd.cmd.keywords or dropCalibs:
                if cmdState.didFlat:
                    cmd.warn('text="Flats are taken; it\'s too late to modify flatTime"')
                else:
                    cmdState.flatTime = float(cmd.cmd.keywords["flatTime"].values[0])
            if "guiderFlatTime" in cmd.cmd.keywords:
                cmdState.guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0])
            if "guiderTime" in cmd.cmd.keywords:
                cmdState.guiderTime = float(cmd.cmd.keywords["guiderTime"].values[0])

            # TBD: WARNING! this isn't going to work as written:
            # * "pending" and "off" are not valid stage states.
            # * also, this does not keep track of what's already been done.
            # * would be best off if I had a unified cmdState modification system.
            cmdState.setStageState("slew", "pending" if cmdState.doSlew else "off")
            cmdState.setStageState("hartmann", "pending" if cmdState.doHartmann else "off")
            cmdState.setStageState("calibs", "pending" if cmdState.doCalibs else "off")
            cmdState.setStageState("guider", "pending" if cmdState.doGuider else "off")

            self.status(cmd, threads=False, finish=True, oneCommand="gotoField")
            return
        
        cmdState.reinitialize(output=False)
        
        cmdState.doSlew = "noSlew" not in cmd.cmd.keywords
        cmdState.doGuider = "noGuider" not in cmd.cmd.keywords
        cmdState.doCalibs = ("noCalibs" not in cmd.cmd.keywords and survey != sopActor.APOGEE)
        cmdState.doHartmann = ("noHartmann" not in cmd.cmd.keywords and survey != sopActor.APOGEE)
        if cmdState.doCalibs:
            if "arcTime" in cmd.cmd.keywords:
                cmdState.arcTime = float(cmd.cmd.keywords["arcTime"].values[0])
            else:
                cmdState.arcTime = getDefaultArcTime(survey)
            if "flatTime" in cmd.cmd.keywords:
                cmdState.flatTime = float(cmd.cmd.keywords["flatTime"].values[0])
            else:
                cmdState.flatTime = getDefaultFlatTime(survey)
            if cmdState.arcTime <= 0:
                cmd.warn('text="GotoField arcTime is not a positive number: are you sure you want that?"')
            if cmdState.flatTime <= 0:
                cmd.warn('text="GotoField flatTime is not a positive number: are you sure you want that?"')
        if cmdState.doGuider:
            cmdState.guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) \
                                      if "guiderFlatTime" in cmd.cmd.keywords else 0.5
            cmdState.guiderTime = float(cmd.cmd.keywords["guiderTime"].values[0]) \
                                  if "guiderTime" in cmd.cmd.keywords else 5
            cmdState.doGuiderFlat = cmdState.guiderFlatTime > 0
            cmdState.keepOffsets = "keepOffsets" in cmd.cmd.keywords
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
            fakeSkyPos = sopState.tccState.obs2Sky(cmd,
                                                     cmdState.fakeAz,
                                                     cmdState.fakeAlt,
                                                     cmdState.fakeRotOffset)
            cmdState.ra = fakeSkyPos[0]
            cmdState.dec = fakeSkyPos[1]
            cmdState.rotang = fakeSkyPos[2]
            cmd.warn('text="FAKING RA DEC:  %g, %g /rotang=%g"' % (cmdState.ra,
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
        
    def gotoPosition(self, cmd, name, cmdState, az, alt, rot=0):
        """Goto a specified alt/az/[rot] position, named 'name'."""
        sopState = myGlobals.actorState

        cmdState.setCommandState('running')
        cmdState.setStageState("slew", "running")

        sopState.aborting = False
        #
        # Try to guess how long the slew will take
        #
        slewDuration = 210

        multiCmd = MultiCommand(cmd, slewDuration + sopState.timeout, None)

        tccDict = sopState.models["tcc"].keyVarDict
        if az == None:
            az = tccDict['axePos'][0]
        if alt == None:
            alt = tccDict['axePos'][1]
        if rot == None:
            rot = tccDict['axePos'][2]

        multiCmd.append(sopActor.TCC, Msg.SLEW, actorState=sopState, az=az, alt=alt, rot=rot)

        if not multiCmd.run():
            cmdState.setStageState("slew", "failed")
            cmdState.setCommandState('failed', stateText="failed to move telescope")
            cmd.fail('text="Failed to slew to %s"' % (name))
            return

        cmdState.setStageState("slew", "done")
        cmdState.setCommandState('done', stateText='OK')
        cmd.finish('text="at %s position"' % (name))

    def isSlewingDisabled(self, cmd):
        """Return False if we can slew, otherwise return a string describing why we cannot."""
        sopState = myGlobals.actorState

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

    def gotoInstrumentChange(self, cmd):
        """Go to the instrument change position"""

        sopState = myGlobals.actorState

        blocked = self.isSlewingDisabled(cmd)
        if blocked:
            cmd.fail('text=%s' % (qstr('will not go to instrument change: %s' % (blocked))))
            return

        sopState.gotoInstrumentChange.reinitialize(cmd)
        self.gotoPosition(cmd, "instrument change", sopState.gotoInstrumentChange, 121, 90)

    def gotoStow(self, cmd):
        """Go to the gang connector change/stow position"""

        sopState = myGlobals.actorState

        blocked = self.isSlewingDisabled(cmd)
        if blocked:
            cmd.fail('text=%s' % (qstr('will not go to stow position: %s' % (blocked))))
            return

        sopState.gotoStow.reinitialize(cmd)
        self.gotoPosition(cmd, "stow", sopState.gotoStow, None, 30, rot=None)

    def gotoGangChange(self, cmd):
        """Go to the gang connector change position"""

        sopState = myGlobals.actorState

        blocked = self.isSlewingDisabled(cmd)
        if blocked:
            cmd.fail('text=%s' % (qstr('will not go to gang change: %s' % (blocked))))
            return

        sopState.gotoGangChange.reinitialize(cmd)
        alt = cmd.cmd.keywords["alt"].values[0] \
              if "alt" in cmd.cmd.keywords else 45.0

        cmdState = sopState.gotoGangChange
        cmdState.alt = alt
        
        # TBD: this needs to be implemented eventually...
        if 'stop' in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            cmd.fail('text="sorry, I cannot stop or abort a gotoGangChange command. (yet)"')
            return
        
        sopState.queues[sopActor.MASTER].put(Msg.GOTO_GANG_CHANGE, cmd, replyQueue=self.replyQueue,
                                             actorState=sopState, cmdState=cmdState)
    
    def doApogeeDomeFlat(self, cmd):
        """Take an APOGEE dome flat, with FFS closed and FFlamps on."""
        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeDomeFlat
        
        if 'stop' in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            cmd.fail('text"sorry, I cannot stop or abort an apogeeDomeFlat command. (yet)"')
            return
        
        sopState.queues[sopActor.MASTER].put(Msg.APOGEE_DOME_FLAT, cmd, replyQueue=sopState.queues[sopActor.MASTER],
                                               actorState=sopState, cmdState=cmdState,
                                               survey=sopState.survey)
    
    def runScript(self, cmd):
        """ Run the named script from the SOPACTOR_DIR/scripts directory. """
        sopState = myGlobals.actorState

        sopState.queues[sopActor.SCRIPT].put(Msg.NEW_SCRIPT, cmd, replyQueue=sopState.queues[sopActor.MASTER],
                                             actorState=sopState,
                                             survey=sopState.survey,
                                             scriptName = cmd.cmd.keywords["scriptName"].values[0])

    def listScripts(self, cmd):
        """ List available script names for the runScript command."""
        path = os.path.join(os.environ['SOPACTOR_DIR'],'scripts','*.inp')
        scripts = glob.glob(path)
        scripts = ','.join(os.path.splitext(os.path.basename(s))[0] for s in scripts)
        cmd.inform('availableScripts="%s"'%scripts)
        cmd.finish('')
        
    def ping(self, cmd):
        """ Query sop for liveness/happiness. """

        cmd.finish('text="Yawn; how soporific"')

    def restart(self, cmd):
        """Restart the worker threads"""

        sopState = myGlobals.actorState

        threads = cmd.cmd.keywords["threads"].values if "threads" in cmd.cmd.keywords else None
        keepQueues = True if "keepQueues" in cmd.cmd.keywords else False

        if threads == ["pdb"]:
            cmd.warn('text="The sopActor is about to break to a pdb prompt"')
            import pdb; pdb.set_trace()
            cmd.finish('text="We now return you to your regularly scheduled sop session"')
            return


        if sopState.restartCmd:
            sopState.restartCmd.finish("text=\"secundum verbum tuum in pace\"")
            sopState.restartCmd = None
        #
        # We can't finish this command now as the threads may not have died yet,
        # but we can remember to clean up _next_ time we restart
        #
        cmd.inform("text=\"Restarting threads\"")
        sopState.restartCmd = cmd

        sopState.actor.startThreads(sopState, cmd, restart=True,
                                    restartThreads=threads, restartQueues=not keepQueues)

    def reinit(self, cmd):
        """ (engineering command) Recreate the objects which hold the state of the various top-level commands. """

        cmd.inform('text="recreating command objects"')
        try:
            self.initCommands()
        except Exception as e:
            cmd.fail('text="failed to re-initialize command state: %s"'%e)
            return

        cmd.finish('')

    def status(self, cmd, threads=False, finish=True, oneCommand=None):
        """Return sop status.
        
        If threads is true report on SOP's threads; (also if geek in cmd.keywords)
        If finish complete the command.
        Trim output to contain just keys relevant to oneCommand.
        """

        sopState = myGlobals.actorState
        bypass = myGlobals.bypass

        self.actor.sendVersionKey(cmd)

        if hasattr(cmd, 'cmd') and cmd.cmd != None and "geek" in cmd.cmd.keywords:
            threads = True
            for t in threading.enumerate():
                cmd.inform('text="%s"' % t)

        bypassNames, bypassStates = bypass.get_bypass_list()

        cmd.inform("bypassNames=" + ", ".join(bypassNames))
        cmd.inform("bypassed=" + ", ".join([str(x) for x in bypassStates]))
        cmd.inform("bypassedNames=" + ", ".join(bypass.get_bypassedNames()))
        cmd.inform('text="apogeeGang: %s"' % (sopState.apogeeGang.getPos()))

        cmd.inform("surveyCommands=" + ", ".join(sopState.validCommands))

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

        # commands with no state
        sopState.gotoStow.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.gotoInstrumentChange.genKeys(cmd=cmd, trimKeys=oneCommand)

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
                        cmd.fail("")
                        return
                    else:
                        cmd.warn("")
            finally:
                sopState.ignoreAborting = False

        if finish:
            cmd.finish("")

    def initCommands(self):
        """Recreate the objects that hold the state of the various commands."""
        sopState = myGlobals.actorState

        sopState.gotoField = GotoFieldCmd()
        sopState.doBossCalibs = DoBossCalibsCmd()
        sopState.doBossScience = DoBossScienceCmd()
        sopState.doMangaDither = DoMangaDitherCmd()
        sopState.doMangaSequence = DoMangaSequenceCmd()
        sopState.doApogeeMangaDither = DoApogeeMangaDitherCmd()
        sopState.doApogeeMangaSequence = DoApogeeMangaSequenceCmd()
        sopState.doApogeeScience = DoApogeeScienceCmd()
        sopState.doApogeeSkyFlats = DoApogeeSkyFlatsCmd()
        sopState.gotoGangChange = GotoGangChangeCmd()
        sopState.doApogeeDomeFlat = DoApogeeDomeFlatCmd()
        sopState.hartmann = HartmannCmd()
        sopState.gotoInstrumentChange = CmdState('gotoInstrumentChange',
                                                 ["slew"])
        sopState.gotoStow = CmdState('gotoStow',
                                     ["slew"])

        self.updateCartridge(-1,'None','None')
        # guiderState is smart enough to only call the callback once both have been updated.
        sopState.guiderState.setCartridgeLoadedCallback(self.updateCartridge)
        sopState.guiderState.setSurveyCallback(self.updateCartridge)

    def updateCartridge(self, cartridge, plateType, surveyModeName, status=True):
        """ Read the guider's notion of the loaded cartridge and configure ourselves appropriately. """

        sopState = myGlobals.actorState
        cmd = sopState.actor.bcast
        
        sopState.cartridge = cartridge
        # save these for when someone sets a bypass.
        sopState.plateType = plateType
        sopState.surveyModeName = surveyModeName
        self.classifyCartridge(cmd, cartridge, plateType, surveyModeName)
        surveyMode = sopState.surveyMode
        survey = sopState.survey

        cmd.warn('text="loadCartridge fired cart=%s survey=%s surveyMode=%s"' % (cartridge, survey, surveyMode))
        cmd.inform('survey=%s,%s'%(qstr(sopState.surveyText[0]),qstr(sopState.surveyText[1])))

        if survey is sopActor.BOSS:
            sopState.gotoField.setStages(['slew', 'hartmann', 'calibs', 'guider', 'cleanup'])
            sopState.validCommands = ['gotoField',
                                      'doBossCalibs', 'doBossScience',
                                      'gotoInstrumentChange']
        elif survey is sopActor.APOGEE:
            sopState.gotoField.setStages(['slew', 'guider', 'cleanup'])
            sopState.validCommands = ['gotoField',
                                      'doApogeeScience', 'doApogeeSkyFlats',
                                      'gotoGangChange', 'gotoInstrumentChange', 'doApogeeDomeFlat']
        elif survey is sopActor.MANGA:
            sopState.gotoField.setStages(['slew', 'hartmann', 'calibs', 'guider', 'cleanup'])
            sopState.validCommands = ['gotoField',
                                      'doBossCalibs',
                                      'doMangaDither','doMangaSequence',
                                      'gotoInstrumentChange']
            if surveyMode is sopActor.MANGADITHER:
                sopState.doMangaSequence.set_mangaDither()
            if surveyMode is sopActor.MANGASTARE:
                sopState.doMangaSequence.set_mangaStare()
        elif survey is sopActor.APOGEEMANGA:
            sopState.gotoField.setStages(['slew', 'hartmann', 'calibs', 'guider', 'cleanup'])
            sopState.validCommands = ['gotoField',
                                      'doBossCalibs',
                                      'doApogeeMangaDither','doApogeeMangaSequence',
                                      'doApogeeSkyFlats', 'gotoGangChange',
                                      'gotoInstrumentChange', 'doApogeeDomeFlat']
            if surveyMode is sopActor.APOGEELEAD:
                sopState.doApogeeMangaDither.set_apogeeLead()
                sopState.doApogeeMangaSequence.set_apogeeLead()
            if surveyMode is sopActor.MANGADITHER:
                sopState.doApogeeMangaDither.set_manga()
                sopState.doApogeeMangaSequence.set_mangaDither()
            if surveyMode is sopActor.MANGASTARE:
                sopState.doApogeeMangaDither.set_manga()
                sopState.doApogeeMangaSequence.set_mangaStare()
        else:
            sopState.gotoField.setStages(['slew', 'guider', 'cleanup'])
            sopState.validCommands = ['gotoStow', 'gotoInstrumentChange']

        if status:
            self.status(cmd, threads=False, finish=False)

    def classifyCartridge(self, cmd, cartridge, plateType, surveyMode):
        """Set the survey and surveyMode for this cartridge in actorState."""
        def surveyText_bypass():
            sopState.surveyText = [survey_inv_dict[sopState.survey],
                                   surveyMode_inv_dict[sopState.surveyMode]]

        bypass = myGlobals.bypass
        sopState = myGlobals.actorState
        sopState.surveyText = ['','']

        if bypass.get('isBoss'):
            cmd.warn('text="We are lying about this being a BOSS cartridge"')
            sopState.survey = sopActor.BOSS
            sopState.surveyMode = None
            surveyText_bypass()
            return
        elif bypass.get('isApogee'):
            cmd.warn('text="We are lying about this being an APOGEE cartridge"')
            sopState.survey = sopActor.APOGEE
            sopState.surveyMode = None
            surveyText_bypass()
            return
        elif bypass.get('isMangaStare'):
            cmd.warn('text="We are lying about this being a MaNGA Stare cartridge"')
            sopState.survey = sopActor.MANGA
            sopState.surveyMode = sopActor.MANGASTARE
            surveyText_bypass()
            return
        elif bypass.get('isMangaDither'):
            cmd.warn('text="We are lying about this being a MaNGA Dither cartridge"')
            sopState.survey = sopActor.MANGA
            sopState.surveyMode = sopActor.MANGADITHER
            surveyText_bypass()
            return
        elif bypass.get('isApogeeMangaStare'):
            cmd.warn('text="We are lying about this being an APOGEE&MaNGA Stare cartridge"')
            sopState.survey = sopActor.APOGEEMANGA
            sopState.surveyMode = sopActor.MANGASTARE
            surveyText_bypass()
            return
        elif bypass.get('isApogeeMangaDither'):
            cmd.warn('text="We are lying about this being a APOGEE&MaNGA Dither cartridge"')
            sopState.survey = sopActor.APOGEEMANGA
            sopState.surveyMode = sopActor.MANGADITHER
            surveyText_bypass()
            return
        elif bypass.get('isApogeeLead'):
            cmd.warn('text="We are lying about this being an APOGEE&MaNGA, APOGEE Lead cartridge"')
            sopState.survey = sopActor.APOGEEMANGA
            sopState.surveyMode = sopActor.APOGEELEAD
            surveyText_bypass()
            return

        if cartridge <= 0:
            cmd.warn('text="We do not have a valid cartridge (id=%s)"' % (cartridge))
            sopState.survey = sopActor.UNKNOWN
            sopState.surveyMode = None
            surveyText_bypass()
            return

        # NOTE: not using .get() here so that I can send an error message if
        # the key lookup fails.
        try:
            sopState.survey = survey_dict[plateType]
            sopState.surveyText[0] = plateType
        except KeyError:
            cmd.error('text=%s'%qstr("Do not understand plateType: %s."%plateType))
            sopState.survey = sopActor.UNKNOWN
            sopState.surveyText[0] = 'UNKNOWN'
        try:
            sopState.surveyMode = surveyMode_dict[surveyMode]
            sopState.surveyText[1] = surveyMode
        except KeyError:
            cmd.error('text=%s'%qstr("Do not understand surveyMode: %s."%surveyMode))
            sopState.surveyMode = None
            sopState.surveyText[1] = 'None'

    def doing_science(self,sopState):
        """Return True if any sort of science command is currently running."""
        return (sopState.doBossScience.cmd and sopState.doBossScience.cmd.isAlive()) or \
               (sopState.doApogeeScience.cmd and sopState.doApogeeScience.cmd.isAlive()) or \
               (sopState.doMangaDither.cmd and sopState.doMangaDither.cmd.isAlive()) or \
               (sopState.doMangaSequence.cmd and sopState.doMangaSequence.cmd.isAlive()) or \
               (sopState.doApogeeMangaDither.cmd and sopState.doApogeeMangaDither.cmd.isAlive()) or \
               (sopState.doApogeeMangaSequence.cmd and sopState.doApogeeMangaSequence.cmd.isAlive())
