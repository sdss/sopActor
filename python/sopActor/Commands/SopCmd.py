#!/usr/bin/env python

""" Wrap top-level ICC functions. """

import re, sys, time
import threading

import opscore.protocols.keys as keys
import opscore.protocols.types as types

from opscore.utility.qstr import qstr

from sopActor import *
import sopActor
import sopActor.myGlobals as myGlobals
from sopActor import MultiCommand

if not False:
    oldPrecondition = sopActor.Precondition
    oldMultiCommand = sopActor.MultiCommand
    print "Reloading sopActor"
    reload(sopActor)
    sopActor.Precondition = oldPrecondition
    sopActor.MultiCommand = oldMultiCommand

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


try:
    fakeMarvels                         # force this to be interpreted as a Marvels cartridge
except:
    fakeBoss = False
    fakeMarvels = False

class SopCmd(object):
    """ Wrap commands to the sop actor"""

    def __init__(self, actor):
        self.actor = actor
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
                                        keys.Key("guiderExpTime", types.Float(), help="Exposure time for guider exposures"),
                                        keys.Key("keepQueues", help="Restart thread queues"),
                                        keys.Key("noSlew", help="Don't slew to field"),
                                        keys.Key("noHartmann", help="Don't make Hartmann corrections"),
                                        keys.Key("noGuider", help="Don't start the guider"),
                                        keys.Key("noCalibs", help="Don't run the calibration step"),
                                        keys.Key("sp1", help="Select SP1"),
                                        keys.Key("sp2", help="Select SP2"),
                                        keys.Key("geek", help="Show things that only some of us love"),
                                        keys.Key("subSystem", types.String(), help="The sub-system to bypass"),
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
                                        keys.Key("comment", help="comment for headers"),
                                        )
        #
        # Declare commands
        #
        self.vocab = [
            ("bypass", "<subSystem> [clear]", self.bypass),
            ("doCalibs",
             "[<narc>] [<nbias>] [<ndark>] [<nflat>] [<arcTime>] [<darkTime>] [<flatTime>] [<guiderFlatTime>] [abort] [test]",
             self.doCalibs),
            ("doScience", "[<expTime>] [<nexp>] [abort] [stop] [test]", self.doScience),
            ("doApogeeScience", "[<expTime>] [<ditherSeq>] [<seqCount>] [stop] [<comment>]", self.doApogeeScience),
            ("ditheredFlat", "[sp1] [sp2] [<expTime>] [<nStep>] [<nTick>]", self.ditheredFlat),
            ("hartmann", "[sp1] [sp2] [<expTime>]", self.hartmann),
            ("lampsOff", "", self.lampsOff),
            ("ping", "", self.ping),
            ("restart", "[<threads>] [keepQueues]", self.restart),
            ("gotoField", "[<arcTime>] [<flatTime>] [<guiderFlatTime>] [<guiderExpTime>] [noSlew] [noHartmann] [noCalibs] [noGuider] [abort] [keepOffsets]",
             self.gotoField),
            ("gotoInstrumentChange", "", self.gotoInstrumentChange),
            ("gotoStow", "", self.gotoStow),
            ("gotoGangChange", "", self.gotoGangChange),
            ("setScale", "<delta>|<scale>", self.setScale),
            ("scaleChange", "<delta>|<scale>", self.scaleChange),
            ("status", "[geek]", self.status),
            ("reinit", "", self.reinit),
            ]
    #
    # Declare systems that can be bypassed
    #
    if not Bypass.get():
        for ss in ("ffs", "ff_lamp", "hgcd_lamp", "ne_lamp", "uv_lamp", "wht_lamp", "boss", "gcamera", "axes"):
            Bypass.set(ss, False, define=True)
    #
    # Define commands' callbacks
    #
    def doCalibs(self, cmd):
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

          test        ? If set, the boss exposure QUALITY cards will be "test"

          """
        
        sopState = myGlobals.actorState
        if sopState.doScience.cmd and sopState.doScience.cmd.isAlive():
            cmd.fail("text='a science exposure sequence is running -- will not take calibration frames!")
            return
    
        actorState = myGlobals.actorState
        actorState.aborting = False

        if "abort" in cmd.cmd.keywords:
            if sopState.doCalibs.cmd and sopState.doCalibs.cmd.isAlive():
                actorState.aborting = True
                cmd.warn('text="doCalibs will abort when it finishes its current activities; be patient"')

                sopState.doCalibs.nArc = sopState.doCalibs.nArcDone
                sopState.doCalibs.nBias = sopState.doCalibs.nBiasDone
                sopState.doCalibs.nDark = sopState.doCalibs.nDarkDone
                sopState.doCalibs.nFlat = sopState.doCalibs.nFlatDone

                sopState.doCalibs.abortStages()
                self.status(cmd, threads=False, finish=True, oneCommand='doCalibs')
            else:
                cmd.fail('text="No doCalibs command is active"')

            return
                
        if sopState.doCalibs.cmd and sopState.doCalibs.cmd.isAlive():
            # Modify running doCalibs command
            if "narc" in cmd.cmd.keywords:
                nArcDoneOld = sopState.doCalibs.nArcDone
                sopState.doCalibs.nArc = int(cmd.cmd.keywords["narc"].values[0])
                sopState.doCalibs.nArcLeft = sopState.doCalibs.nArc - nArcDoneOld
            if "nbias" in cmd.cmd.keywords:
                nBiasDoneOld = sopState.doCalibs.nBiasDone
                sopState.doCalibs.nBias = int(cmd.cmd.keywords["nbias"].values[0])
                sopState.doCalibs.nBiasLeft = sopState.doCalibs.nBias - nBiasDoneOld
            if "ndark" in cmd.cmd.keywords:
                nDarkDoneOld = sopState.doCalibs.nDarkDone
                sopState.doCalibs.nDark = int(cmd.cmd.keywords["ndark"].values[0])
                sopState.doCalibs.nDarkLeft = sopState.doCalibs.nDark - nDarkDoneOld
            if "nflat" in cmd.cmd.keywords:
                nFlatDoneOld = sopState.doCalibs.nFlatDone
                sopState.doCalibs.nFlat = int(cmd.cmd.keywords["nflat"].values[0])
                sopState.doCalibs.nFlatLeft = sopState.doCalibs.nFlat - nFlatDoneOld

            if "arcTime" in cmd.cmd.keywords:
                sopState.doCalibs.arcTime = float(cmd.cmd.keywords["arcTime"].values[0])
            if "darkTime" in cmd.cmd.keywords:
                sopState.doCalibs.darkTime = float(cmd.cmd.keywords["darkTime"].values[0])
            if "flatTime" in cmd.cmd.keywords:
                sopState.doCalibs.flatTime = float(cmd.cmd.keywords["flatTime"].values[0])
            if "guiderFlatTime" in cmd.cmd.keywords:
                sopState.doCalibs.guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0])

            sopState.doCalibs.testExposures = "test" in cmd.cmd.keywords

            self.status(cmd, threads=False, finish=True, oneCommand='doCalibs')
            return
        #
        # Lookup the current cartridge
        #
        survey = sopState.survey
        cartridge = sopState.cartridge
        if survey != sopActor.BOSS:
            cmd.fail('text="current cartridge is not for BOSS; use bypass if you want to force calibrations"')
            return

        sopState.doCalibs.cmd = None

        sopState.doCalibs.nArc = int(cmd.cmd.keywords["narc"].values[0])   \
                                 if "narc" in cmd.cmd.keywords else 0
        sopState.doCalibs.nBias = int(cmd.cmd.keywords["nbias"].values[0]) \
                                  if "nbias" in cmd.cmd.keywords else 0
        sopState.doCalibs.nDark = int(cmd.cmd.keywords["ndark"].values[0]) \
                                  if "ndark" in cmd.cmd.keywords else 0
        sopState.doCalibs.nFlat = int(cmd.cmd.keywords["nflat"].values[0]) \
                                  if "nflat" in cmd.cmd.keywords else 0
        sopState.doCalibs.arcTime = float(cmd.cmd.keywords["arcTime"].values[0]) \
                                    if "arcTime" in cmd.cmd.keywords else getDefaultArcTime(survey)
        sopState.doCalibs.darkTime = float(cmd.cmd.keywords["darkTime"].values[0]) \
                                     if "darkTime" in cmd.cmd.keywords else 0
        sopState.doCalibs.flatTime = float(cmd.cmd.keywords["flatTime"].values[0]) \
                                     if "flatTime" in cmd.cmd.keywords else getDefaultFlatTime(survey)
        sopState.doCalibs.guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) \
                                           if "guiderFlatTime" in cmd.cmd.keywords else 0
        sopState.doCalibs.testExposures = "test" in cmd.cmd.keywords

        if sopState.doCalibs.nArc + sopState.doCalibs.nBias + \
               sopState.doCalibs.nDark + sopState.doCalibs.nFlat == 0:
            cmd.fail('text="You must take at least one arc, bias, dark, or flat exposure"')
            return

        if sopState.doCalibs.nDark and sopState.doCalibs.darkTime <= 0:
            cmd.fail('text="Please decide on a value for darkTime"')
            return
        #
        # How many exposures we have left/have done
        #
        sopState.doCalibs.nArcLeft = sopState.doCalibs.nArc; sopState.doCalibs.nArcDone = 0
        sopState.doCalibs.nBiasLeft = sopState.doCalibs.nBias; sopState.doCalibs.nBiasDone = 0
        sopState.doCalibs.nDarkLeft = sopState.doCalibs.nDark; sopState.doCalibs.nDarkDone = 0
        sopState.doCalibs.nFlatLeft = sopState.doCalibs.nFlat; sopState.doCalibs.nFlatDone = 0

        if sopState.doCalibs.nFlat > 0 and sopState.doCalibs.guiderFlatTime > 0:
            if cartridge < 0:
                cmd.warn('text="No cartridge is known to be loaded; not taking guider flats"')
                sopState.doCalibs.guiderFlatTime = 0
                
        if survey == sopActor.MARVELS:
            sopState.doCalibs.flatTime = 0                # no need to take a BOSS flat

        sopState.doCalibs.setupCommand("doCalibs", cmd,
                                       ["doCalibs"])
        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.DO_CALIBS, actorState=actorState, cartridge=cartridge,
                            survey=survey, cmdState=sopState.doCalibs).run():
            cmd.fail('text="Failed to issue doCalibs"')

        # self.status(cmd, threads=False, finish=False, oneCommand="doCalibs")

    def doScience(self, cmd):
        """Take a set of science frames"""

        actorState = myGlobals.actorState
        sopState = myGlobals.actorState
        actorState.aborting = False

        if "abort" in cmd.cmd.keywords or "stop" in cmd.cmd.keywords:
            if sopState.doScience.cmd and sopState.doScience.cmd.isAlive():
                actorState.aborting = True
                cmd.warn('text="doScience will cancel pending exposures and stop and readout any running one."')

                sopState.doScience.nExp = sopState.doScience.nExpDone
                sopState.doScience.nExpLeft = 0                
                cmdVar = actorState.actor.cmdr.call(actor="boss", forUserCmd=cmd, cmdStr="exposure stop")
                if cmdVar.didFail:
                    cmd.warn('text="Failed to stop running exposure"')

                sopState.doScience.abortStages()
                self.status(cmd, threads=False, finish=True, oneCommand='doScience')
            else:
                cmd.fail('text="No doScience command is active"')
            return
                
        if sopState.doScience.cmd and sopState.doScience.cmd.isAlive():
            # Modify running doScience command
            if "nexp" in cmd.cmd.keywords:
                nExpDoneOld = sopState.doScience.nExpDone
                sopState.doScience.nExp = int(cmd.cmd.keywords["nexp"].values[0])
                sopState.doScience.nExpLeft = sopState.doScience.nExp - nExpDoneOld

            if "expTime" in cmd.cmd.keywords:
                sopState.doScience.expTime = float(cmd.cmd.keywords["expTime"].values[0])

            self.status(cmd, threads=False, finish=True, oneCommand='doScience')
            return

        sopState.doScience.cmd = None
        
        sopState.doScience.nExp = int(cmd.cmd.keywords["nexp"].values[0])   \
                                 if "nexp" in cmd.cmd.keywords else 1
        sopState.doScience.expTime = float(cmd.cmd.keywords["expTime"].values[0]) \
                                 if "expTime" in cmd.cmd.keywords else 900

        if sopState.doScience.nExp == 0:
            cmd.fail('text="You must take at least one exposure"')
            return
        #
        # How many exposures we have left/have done
        #
        sopState.doScience.nExpLeft = sopState.doScience.nExp; sopState.doScience.nExpDone = 0

        sopState.doScience.setupCommand("doScience", cmd,
                                        ["doScience"])
        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.DO_SCIENCE, actorState=actorState, cartridge=cartridge,
                            survey=sopState.survey, cmdState=sopState.doScience).run():
            cmd.fail('text="Failed to issue doScience"')

    def doApogeeScience(self, cmd):
        """Take a sequence of dithered APOGEE science frames, or stop or modify a running sequence."""

        actorState = myGlobals.actorState
        sopState = myGlobals.actorState
        actorState.aborting = False

        if "stop" in cmd.cmd.keywords:
            if sopState.doApogeeScience.cmd and sopState.doApogeeScience.cmd.isAlive():
                cmd.warn('text="doApogeeScience will cancel pending exposures and stopping any running one."')

                sopState.doApogeeScience.exposureSeq = sopState.doApogeeScience.exposureSeq[:sopState.doApogeeScience.index]

                cmdVar = actorState.actor.cmdr.call(actor="apogee", forUserCmd=cmd, cmdStr="expose stop")
                if cmdVar.didFail:
                    cmd.warn('text="Failed to stop running exposure"')

                sopState.doApogeeScience.abortStages()
                self.status(cmd, threads=False, finish=True, oneCommand='doApogeeScience')
            else:
                cmd.fail('text="No doApogeeScience command is active"')
            return
                
        sopState.doApogeeScience.expTime = float(cmd.cmd.keywords["expTime"].values[0]) if \
                                               "expTime" in cmd.cmd.keywords else 10*60.0

        if sopState.doApogeeScience.cmd and sopState.doApogeeScience.cmd.isAlive():
            # Modify running doApogeeScience command
            if ("ditherSeq" in cmd.cmd.keywords
                and cmd.cmd.keywords['ditherSeq'].values[0] != sopState.doApogeeScience.ditherSeq):
                cmd.fail('text="Cannot modify dither sequence"')
                return
            
            if "seqCount" in cmd.cmd.keywords:
                seqCount = int(cmd.cmd.keywords["seqCount"].values[0])
                exposureSeq = sopState.doApogeeScience.ditherSeq * seqCount
                sopState.doApogeeScience.seqCount = seqCount
                sopState.doApogeeScience.exposureSeq = exposureSeq                
                if sopState.doApogeeScience.index > len(sopState.doApogeeScience.exposureSeq):
                    cmd.warn('text="truncating previous exposure sequence, but NOT trying to stop current exposure"')
                    sopState.doApogeeScience.index = len(sopState.doApogeeScience.exposureSeq)
                    
            self.status(cmd, threads=False, finish=True, oneCommand='doApogeeScience')
            return

        seqCount = int(cmd.cmd.keywords["seqCount"].values[0]) \
                   if "seqCount" in cmd.cmd.keywords else 1
        ditherSeq = cmd.cmd.keywords["ditherSeq"].values[0] \
                    if "ditherSeq" in cmd.cmd.keywords else "A"

        sopState.doApogeeScience.cmd = cmd
        sopState.doApogeeScience.ditherSeq = ditherSeq
        sopState.doApogeeScience.seqCount = seqCount

        exposureSeq = ditherSeq * seqCount
        sopState.doApogeeScience.exposureSeq = exposureSeq
        sopState.doApogeeScience.index = 0
        
        if len(sopState.doApogeeScience.exposureSeq) == 0:
            cmd.fail('text="You must take at least one exposure"')
            return

        sopState.doApogeeScience.setupCommand("doApogeeScience", cmd,
                                              ["doApogeeScience"])
        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.DO_APOGEE_SCIENCE, actorState=actorState, cartridge=cartridge,
                            survey=sopState.survey, cmdState=sopState.doApogeeScience).run():
            cmd.fail('text="Failed to issue doApogeeScience"')

    def lampsOff(self, cmd, finish=True):
        """Turn all the lamps off"""

        actorState = myGlobals.actorState
        actorState.aborting = False

        multiCmd = MultiCommand(cmd, actorState.timeout, None)

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
        """Tell MultiCmd to ignore errors in a subsystem"""
        subSystem = cmd.cmd.keywords["subSystem"].values[0]        
        doBypass = False if "clear" in cmd.cmd.keywords else True

        actorState = myGlobals.actorState

        if subSystem == "planets":
            global fakeBoss
            fakeBoss = doBypass
            if doBypass:
                fakeMarvels = not fakeBoss
            self.updateCartridge(actorState.cartridge)
            cmd.finish('text="%s"' % ("I'm studying some very faint fuzzies" if fakeBoss else ""))
            return
        elif subSystem == "science":
            global fakeMarvels
            fakeMarvels = doBypass
            if doBypass:
                fakeBoss = not fakeMarvels
            self.updateCartridge(actorState.cartridge)
            cmd.finish('text="%s"' % ("Ah, a Marvels night" if fakeMarvels else ""))
            return

        if Bypass.set(subSystem, doBypass) is None:
            cmd.fail('text="%s is not a recognised and bypassable subSystem"' % subSystem)
            return

        self.status(cmd, threads=False)

    def ditheredFlat(self, cmd, finish=True):
        """Take a set of nStep dithered flats, moving the collimator by nTick between exposures"""

        sopState = myGlobals.actorState

        if sopState.doScience.cmd and sopState.doScience.cmd.isAlive():
            cmd.fail("text='a science exposure sequence is running -- will not start dithered flats!")
            return
    
        actorState = myGlobals.actorState
        actorState.aborting = False

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

        actorState.queues[sopActor.MASTER].put(Msg.DITHERED_FLAT, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState,
                                               expTime=expTime, spN=spN, nStep=nStep, nTick=nTick)

    def hartmann(self, cmd, finish=True):
        """Take two arc exposures, one with the Hartmann left screen in and one with the right one in.

If the flat field screens are initially open they are closed, and the Ne/HgCd lamps are turned on.
You may specify using only one spectrograph with sp1 or sp2; the default is both.
The exposure time is set by expTime

When the sequence is finished the Hartmann screens are moved out of the beam, the lamps turned off, and the
flat field screens returned to their initial state.
"""
        sopState = myGlobals.actorState
        if sopState.doScience.cmd and sopState.doScience.cmd.isAlive():
            cmd.fail("text='a science exposure sequence is running -- will not start a hartmann sequence!")
            return
    
        actorState = myGlobals.actorState
        actorState.aborting = False

        expTime = float(cmd.cmd.keywords["expTime"].values[0]) \
                  if "expTime" in cmd.cmd.keywords else getDefaultArcTime(sopActor.BOSS)
        sp1 = "sp1" in cmd.cmd.keywords
        sp2 = "sp2" in cmd.cmd.keywords
        if not sp1 and not sp2:
            sp1 = True; sp2 = True; 

        actorState.queues[sopActor.MASTER].put(Msg.HARTMANN, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState, expTime=expTime, sp1=sp1, sp2=sp2)

    def gotoField(self, cmd):
        """Slew to the current cartridge/pointing

Slew to the position of the currently loaded cartridge. At the beginning of the slew all the lamps are turned on and the flat field screen petals are closed.  When you arrive at the field, all the lamps are turned off again and the flat field petals are opened if you specified openFFS.
        """
        
        sopState = myGlobals.actorState

        if sopState.doScience.cmd and sopState.doScience.cmd.isAlive():
            cmd.fail("text='a science exposure sequence is running -- will not go to field!")
            return
    
        actorState = myGlobals.actorState
        actorState.aborting = False

        if "abort" in cmd.cmd.keywords:
            if sopState.gotoField.cmd and sopState.gotoField.cmd.isAlive():
                actorState.aborting = True

                cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="axis stop")
                if cmdVar.didFail:
                    cmd.warn('text="Failed to abort slew"')

                sopState.gotoField.doSlew = False

                sopState.gotoField.nArcLeft = 0
                sopState.gotoField.nArcDone = sopState.gotoField.nArc
                sopState.gotoField.nFlatLeft = 0
                sopState.gotoField.nFlatDone = sopState.gotoField.nFlat
                sopState.gotoField.doHartmann = False
                sopState.gotoField.doGuider = False

                sopState.gotoField.abortStages()
                
                cmd.warn('text="gotoField will abort when it finishes its current activities; be patient"')
                self.status(cmd, threads=False, finish=True, oneCommand='gotoField')
            else:
                cmd.fail('text="No gotoField command is active"')

            return

        if sopState.gotoField.cmd and sopState.gotoField.cmd.isAlive():
            # Modify running gotoField command
            sopState.gotoField.doSlew = True if "noSlew" not in cmd.cmd.keywords else False
            sopState.gotoField.doGuider = True if "noGuider" not in cmd.cmd.keywords else False
            sopState.gotoField.doHartmann = True if "noHartmann" not in cmd.cmd.keywords else False

            dropCalibs = False
            if "noCalibs" in cmd.cmd.keywords:
                if sopState.gotoField.nArcDone > 0 or sopState.gotoField.nFlatDone > 0:
                    cmd.warn('text="Some cals have been taken; it\'s too late to disable them."')
                else:
                    dropCalibs = True
            if "arcTime" in cmd.cmd.keywords or dropCalibs:
                if sopState.gotoField.nArcDone > 0:
                    cmd.warn('text="Arcs are taken; it\'s too late to modify arcTime"')
                else:
                    sopState.gotoField.arcTime = float(cmd.cmd.keywords["arcTime"].values[0])
                    sopState.gotoField.nArc = 1 if sopState.gotoField.arcTime > 0 else 0
                    sopState.gotoField.nArcLeft = sopState.gotoField.nArc
            if "flatTime" in cmd.cmd.keywords or dropCalibs:
                if sopState.gotoField.nFlatDone > 0:
                    cmd.warn('text="Flats are taken; it\'s too late to modify flatTime"')
                else:
                    sopState.gotoField.flatTime = float(cmd.cmd.keywords["flatTime"].values[0])
                    sopState.gotoField.nFlat = 1 if sopState.gotoField.flatTime > 0 else 0
                    sopState.gotoField.nFlatLeft = sopState.gotoField.nFlat
            if "guiderFlatTime" in cmd.cmd.keywords:
                sopState.gotoField.guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0])
            if "guiderTime" in cmd.cmd.keywords:
                sopState.gotoField.guiderTime = float(cmd.cmd.keywords["guiderTime"].values[0])

            sopState.gotoField.setStageState("slew", "pending" if sopState.gotoField.doSlew else "off")
            sopState.gotoField.setStageState("hartmann", "pending" if sopState.gotoField.doHartmann else "off")
            sopState.gotoField.setStageState("calibs", "pending" if (sopState.gotoField.nArc > 0 or sopState.gotoField.nFlat > 0) else "off")
            sopState.gotoField.setStageState("guider", "pending" if sopState.gotoField.doGuider else "off")

            self.status(cmd, threads=False, finish=True, oneCommand="gotoField")
            return

        sopState.gotoField.cmd = None
        survey = sopState.survey
        
        sopState.gotoField.doSlew = "noSlew" not in cmd.cmd.keywords
        sopState.gotoField.doGuider = "noGuider" not in cmd.cmd.keywords
        sopState.gotoField.doHartmann = True if (survey == sopActor.BOSS and
                                                 "noHartmann" not in cmd.cmd.keywords) else False
        sopState.gotoField.arcTime = float(cmd.cmd.keywords["arcTime"].values[0]) \
                                     if "arcTime" in cmd.cmd.keywords else getDefaultArcTime(survey)
        sopState.gotoField.flatTime = float(cmd.cmd.keywords["flatTime"].values[0]) \
                                      if "flatTime" in cmd.cmd.keywords else getDefaultFlatTime(survey)
        sopState.gotoField.guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) \
                                            if "guiderFlatTime" in cmd.cmd.keywords else 0.5
        sopState.gotoField.guiderTime = float(cmd.cmd.keywords["guiderTime"].values[0]) \
                                        if "guiderTime" in cmd.cmd.keywords else 5
        sopState.gotoField.keepOffsets = "keepOffsets" in cmd.cmd.keywords

        sopState.gotoField.nArc = 0 if ("noCalibs" in cmd.cmd.keywords
                                        or sopState.gotoField.arcTime == 0
                                        or survey != sopActor.BOSS) else 1
        sopState.gotoField.nFlat = 0 if ("noCalibs" in cmd.cmd.keywords
                                         or sopState.gotoField.flatTime == 0
                                         or survey != sopActor.BOSS) else 1

        if survey == sopActor.UNKNOWN:
            cmd.warn('text="No cartridge is known to be loaded; disabling guider"')
            sopState.gotoField.doGuider = False
        #
        # How many exposures we have left/have done
        #
        sopState.gotoField.nArcLeft = sopState.gotoField.nArc; sopState.gotoField.nArcDone = 0
        sopState.gotoField.nFlatLeft = sopState.gotoField.nFlat; sopState.gotoField.nFlatDone = 0

        pointingInfo = actorState.models["platedb"].keyVarDict["pointingInfo"]
        sopState.gotoField.ra = pointingInfo[3]
        sopState.gotoField.dec = pointingInfo[4]
        sopState.gotoField.rotang = 0.0                    # Rotator angle; should always be 0.0

        if False:
            sopState.gotoField.ra = 82
            sopState.gotoField.dec = 40
            sopState.gotoField.rotang = 70
            cmd.warn('text="FAKING RA DEC:  %g, %g /rotang=%g"' % (sopState.gotoField.ra,
                                                                   sopState.gotoField.dec,
                                                                   sopState.gotoField.rotang))

        # Junk!! Must keep this in one place! Adjustment will be ugly otherwise.
        activeStages = []
        
        if sopState.gotoField.doSlew: activeStages.append("slew")
        if sopState.gotoField.doHartmann: activeStages.append("hartmann")
        if sopState.gotoField.nArc > 0 or sopState.gotoField.nFlat > 0:
            activeStages.append("calibs")
        if sopState.gotoField.doGuider: activeStages.append("guider")
        sopState.gotoField.setupCommand("gotoField", cmd,
                                        activeStages)
        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.GOTO_FIELD, actorState=actorState, cartridge=cartridge,
                            survey=sopState.survey, cmdState=sopState.gotoField).run():
            cmd.fail('text="Failed to issue gotoField"')

        # self.status(cmd, threads=False, finish=False, oneCommand="gotoField")
            
    def gotoPosition(self, cmd, name, cmdState, az, alt, rot=0):
        sopState = myGlobals.actorState
        actorState = myGlobals.actorState

        cmdState.setCommandState('running')
        cmdState.setStageState("slew", "running")

        actorState.aborting = False
        #
        # Try to guess how long the slew will take
        #
        slewDuration = 210

        multiCmd = MultiCommand(cmd, slewDuration + actorState.timeout, None)

        tccDict = actorState.models["tcc"].keyVarDict
        if az == None:
            az = tccDict['axePos'][0]
        if alt == None:
            alt = tccDict['axePos'][1]
        if rot == None:
            rot = tccDict['axePos'][2]
            
        multiCmd.append(sopActor.TCC, Msg.SLEW, actorState=actorState, az=az, alt=alt, rot=rot)

        if not multiCmd.run():
            cmdState.setStageState("slew", "failed")
            cmdState.setCommandState('failed', stateText="failed to move telescope")
            cmd.fail('text="Failed to slew to %s"' % (name))
            return
        
        cmdState.setStageState("slew", "done")
        cmdState.setCommandState('done', stateText="failed to move telescope")
        cmd.finish('text="at %s position"' % (name))

    def gotoInstrumentChange(self, cmd):
        """Go to the instrument change position"""

        actorState = myGlobals.actorState
        sopState = myGlobals.actorState

        if (sopState.survey == sopActor.BOSS and
            sopState.doScience.cmd and sopState.doScience.cmd.isAlive() and
            not (sopState.doScience.nExpLeft == 1 and actorState.models["boss"].keyVarDict["exposureState"][0] == 'READING')):
            
            cmd.warn("text='%d exposures left; exposureState=%s'" % (sopState.doScience.nExpLeft,
                                                                     actorState.models["boss"].keyVarDict["exposureState"][0]))
            cmd.fail("text='a BOSS science exposure sequence is running --- will not go to instrument change!")
            return
    
        sopState.gotoInstrumentChange.setupCommand("gotoInstrumentChange", cmd, ['slew'])
        self.gotoPosition(cmd, "instrument change", sopState.gotoInstrumentChange, 121, 90)
        
    def gotoStow(self, cmd):
        """Go to the gang connector change/stow position"""

        sopState = myGlobals.actorState

        sopState.gotoStow.setupCommand("gotoStow", cmd, ['slew'])
        self.gotoPosition(cmd, "stow", sopState.gotoStow, None, 30, rot=None)
        
    def gotoGangChange(self, cmd):
        """Go to the gang connector change position"""

        sopState = myGlobals.actorState

        sopState.gotoGangChange.setupCommand("gotoGangChange", cmd, ['slew'])
        self.gotoPosition(cmd, "gangChange", sopState.gotoGangChange, None, 30, rot=None)
        
    def ping(self, cmd):
        """ Query sop for liveness/happiness. """

        cmd.finish('text="Yawn; how soporific"')

    def restart(self, cmd):
        """Restart the worker threads"""

        threads = cmd.cmd.keywords["threads"].values if "threads" in cmd.cmd.keywords else None
        keepQueues = True if "keepQueues" in cmd.cmd.keywords else False

        if threads == ["pdb"]:
            cmd.warn('text="The sopActor is about to break to a pdb prompt"')
            import pdb; pdb.set_trace()
            cmd.finish('text="We now return you to your regularly scheduled sop session"')
            return

        actorState = myGlobals.actorState

        if actorState.restartCmd:
            actorState.restartCmd.finish("text=\"secundum verbum tuum in pace\"")
            actorState.restartCmd = None
        #
        # We can't finish this command now as the threads may not have died yet,
        # but we can remember to clean up _next_ time we restart
        #
        cmd.inform("text=\"Restarting threads\"")
        actorState.restartCmd = cmd

        actorState.actor.startThreads(actorState, cmd, restart=True,
                                      restartThreads=threads, restartQueues=not keepQueues)

    def scaleChange(self, cmd):
        """Alias for setScale
        """
        self.setScale(cmd)

    def setScale(self, cmd):
        """Change telescope scale by a factor of (1 + 0.01*delta), or to scale
        """

        actorState = myGlobals.actorState

        scale = actorState.models["tcc"].keyVarDict["scaleFac"][0]

        if "delta" in cmd.cmd.keywords:
            delta = float(cmd.cmd.keywords["delta"].values[0])

            newScale = (1 + 0.01*delta)*scale
        else:
            newScale = float(cmd.cmd.keywords["scale"].values[0])

        cmd.inform('text="currentScale=%g  newScale=%g"' % (scale, newScale))

        cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd,
                                            cmdStr="set scale=%.6f" % (newScale))
        if cmdVar.didFail:
            cmd.fail('text="Failed to set scale"')
        else:
            cmd.finish('text="scale change completed"')

    def reinit(self, cmd):
        cmd.inform('text="recreating command objects"')
        try:
            self.initCommands()
        except Exception, e:
            cmd.fail('text="failed to re-initialize command state"')
            return

        cmd.finish('')
        
    def status(self, cmd, threads=False, finish=True, oneCommand=None):
        """ Return sop status.  If threads is true report on SOP's threads; if finish complete the command"""

        sopState = myGlobals.actorState
        actorState = myGlobals.actorState

        if hasattr(cmd, 'cmd') and cmd.cmd != None and "geek" in cmd.cmd.keywords:
            threads = True
            for t in threading.enumerate():
                cmd.inform('text="%s"' % t)

        bypassStates = []
        bypassNames = []
        for name, state in Bypass.get():
            bypassNames.append(qstr(name))
            bypassStates.append("1" if state else "0")
        cmd.inform("bypassNames=" + ", ".join(bypassNames))
        cmd.inform("bypassed=" + ", ".join(bypassStates))

        cmd.inform("surveyCommands=" + ", ".join(sopState.validCommands))
        
        #
        # doCalibs
        #
        cmdState = sopState.doCalibs
        cmdState.genCommandKeys(cmd=cmd)
        if cmdState.cmd: # and sopState.doCalibs.cmd.isAlive():
            if not oneCommand or oneCommand == 'doCalibs':
                msg = []

                for keyName, default in cmdState.keywords.iteritems():
                    msg.append("%s=%s,%s" % (keyName, getattr(cmdState, keyName),
                                             default))
                msg.append("nBias=%d,%d" % (cmdState.nBiasDone, cmdState.nBias))
                msg.append("nDark=%d,%d" % (cmdState.nDarkDone, cmdState.nDark))
                msg.append("nFlat=%d,%d" % (cmdState.nFlatDone, cmdState.nFlat))
                msg.append("nArc=%d,%d" % (cmdState.nArcDone, cmdState.nArc))

                cmd.inform("; ".join(["doCalibs_"+m for m in msg]))
                
        #
        # doScience
        #
        cmdState = sopState.doScience
        cmdState.genCommandKeys(cmd=cmd)
        if cmdState.cmd: # and sopState.doScience.cmd.isAlive():
            if not oneCommand or oneCommand == 'doScience':
                msg = []

                for keyName, default in cmdState.keywords.iteritems():
                    msg.append("%s=%s,%s" % (keyName, getattr(cmdState, keyName),
                                             default))
                msg.append("nExp=%d,%d" % (cmdState.nExpDone, cmdState.nExp))
                
                cmd.inform("; ".join(["doScience_"+m for m in msg]))

        #
        # doApogeeScience
        #
        cmdState = sopState.doApogeeScience
        cmdState.genCommandKeys(cmd=cmd)
        if cmdState.cmd: # and sopState.doScience.cmd.isAlive():
            if not oneCommand or oneCommand == 'doApogeeScience':
                msg = []

                for keyName, default in cmdState.keywords.iteritems():
                    msg.append("%s=%s,%s" % (keyName, getattr(cmdState, keyName),
                                             default))
                cmd.inform("; ".join(["doApogeeScience_"+m for m in msg]))

                msg = []
                msg.append('sequenceState="%s",%d' % (sopState.doApogeeScience.exposureSeq,
                                                      sopState.doApogeeScience.index))
                cmd.inform("; ".join(["doApogeeScience_"+m for m in msg]))

        #
        # gotoField
        #
        cmdState = sopState.gotoField
        cmdState.genCommandKeys(cmd=cmd)
        if cmdState.cmd: # and sopState.gotoField.cmd.isAlive():
            if not oneCommand or oneCommand == 'gotoField':
                msg = []

                for keyName, default in cmdState.keywords.iteritems():
                    msg.append("%s=%s,%s" % (keyName, getattr(cmdState, keyName),
                                             default))
                cmd.inform("; ".join(["gotoField_"+m for m in msg]))

        #
        # commands with no state
        #
        sopState.gotoStow.genCommandKeys(cmd=cmd)
        sopState.gotoInstrumentChange.genCommandKeys(cmd=cmd)
        sopState.gotoGangChange.genCommandKeys(cmd=cmd)

        if threads:
            try:
                actorState.ignoreAborting = True
                getStatus = MultiCommand(cmd, 5.0, None)

                for tid in actorState.threads.keys():
                    getStatus.append(tid, Msg.STATUS)

                if not getStatus.run():
                    if finish:
                        cmd.fail("")
                        return
                    else:
                        cmd.warn("")
            finally:
                actorState.ignoreAborting = False

        self.actor.sendVersionKey(cmd)
        if finish:
            cmd.finish("")

    def initCommands(self):
        actorState = myGlobals.actorState
        
        actorState.gotoField = CmdState('gotoField', 
                                        ["slew", "hartmann", "calibs", "guider"],
                                        keywords=dict(arcTime=4,
                                                      flatTime=30,
                                                      guiderExpTime=5.0,
                                                      guiderFlatTime=0.5))
        actorState.doCalibs = CmdState('doCalibs',
                                       ["doCalibs"],
                                       keywords=dict(darkTime=900.0,
                                                     flatTime=30.0,
                                                     arcTime=4.0,
                                                     guiderFlatTime=0.5))
        actorState.doCalibs.nBias = 0; actorState.doCalibs.nBiasDone = 0
        actorState.doCalibs.nDark = 0; actorState.doCalibs.nDarkDone = 0
        actorState.doCalibs.nFlat = 0; actorState.doCalibs.nFlatDone = 0
        actorState.doCalibs.nArc = 0; actorState.doCalibs.nArcDone = 0

        actorState.doScience = CmdState('doScience',
                                        ["doScience"],
                                        keywords=dict(expTime=900.0))
        actorState.doScience.nExp = 0
        actorState.doScience.nExpDone = 0
        actorState.doScience.nExpLeft = 0
        
        actorState.doApogeeScience = CmdState('doApogeeScience',
                                              ["doApogeeScience"],
                                              keywords=dict(ditherSeq="A",
                                                            seqCount=7,
                                                            expTime=500.0))
        actorState.doApogeeScience.exposureSeq = "A"*7
        actorState.doApogeeScience.index = 0
        
        actorState.gotoInstrumentChange = CmdState('gotoInstrumentChange',
                                                   ["slew"])
        actorState.gotoStow = CmdState('gotoStow',
                                       ["slew"])
        actorState.gotoGangChange = CmdState('gotoGangChange',
                                             ["slew"])

        self.updateCartridge(-1)
        actorState.guiderState.setCartridgeLoadedCallback(self.updateCartridge)

    def updateCartridge(self, cartridge):
        """ Read the guider's notion of the loaded cartridge and configure ourselves appropriately. """

        actorState = myGlobals.actorState
        cmd = actorState.actor.bcast

        survey = self.classifyCartridge(cmd, cartridge)

        cmd.warn('text="loadCartridge fired cart=%s survey=%s"' % (cartridge, survey))
        
        actorState.cartridge = cartridge
        actorState.survey = survey

        if survey == sopActor.BOSS:
            actorState.gotoField.setStages(['slew', 'hartmann', 'calibs', 'guider'])
            actorState.validCommands = ['gotoField',
                                        'doHartmann', 'doCalibs', 'doScience',
                                        'gotoStow', 'gotoInstrumentChange']
        elif survey == sopActor.MARVELS:
            actorState.gotoField.setStages(['slew', 'guider'])
            actorState.validCommands = ['gotoField',
                                        'doApogeeScience',
                                        'gotoStow', 'gotoGangChange', 'gotoInstrumentChange']
        else:
            actorState.gotoField.setStages(['slew', 'guider'])
            actorState.validCommands = ['gotoStow', 'gotoInstrumentChange']
            
        self.status(cmd, threads=False, finish=False)
        # actorState.gotoField.genCommandKeys()

    def classifyCartridge(self, cmd, cartridge):
        """Return the survey type corresponding to this cartridge"""

        if fakeBoss:
            cmd.warn('text="We are lying about this being a Boss cartridge"')
            return sopActor.BOSS
        elif fakeMarvels:
            cmd.warn('text="We are lying about this being a Marvels cartridge"')
            return sopActor.MARVELS

        if cartridge <= 0:
            cmd.warn('text="We do not have a valid cartridge (id=%s)"' % (cartridge))
            return sopActor.UNKNOWN

        return sopActor.MARVELS if cartridge in range(1, 10) else sopActor.BOSS

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def getDefaultArcTime(survey):
    """Get the default arc time for this survey"""
    return 4

def getDefaultFlatTime(survey):
    """Get the default flat time for this survey"""
    return 30

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

