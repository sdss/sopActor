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
                                        keys.Key("alt", types.Float(), help="what altitude to slew to"),
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
            ("doApogeeScience", "[<expTime>] [<ditherSeq>] [<seqCount>] [stop] [<abort>] [<comment>]", self.doApogeeScience),
            ("doApogeeSkyFlats", "[<expTime>] [<ditherSeq>] [stop] [abort]", self.doApogeeSkyFlats),
            ("ditheredFlat", "[sp1] [sp2] [<expTime>] [<nStep>] [<nTick>]", self.ditheredFlat),
            ("hartmann", "[sp1] [sp2] [<expTime>]", self.hartmann),
            ("lampsOff", "", self.lampsOff),
            ("ping", "", self.ping),
            ("restart", "[<threads>] [keepQueues]", self.restart),
            ("gotoField", "[<arcTime>] [<flatTime>] [<guiderFlatTime>] [<guiderExpTime>] [noSlew] [noHartmann] [noCalibs] [noGuider] [abort] [keepOffsets]",
             self.gotoField),
            ("gotoInstrumentChange", "", self.gotoInstrumentChange),
            ("gotoStow", "", self.gotoStow),
            ("gotoGangChange", "[<alt>] [abort] [stop]", self.gotoGangChange),
            ("setScale", "<delta>|<scale>", self.setScale),
            ("scaleChange", "<delta>|<scale>", self.scaleChange),
            ("status", "[geek]", self.status),
            ("reinit", "", self.reinit),
            ]
    #
    # Declare systems that can be bypassed
    #
    if not Bypass.get():
        # Pulled a couple to get the count under 9
        # "uv_lamp", "wht_lamp", "boss", "gcamera", 
        for ss in ("ffs", "ff_lamp", "hgcd_lamp", "ne_lamp", "axes",
                   "brightPlate", "darkPlate", "gangCart", "gangPodium", "slewToField"):
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
                            sopActor.MASTER, Msg.DO_SCIENCE, actorState=actorState,
                            cartridge=sopState.cartridge,
                            survey=sopState.survey, cmdState=sopState.doScience).run():
            cmd.fail('text="Failed to issue doScience"')

    def doApogeeScience(self, cmd):
        """Take a sequence of dithered APOGEE science frames, or stop or modify a running sequence."""

        actorState = myGlobals.actorState
        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeScience
        
        actorState.aborting = False

        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            if cmdState.cmd and cmdState.cmd.isAlive():
                cmd.warn('text="doApogeeScience will cancel pending exposures and stopping any running one."')

                cmdState.exposureSeq = cmdState.exposureSeq[:cmdState.index]
                # Need to work out seqCount/seqDone -- CPL
                
                cmdVar = actorState.actor.cmdr.call(actor="apogee", forUserCmd=cmd, cmdStr="expose stop")
                if cmdVar.didFail:
                    cmd.warn('text="Failed to stop running exposure"')

                cmdState.abortStages()
                self.status(cmd, threads=False, finish=True, oneCommand='doApogeeScience')
            else:
                cmd.fail('text="No doApogeeScience command is active"')
            return
                
        cmdState.expTime = float(cmd.cmd.keywords["expTime"].values[0]) if \
                                               "expTime" in cmd.cmd.keywords else 10*60.0

        if cmdState.cmd and cmdState.cmd.isAlive():
            # Modify running doApogeeScience command
            if ("ditherSeq" in cmd.cmd.keywords
                and cmd.cmd.keywords['ditherSeq'].values[0] != cmdState.ditherSeq):
                cmd.fail('text="Cannot modify dither sequence"')
                return
            
            if "seqCount" in cmd.cmd.keywords:
                seqCount = int(cmd.cmd.keywords["seqCount"].values[0])
                exposureSeq = cmdState.ditherSeq * seqCount
                cmdState.seqCount = seqCount
                cmdState.exposureSeq = exposureSeq                
                if cmdState.index > len(cmdState.exposureSeq):
                    cmd.warn('text="truncating previous exposure sequence, but NOT trying to stop current exposure"')
                    cmdState.index = len(cmdState.exposureSeq)
                    
            self.status(cmd, threads=False, finish=True, oneCommand='doApogeeScience')
            return

        seqCount = int(cmd.cmd.keywords["seqCount"].values[0]) \
                   if "seqCount" in cmd.cmd.keywords else 2
        ditherSeq = cmd.cmd.keywords["ditherSeq"].values[0] \
                    if "ditherSeq" in cmd.cmd.keywords else "ABBA"
        comment = cmd.cmd.keywords["comment"].values[0] \
                    if "comment" in cmd.cmd.keywords else ""

        cmdState.cmd = cmd
        cmdState.ditherSeq = ditherSeq
        cmdState.seqCount = seqCount
        cmdState.comment = comment
        
        exposureSeq = ditherSeq * seqCount
        cmdState.exposureSeq = exposureSeq
        cmdState.index = 0
        
        if len(cmdState.exposureSeq) == 0:
            cmd.fail('text="You must take at least one exposure"')
            return

        cmdState.setupCommand("doApogeeScience", cmd,
                              ["doApogeeScience"])
        cmd.diag('text="Issuing doApogeeScience"')
        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.DO_APOGEE_EXPOSURES, actorState=actorState,
                            expType='object',
                            cartridge=sopState.cartridge,
                            survey=sopState.survey, cmdState=cmdState).run():
            cmd.fail('text="Failed to issue doApogeeScience"')

    def doApogeeSkyFlats(self, cmd):
        """ Take sky flats. """

        actorState = myGlobals.actorState
        sopState = myGlobals.actorState
        cmdState = sopState.doApogeeSkyFlats

        if "stop" in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            if cmdState.cmd and cmdState.cmd.isAlive():
                cmd.warn('text="doApogeeSkyFlats will cancel pending exposures and stopping any running one."')

                cmdState.exposureSeq = cmdState.exposureSeq[:cmdState.index]
                # Need to work out seqCount/seqDone -- CPL
                
                cmdVar = actorState.actor.cmdr.call(actor="apogee", forUserCmd=cmd, cmdStr="expose stop")
                if cmdVar.didFail:
                    cmd.warn('text="Failed to stop running exposure"')

                cmdState.abortStages()
                self.status(cmd, threads=False, finish=True, oneCommand='doApogeeScience')
            else:
                cmd.fail('text="No doApogeeScience command is active"')
            return
                
        cmdState.expTime = float(cmd.cmd.keywords["expTime"].values[0]) if \
                           "expTime" in cmd.cmd.keywords else 150.0
        seqCount = 1
        ditherSeq = cmd.cmd.keywords["ditherSeq"].values[0] \
                    if "ditherSeq" in cmd.cmd.keywords else "ABBA"

        cmdState.cmd = cmd
        cmdState.ditherSeq = ditherSeq
        cmdState.seqCount = seqCount
        cmdState.comment = "sky flat, offset 0.01 degree in RA"
        
        exposureSeq = ditherSeq * seqCount
        cmdState.exposureSeq = exposureSeq
        cmdState.index = 0
        
        if len(cmdState.exposureSeq) == 0:
            cmd.fail('text="You must take at least one exposure"')
            return

        cmdState.setupCommand("doApogeeSkyFlats", cmd,
                              ["doApogeeSkyFlats"])
        cmd.diag('text="Issuing doApogeeSkyFlats"')

        # Offset
        cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd,
                                            cmdStr="offset arc 0.01,0.0",
                                            timeLim=actorState.timeout)
        if cmdVar.didFail:
            cmd.fail('text="Failed to take offset for sky flats."')
            return
        
        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.DO_APOGEE_EXPOSURES,
                            actorState=actorState,
                            expType='object',
                            cartridge=sopState.cartridge,
                            survey=sopState.survey, cmdState=cmdState).run():
            cmd.fail('text="Failed to issue doApogeeSkyFlats"')
        
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
        subSystems = cmd.cmd.keywords["subSystem"].values
        doBypass = False if "clear" in cmd.cmd.keywords else True

        actorState = myGlobals.actorState

        for subSystem in subSystems:
            if Bypass.set(subSystem, doBypass) is None:
                cmd.fail('text="%s is not a recognised and bypassable subSystem"' % subSystem)
                return

            if subSystem in ("darkPlate", "brightPlate"):
                # Clear the other one
                if doBypass:
                    Bypass.set("darkPlate" if subSystem == "brightPlate" else "brightPlate", False)            
                self.updateCartridge(actorState.cartridge)
            elif subSystem in ('gangPodium', 'gangCart'):
                # Clear the other one
                if doBypass:
                    Bypass.set("gangCart" if subSystem == "gangPodium" else "gangPodium", False)            
                cmd.warn('text="gang bypass: %s"' % (actorState.apogeeGang.getPos()))

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
        survey = sopState.survey

        # APOGEE -- CPL
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

        # Tricky. if we are APOGEE, behave differently depending on where the gang connector is:
        #  on podium: take a guider flat.
        #  elsewhere: don't
        #
        # Take out the BOSS test if we trust the switches/bypasses
        if survey != sopActor.BOSS:
            if actorState.apogeeGang.atPodium():
                sopState.gotoField.doGuiderFlat = True
            else:
                shutterStatus = actorState.models["apogee"].keyVarDict["shutterLimitSwitch"]
                if shutterStatus[0] and not shutterStatus[1]:                
                    sopState.gotoField.doGuiderFlat = True
                else:
                    cmd.warn('text="skipping guider flat because APOGGE gang connector is not on the podium _and_ the cold shutter is open."')
                    sopState.gotoField.doGuiderFlat = False
        else:
            sopState.gotoField.doGuiderFlat = (True if (sopState.gotoField.doGuider and
                                                        sopState.gotoField.guiderFlatTime > 0)
                                               else False)
    
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

        if Bypass.set(name='slewToField'):
            here = actorState.tccState.here(cmd)
            sopState.gotoField.ra = here[0]
            sopState.gotoField.dec = here[1]
            sopState.gotoField.rotang = here[2]
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
                            sopActor.MASTER, Msg.GOTO_FIELD, actorState=actorState,
                            cartridge=sopState.cartridge,
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

        actorState = myGlobals.actorState
        sopState = myGlobals.actorState

        alt = float(cmd.cmd.keywords["alt"].values[0]) \
              if "alt" in cmd.cmd.keywords else 45.0

        cmdState = sopState.gotoGangChange
        cmdState.alt = alt
        
        if 'stop' in cmd.cmd.keywords or 'abort' in cmd.cmd.keywords:
            cmd.fail('text"sorry, I cannot stop or abort a gotoGangChange command. (yet)"')
            return
        
        cmdState.setupCommand("gotoGangChange", cmd, ['slew'])

        actorState.queues[sopActor.MASTER].put(Msg.GOTO_GANG_CHANGE, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState, cmdState=cmdState,
                                               alt=alt, survey=actorState.survey)
        
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
        """ (engineering command) Recreate the objects which hold the state of the various top-level commands. """
        
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
        bypassedNames = [] 
        for name, state in Bypass.get():
            bypassNames.append(qstr(name))
            bypassStates.append("1" if state else "0")
            if state:
                bypassedNames.append(qstr(name))
                
        cmd.inform("bypassNames=" + ", ".join(bypassNames))
        cmd.inform("bypassed=" + ", ".join(bypassStates))
        cmd.inform("bypassedNames=" + ", ".join(bypassedNames))

        cmd.inform("surveyCommands=" + ", ".join(sopState.validCommands))
        
        #
        # major commands
        #
        sopState.gotoField.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doCalibs.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doScience.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doApogeeScience.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.doApogeeSkyFlats.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.gotoGangChange.genKeys(cmd=cmd, trimKeys=oneCommand)

        #
        # commands with no state
        #
        sopState.gotoStow.genKeys(cmd=cmd, trimKeys=oneCommand)
        sopState.gotoInstrumentChange.genKeys(cmd=cmd, trimKeys=oneCommand)

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

        cmd.inform('text="apogeeGang: %s"' % (actorState.apogeeGang.getPos()))
        self.actor.sendVersionKey(cmd)
        if finish:
            cmd.finish("")

    def initCommands(self):
        actorState = myGlobals.actorState
        
        actorState.gotoField = GotoFieldCmd()
        actorState.doCalibs = DoCalibsCmd()
        actorState.doScience = DoScienceCmd()
        actorState.doApogeeScience = DoApogeeScienceCmd()
        actorState.doApogeeSkyFlats = DoApogeeSkyFlatsCmd()
        actorState.gotoGangChange = GotoGangChangeCmd()
        
        actorState.gotoInstrumentChange = CmdState('gotoInstrumentChange',
                                                   ["slew"])
        actorState.gotoStow = CmdState('gotoStow',
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
                                        'hartmann', 'doCalibs', 'doScience',
                                        'gotoStow', 'gotoInstrumentChange']
        elif survey == sopActor.MARVELS:
            actorState.gotoField.setStages(['slew', 'guider'])
            actorState.validCommands = ['gotoField',
                                        'doApogeeScience', 'doApogeeSkyFlats',
                                        'gotoStow', 'gotoGangChange', 'gotoInstrumentChange']
        else:
            actorState.gotoField.setStages(['slew', 'guider'])
            actorState.validCommands = ['gotoStow', 'gotoInstrumentChange']

        self.status(cmd, threads=False, finish=False)

    def classifyCartridge(self, cmd, cartridge):
        """Return the survey type corresponding to this cartridge"""

        if Bypass.get(name='brightPlate'):
            cmd.warn('text="We are lying about this being a Boss cartridge"')
            return sopActor.BOSS
        elif Bypass.get(name='darkPlate'):
            cmd.warn('text="We are lying about this being a bright-time cartridge"')
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

class GotoGangChangeCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'gotoGangChange',
                          ["slew"],
                          keywords=dict(alt=45.0))
    
class GotoFieldCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'gotoField', 
                          ["slew", "hartmann", "calibs", "guider"],
                          keywords=dict(arcTime=4,
                                        flatTime=30,
                                        guiderExpTime=5.0,
                                        guiderFlatTime=0.5))

class DoCalibsCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doCalibs',
                          ["doCalibs"],
                          keywords=dict(darkTime=900.0,
                                        flatTime=30.0,
                                        arcTime=4.0,
                                        guiderFlatTime=0.5))
        self.nBias = 0; self.nBiasDone = 0
        self.nDark = 0; self.nDarkDone = 0
        self.nFlat = 0; self.nFlatDone = 0
        self.nArc = 0; self.nArcDone = 0

    def getUserKeys(self):
        msg = []
        msg.append("nBias=%d,%d" % (self.nBiasDone, self.nBias))
        msg.append("nDark=%d,%d" % (self.nDarkDone, self.nDark))
        msg.append("nFlat=%d,%d" % (self.nFlatDone, self.nFlat))
        msg.append("nArc=%d,%d" % (self.nArcDone, self.nArc))

        return ["%s_%s" % (self.name, m) for m in msg]
    
class DoApogeeScienceCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doApogeeScience',
                          ["doApogeeScience"],
                          keywords=dict(ditherSeq="ABBA",
                                        expTime=500.0,
                                        comment=""))
        self.seqCount = 0
        self.seqDone = 0
        
        self.exposureSeq = "ABBA"*2
        self.index = 0

    def getUserKeys(self):
        msg = []
        msg.append("%s_seqCount=%d,%d" % (self.name,
                                          self.seqDone, self.seqCount))
        msg.append('%s_sequenceState="%s",%d' % (self.name,
                                                 self.exposureSeq,
                                                 self.index))
        return msg

class DoApogeeSkyFlatsCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doApogeeSkyFlats',
                          ["doApogeeSkyFlats"],
                          keywords=dict(ditherSeq="ABBA",
                                        expTime=150.0))
        self.seqCount = 0
        self.seqDone = 0
        
        self.exposureSeq = "ABBA"
        self.index = 0

class DoScienceCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doScience',
                          ["doScience"],
                          keywords=dict(expTime=900.0))
        self.nExp = 0
        self.nExpDone = 0
        self.nExpLeft = 0
        
    def getUserKeys(self):
        msg = []

        msg.append("%s_nExp=%d,%d" % (self.name,
                                      self.nExpDone, self.nExp))
        return msg
