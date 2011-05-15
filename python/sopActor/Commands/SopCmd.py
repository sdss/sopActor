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

class SopState(object):
    """The state of SOP"""

    class state(object):
        validStageStates = ('starting', 'prepping', 'running', 'done', 'failed', 'aborted')

        """A class that's intended to hold state data"""
        def __init__(self, name, allStages):
            self.name = name
            self.cmd = None
            self.cmdState = "idle"
            self.allStages = allStages
            self.stages = dict(zip(self.allStages, ["idle"] * len(self.allStages)))
            self.stateText="OK"

        def setupCommand(self, name, cmd, activeStages):
            self.name = name
            self.cmd = cmd
            self.stateText="OK"
            self.activeStages = activeStages
            for s in self.allStages:
                self.stages[s] = "pending" if s in activeStages else "off"
            self.genCommandKeys()

        def setCommandState(self, state, genKeys=True, stateText=None):
            self.cmdState = state
            if stateText:
                self.stateText=stateText

            if genKeys:
                self.genCmdStateKeys()

        def setStageState(self, name, stageState, genKeys=True):
            assert name in self.stages
            assert stageState in self.validStageStates, "state %s is unknown" % (stageState)
            self.stages[name] = stageState

            if genKeys:
                self.genCmdStateKeys()

        def abortStages(self):
            """ Mark all unstarted stages as aborted. """
            for s in self.allStages:
                if not self.stages[s] in ("pending", "done", "failed"):
                    self.stages[s] = "aborted"
            self.genCmdStateKeys()

        def setActiveStages(self, stages, genKeys=True):
            raise NotImplementedError()
            for s in stages:
                assert s in self.allStages

        def genCmdStateKeys(self, cmd=None):
            if not cmd:
                cmd = self.cmd
            cmd.inform("%sState=%s,%s,%s" % (self.name, qstr(self.cmdState),
                                             qstr(self.stateText),
                                             ",".join([qstr(self.stages[sname]) \
                                                           for sname in self.allStages])))
            
        def genCommandKeys(self, cmd=None):
            """ Return a list of the keywords describing our command. """

            if not cmd:
                cmd = self.cmd

            cmd.inform("%sStages=%s" % (self.name,
                                        ",".join([qstr(sname) \
                                                      for sname in self.allStages])))
            self.genCmdStateKeys(cmd=cmd)
            
    def __init__(self):
        self.gotoField = SopState.state('gotoField', 
                                        ["slew", "hartmann", "calibs", "guider"])
        self.doCalibs = SopState.state('doCalibs',
                                       ["doCalibs"])
        self.doScience = SopState.state('doScience',
                                        ["doScience"])
        
sopState = SopState()
try:
    sopState
except:
    sopState = SopState()

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
            ("ditheredFlat", "[sp1] [sp2] [<expTime>] [<nStep>] [<nTick>]", self.ditheredFlat),
            ("hartmann", "[sp1] [sp2] [<expTime>]", self.hartmann),
            ("lampsOff", "", self.lampsOff),
            ("ping", "", self.ping),
            ("restart", "[<threads>] [keepQueues]", self.restart),
            ("gotoField", "[<arcTime>] [<flatTime>] [<guiderFlatTime>] [<guiderExpTime>] [noSlew] [noHartmann] [noCalibs] [noGuider] [abort] [keepOffsets]",
             self.gotoField),
            ("gotoInstrumentChange", "", self.gotoInstrumentChange),
            ("setScale", "<delta>|<scale>", self.setScale),
            ("scaleChange", "<delta>|<scale>", self.scaleChange),
            ("status", "[geek]", self.status),
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
        try:
            cartridge = int(actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
        except TypeError:
            cartridge = -1

        survey = classifyCartridge(cmd, cartridge)
        if survey != sopActor.BOSS:
            cmd.warn('text="current cartridge is not for BOSS; continuing with calibs anyhow."')
            survey = sopActor.BOSS

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
        #
        # Lookup the current cartridge
        #
        try:
            cartridge = int(actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
        except TypeError:
            cmd.warn('text="No cartridge is known to be loaded"')
            cartridge = -1
                
        survey = classifyCartridge(cmd, cartridge)

        sopState.doScience.setupCommand("doScience", cmd,
                                        ["doScience"])
        if not MultiCommand(cmd, 2, None,
                            sopActor.MASTER, Msg.DO_SCIENCE, actorState=actorState, cartridge=cartridge,
                            survey=survey, cmdState=sopState.doScience).run():
            cmd.fail('text="Failed to issue doScience"')

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

        if subSystem == "planets":
            global fakeBoss
            fakeBoss = doBypass
            cmd.finish('text="%s"' % ("I'm studying some very faint fuzzies" if fakeBoss else ""))
            return
        elif subSystem == "science":
            global fakeMarvels
            fakeMarvels = doBypass
            cmd.finish('text="%s"' % ("Ah, a Marvels night" if fakeMarvels else ""))
            return

        if Bypass.set(subSystem, doBypass) is None:
            cmd.fail('text="%s is not a recognised and bypassable subSystem"' % subSystem)
            return

        self.status(cmd, threads=False)

    def ditheredFlat(self, cmd, finish=True):
        """Take a set of nStep dithered flats, moving the collimator by nTick between exposures"""

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

                sopState.gotoField.nArc = sopState.gotoField.nArcDone = 0
                sopState.gotoField.nFlat = sopState.gotoField.nFlatDone = 0
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
        #
        # Lookup the current cartridge
        #
        try:
            cartridge = int(actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
        except TypeError:
            cartridge = -1

        survey = classifyCartridge(cmd, cartridge)

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
            sopState.gotoField.ra = 150
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
                            survey=survey, cmdState=sopState.gotoField).run():
            cmd.fail('text="Failed to issue gotoField"')

        # self.status(cmd, threads=False, finish=False, oneCommand="gotoField")
            
    def gotoInstrumentChange(self, cmd):
        """Go to the instrument change position"""

        actorState = myGlobals.actorState

        if (sopState.doScience.cmd and sopState.doScience.cmd.isAlive() and
            (sopState.doScience.nExpLeft > 1 or
             actorState.models["boss"].keyVarDict["exposureState"][0] != 'READING')):
            
            cmd.warn("text='%d left; exposureState=%s'" % (sopState.doScience.nExpLeft,
                                                           actorState.models["boss"].keyVarDict["exposureState"][0]))
            cmd.fail("text='a science exposure sequence is running -- will not go to instrument change!")
            return
    
        actorState.aborting = False
        #
        # Try to guess how long the slew will take
        #
        slewDuration = 180

        multiCmd = MultiCommand(cmd, slewDuration + actorState.timeout, None)

        multiCmd.append(sopActor.TCC, Msg.SLEW, actorState=actorState, az=121, alt=90, rot=0)

        if not multiCmd.run():
            cmd.fail('text="Failed to slew to instrument change"')
            return
        
        cmd.finish('text="At instrument change position')

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

    def status(self, cmd, threads=False, finish=True, oneCommand=None):
        """ Return sop status.  If threads is true report on SOP's threads; if finish complete the command"""

        actorState = myGlobals.actorState

        if "geek" in cmd.cmd.keywords:
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

        #
        # doCalibs
        #
        sopState.doCalibs.genCommandKeys(cmd=cmd)
        if sopState.doCalibs.cmd: # and sopState.doCalibs.cmd.isAlive():
            if not oneCommand or oneCommand == 'doCalibs':
                msg = []

                msg.append("nBias=%d,%d" % (sopState.doCalibs.nBiasDone, sopState.doCalibs.nBias))
                msg.append("nDark=%d,%d" % (sopState.doCalibs.nDarkDone, sopState.doCalibs.nDark))
                msg.append("nFlat=%d,%d" % (sopState.doCalibs.nFlatDone, sopState.doCalibs.nFlat))
                msg.append("nArc=%d,%d" % (sopState.doCalibs.nArcDone, sopState.doCalibs.nArc))

                msg.append("darkTime=%g,%g" % (sopState.doCalibs.darkTime, 900))
                msg.append("flatTime=%g,%g" % (sopState.doCalibs.flatTime, 30))
                msg.append("arcTime=%g,%g" % (sopState.doCalibs.arcTime, 4))
                msg.append("guiderFlatTime=%g,%g" % (sopState.doCalibs.guiderFlatTime, 0.5))

                cmd.inform("; ".join(["doCalibs_"+m for m in msg]))
                
        #
        # doScience
        #
        sopState.doScience.genCommandKeys(cmd=cmd)
        if sopState.doScience.cmd: # and sopState.doScience.cmd.isAlive():
            if not oneCommand or oneCommand == 'doScience':
                msg = []

                msg.append("nExp=%d,%d" % (sopState.doScience.nExpDone, sopState.doScience.nExp))
                msg.append("expTime=%g,%g" % (sopState.doScience.expTime, 900))
            
                cmd.inform("; ".join(["doScience_"+m for m in msg]))

        #
        # gotoField
        #
        sopState.gotoField.genCommandKeys(cmd=cmd)
        if sopState.gotoField.cmd: # and sopState.gotoField.cmd.isAlive():
            if not oneCommand or oneCommand == 'gotoField':
                msg = []

                #msg.append("slew=%s" % ("yes" if sopState.gotoField.doSlew else "no"))
                #msg.append("hartmann=%s" % ("yes" if sopState.gotoField.doHartmann else "no"))
                #msg.append("guider=%s" % ("yes" if sopState.gotoField.doGuider else "no"))

                #msg.append("nArc=%d,%d" % (sopState.gotoField.nArcDone, sopState.gotoField.nArc))
                #msg.append("nFlat=%d,%d" % (sopState.gotoField.nFlatDone, sopState.gotoField.nFlat))

                msg.append("arcTime=%g,%g" % (sopState.gotoField.arcTime, 4))
                msg.append("flatTime=%g,%g" % (sopState.gotoField.flatTime, 30))
                msg.append("guiderExpTime=%g,%g" % (sopState.gotoField.guiderTime, 5.0))
                msg.append("guiderFlatTime=%g,%g" % (sopState.gotoField.guiderFlatTime, 0.5))
                
                cmd.inform("; ".join(["gotoField_"+m for m in msg]))

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

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def getDefaultArcTime(survey):
    """Get the default arc time for this survey"""
    return 4 if survey == sopActor.BOSS else 0

def getDefaultFlatTime(survey):
    """Get the default flat time for this survey"""
    return 30 if survey == sopActor.BOSS else 0

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def classifyCartridge(cmd, cartridge):
    """Return the survey type corresponding to this cartridge"""

    if fakeBoss:
        cmd.warn('text="We are lying about this being a Boss cartridge"')
        return sopActor.BOSS
    elif fakeMarvels:
        cmd.warn('text="We are lying about this being a Marvels cartridge"')
        return sopActor.MARVELS
    else:
        if cartridge <= 0:
            return sopActor.UNKNOWN
        
        return sopActor.MARVELS if cartridge in range(1, 10) else sopActor.BOSS
