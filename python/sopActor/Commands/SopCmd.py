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
    print "Reloading sopActor";
    reload(sopActor)
    sopActor.Precondition = oldPrecondition

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class SopState(object):
    """The state of SOP"""

    class state(object):
        """A class that's intended to hold state data"""
        def __init__(self):
            self.cmd = None

    def __init__(self):
        self.gotoField = SopState.state()
        self.doCalibs = SopState.state()
        self.doScience = SopState.state()

try:
    sopState
except:
    sopState = SopState()

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

try:
    fakeMarvels                         # force this to be interpreted as a Marvels cartridge
except:
    fakeMarvels = False

class SopCmd(object):
    """ Wrap commands to the sop actor"""

    def __init__(self, actor):
        self.actor = actor
        #
        # Declare keys that we're going to use
        #
        self.keys = keys.KeysDictionary("sop_sop", (1, 1),
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
                                        keys.Key("sp1", help="Select SP1"),
                                        keys.Key("sp2", help="Select SP2"),
                                        keys.Key("geek", help="Show things that only some of us love"),
                                        keys.Key("subSystem", types.String(), help="The sub-system to bypass"),
                                        keys.Key("threads", types.String()*(1,), help="Threads to restart; default: all"),
                                        keys.Key("scale", types.Float(), help="Current scale from \"tcc show scale\""),
                                        keys.Key("delta", types.Float(), help="Delta scale (percent)"),
                                        keys.Key("absolute", help="Set scale to provided value"),
                                        )
        #
        # Declare commands
        #
        self.vocab = [
            ("bypass", "<subSystem> [clear]", self.bypass),
            ("doCalibs",
             "[<narc>] [<nbias>] [<ndark>] [<nflat>] [<arcTime>] [<darkTime>] [<flatTime>] [<guiderFlatTime>]",
             self.doCalibs),
            ("doScience", "[<expTime>] [<nexp>]", self.doScience),
            ("ditheredFlat", "[sp1] [sp2] [<expTime>] [<nStep>] [<nTick>]", self.ditheredFlat),
            ("fk5InFiber", "<fiberId>", self.fk5InFiber),
            ("hartmann", "[sp1] [sp2] [<expTime>]", self.hartmann),
            ("lampsOff", "", self.lampsOff),
            ("ping", "", self.ping),
            ("restart", "[<threads>] [keepQueues]", self.restart),
            ("gotoField", "[<arcTime>] [<flatTime>] [<guiderFlatTime>] [noSlew] [noHartmann] [noGuider] [abort]",
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
        for ss in ("ffs", "ff_lamp", "hgcd_lamp", "ne_lamp", "uv_lamp", "wht_lamp", "boss", "gcamera"):
            Bypass.set(ss, False, define=True)
    #
    # Define commands' callbacks
    #
    def doCalibs(self, cmd):
        """Take a set of calibration frames"""

        actorState = myGlobals.actorState
        actorState.aborting = False

        if "abort" in cmd.cmd.keywords:
            if sopState.doCalibs.cmd and sopState.doCalibs.cmd.isAlive():
                actorState.aborting = True
                cmd.warn('text="doCalibs will abort when it finishes its current activities; be patient"')

                sopState.doCalibs.nArc = sopState.doCalibs.nArcLeft = sopState.doCalibs.nArcDone = 0
                sopState.doCalibs.nBias = sopState.doCalibs.nBiasLeft = sopState.doCalibs.nBiasDone = 0
                sopState.doCalibs.nDark = sopState.doCalibs.nDarkLeft = sopState.doCalibs.nDarkDone = 0
                sopState.doCalibs.nFlat = sopState.doCalibs.nFlatLeft = sopState.doCalibs.nFlatDone = 0

                self.status(cmd, threads=False, finish=True)
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

            self.status(cmd, threads=False, finish=True)
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
                                    if "arcTime" in cmd.cmd.keywords else 4
        sopState.doCalibs.darkTime = float(cmd.cmd.keywords["darkTime"].values[0]) \
                                     if "darkTime" in cmd.cmd.keywords else 0
        sopState.doCalibs.flatTime = float(cmd.cmd.keywords["flatTime"].values[0]) \
                                     if "flatTime" in cmd.cmd.keywords else 30
        sopState.doCalibs.guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) \
                                           if "guiderFlatTime" in cmd.cmd.keywords else 0

        if sopState.doCalibs.nArc + sopState.doCalibs.nBias + \
               sopState.doCalibs.nDark + sopState.doCalibs.nFlat == 0:
            cmd.fail('text="You must take at least one arc, bias, dark, or flat exposure"')
            return

        if sopState.doCalibs.nDark and sopState.doCalibs.darkTime < 0:
            cmd.fail('text="Please decide on a value for darkTime"')
            return
        #
        # How many exposures we have left/have done
        #
        sopState.doCalibs.nArcLeft = sopState.doCalibs.nArc; sopState.doCalibs.nArcDone = 0
        sopState.doCalibs.nBiasLeft = sopState.doCalibs.nBias; sopState.doCalibs.nBiasDone = 0
        sopState.doCalibs.nDarkLeft = sopState.doCalibs.nDark; sopState.doCalibs.nDarkDone = 0
        sopState.doCalibs.nFlatLeft = sopState.doCalibs.nFlat; sopState.doCalibs.nFlatDone = 0
        #
        # Lookup the current cartridge if we're taking guider flats
        #
        try:
            cartridge = int(actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
        except TypeError:
            cartridge = -1
                
        survey = classifyCartridge(cartridge)

        if sopState.doCalibs.nFlat > 0 and sopState.doCalibs.guiderFlatTime > 0:
            if cartridge < 0:
                cmd.warn('text="No cartridge is known to be loaded; not taking guider flats"')
                sopState.doCalibs.guiderFlatTime = 0
                
        if survey == sopActor.MARVELS:
            sopState.doCalibs.flatTime = 0                # no need to take a BOSS flat

        if not MultiCommand(cmd, 2,
                            sopActor.MASTER, Msg.DO_CALIBS, actorState=actorState, cartridge=cartridge,
                            survey=survey, cmdState=sopState.doCalibs).run():
            cmd.fail('text="Failed to issue doCalibs"')

        sopState.doCalibs.cmd = cmd

        self.status(cmd, threads=False, finish=False)

    def doScience(self, cmd):
        """Take a set of science frames"""

        actorState = myGlobals.actorState
        actorState.aborting = False

        if "abort" in cmd.cmd.keywords:
            if sopState.doScience.cmd and sopState.doScience.cmd.isAlive():
                actorState.aborting = True
                cmd.warn('text="doScience will abort when it finishes its current activities; be patient"')

                sopState.doScience.nExp = sopState.doScience.nExpLeft = sopState.doScience.nExpDone = 0

                self.status(cmd, threads=False, finish=True)
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

            self.status(cmd, threads=False, finish=True)
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
                
        survey = classifyCartridge(cartridge)

        if not MultiCommand(cmd, 2,
                            sopActor.MASTER, Msg.DO_SCIENCE, actorState=actorState, cartridge=cartridge,
                            survey=survey, cmdState=sopState.doScience).run():
            cmd.fail('text="Failed to issue doScience"')

        sopState.doScience.cmd = cmd

        self.status(cmd, threads=False, finish=False)
    def lampsOff(self, cmd, finish=True):
        """Turn all the lamps off"""

        actorState = myGlobals.actorState
        actorState.aborting = False

        multiCmd = MultiCommand(cmd, actorState.timeout)

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

        if subSystem == "science":
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
        expTime = float(cmd.cmd.keywords["expTime"].values[0]) if "expTime" in cmd.cmd.keywords else 2

        actorState.queues[sopActor.MASTER].put(Msg.DITHERED_FLAT, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState,
                                               expTime=expTime, spN=spN, nStep=nStep, nTick=nTick)

    def hartmann(self, cmd, finish=True):
        """Take two arc exposures, one with the Hartmann left screen in and one with the right one in.

If the flat field screens are initially open they are closed, and the Ne/HgCd lamps are turned on.
You may specify using only one spectrograph with sp1 or sp2; the default is both.
The exposure time is set by expTime (default: 4s)

When the sequence is finished the Hartmann screens are moved out of the beam, the lamps turned off, and the
flat field screens returned to their initial state.
"""
        actorState = myGlobals.actorState
        actorState.aborting = False

        expTime = float(cmd.cmd.keywords["expTime"].values[0]) if "expTime" in cmd.cmd.keywords else 4
        sp1 = "sp1" in cmd.cmd.keywords
        sp2 = "sp2" in cmd.cmd.keywords
        if not sp1 and not sp2:
            sp1 = True; sp2 = True; 

        actorState.queues[sopActor.MASTER].put(Msg.HARTMANN, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState, expTime=expTime, sp1=sp1, sp2=sp2)

    def fk5InFiber(self, cmd):
        fiberId = int(cmd.cmd.keywords["fiberId"].values[0])

        cmd.finish('text="fiber=%d"' % fiberId)

    def gotoField(self, cmd):
        """Slew to the current cartridge/pointing

Slew to the position of the currently loaded cartridge. At the beginning of the slew all the lamps are turned on and the flat field screen petals are closed.  When you arrive at the field, all the lamps are turned off again and the flat field petals are opened if you specified openFFS.
        """
        
        actorState = myGlobals.actorState
        actorState.aborting = False

        if "abort" in cmd.cmd.keywords:
            if sopState.gotoField.cmd and sopState.gotoField.cmd.isAlive():
                actorState.aborting = True

                cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd, cmdStr="track /stop")
                if cmdVar.didFail:
                    cmd.warn('text="Failed to abort slew"')

                sopState.gotoField.doSlew = False

                sopState.gotoField.nArc = sopState.gotoField.nArcLeft = sopState.gotoField.nArcDone = 0
                sopState.gotoField.nFlat = sopState.gotoField.nFlatLeft = sopState.gotoField.nFlatDone = 0
                sopState.gotoField.doHartmann = False
                sopState.gotoField.doGuider = False

                cmd.warn('text="gotoField will abort when it finishes its current activities; be patient"')
                self.status(cmd, threads=False, finish=True)
            else:
                cmd.fail('text="No gotoField command is active"')

            return

        if sopState.gotoField.cmd and sopState.gotoField.cmd.isAlive():
            # Modify running gotoField command
            sopState.gotoField.doSlew = True if "noSlew" not in cmd.cmd.keywords else False
            sopState.gotoField.doGuider = True if "noGuider" not in cmd.cmd.keywords else False
            sopState.gotoField.doHartmann = True if "noHartmann" not in cmd.cmd.keywords else False

            if "arcTime" in cmd.cmd.keywords:
                if sopState.gotoField.nArcDone > 0:
                    cmd.warn('text="Arcs are taken; it\'s too late to modify arcTime"')
                else:
                    sopState.gotoField.arcTime = float(cmd.cmd.keywords["arcTime"].values[0])
                    sopState.gotoField.nArc = 1 if sopState.gotoField.arcTime > 0 else 0
                    sopState.gotoField.nArcLeft = sopState.gotoField.nArc
            if "flatTime" in cmd.cmd.keywords:
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

            self.status(cmd, threads=False, finish=True)
            return

        sopState.gotoField.cmd = None

        sopState.gotoField.doSlew = True if "noSlew" not in cmd.cmd.keywords else False
        sopState.gotoField.doGuider = True if "noGuider" not in cmd.cmd.keywords else False
        sopState.gotoField.doHartmann = True if "noHartmann" not in cmd.cmd.keywords else False
        sopState.gotoField.arcTime = float(cmd.cmd.keywords["arcTime"].values[0]) \
                                     if "arcTime" in cmd.cmd.keywords else 4
        sopState.gotoField.flatTime = float(cmd.cmd.keywords["flatTime"].values[0]) \
                                      if "flatTime" in cmd.cmd.keywords else 30
        sopState.gotoField.guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) \
                                            if "guiderFlatTime" in cmd.cmd.keywords else 0.5
        sopState.gotoField.guiderTime = float(cmd.cmd.keywords["guiderTime"].values[0]) \
                                        if "guiderTime" in cmd.cmd.keywords else 5

        sopState.gotoField.nArc = 1 if sopState.gotoField.arcTime > 0 else 0
        sopState.gotoField.nFlat = 1 if sopState.gotoField.flatTime > 0 else 0
        #
        # How many exposures we have left/have done
        #
        sopState.gotoField.nArcLeft = sopState.gotoField.nArc; sopState.gotoField.nArcDone = 0
        sopState.gotoField.nFlatLeft = sopState.gotoField.nFlat; sopState.gotoField.nFlatDone = 0

        try:
            cartridge = int(actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
        except TypeError:
            cmd.warn('text="No cartridge is known to be loaded; disabling guider"')
            cartridge = -1
            sopState.gotoField.doGuider = False

        pointingInfo = actorState.models["platedb"].keyVarDict["pointingInfo"]
        sopState.gotoField.ra = pointingInfo[3]
        sopState.gotoField.dec = pointingInfo[4]
        sopState.gotoField.rotang = 0.0                    # Rotator angle; should always be 0.0

        if False:
            sopState.gotoField.ra = 17*15
            sopState.gotoField.dec = 40
            sopState.gotoField.rotang = 120
            cmd.warn('text="FAKING RA DEC:  %g, %g /rotang=%g"' % (sopState.gotoField.ra,
                                                                   sopState.gotoField.dec,
                                                                   sopState.gotoField.rotang))

        survey = classifyCartridge(cartridge)

        if not MultiCommand(cmd, 2,
                            sopActor.MASTER, Msg.GOTO_FIELD, actorState=actorState, cartridge=cartridge,
                            survey=survey, cmdState=sopState.gotoField).run():
            cmd.fail('text="Failed to issue gotoField"')

        sopState.gotoField.cmd = cmd

        self.status(cmd, threads=False, finish=False)
            
    def gotoInstrumentChange(self, cmd):
        """Go to the instrument change position"""
        
        actorState = myGlobals.actorState
        actorState.aborting = False
        #
        # Try to guess how long the slew will take
        #
        slewDuration = 180

        multiCmd = MultiCommand(cmd, slewDuration + actorState.timeout)

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

    def status(self, cmd, threads=False, finish=True):
        """Return sop status.  If threads is true report on SOP's threads; if finish complete the command"""

        actorState = myGlobals.actorState

        if "geek" in cmd.cmd.keywords:
            threads = True
            for t in threading.enumerate():
                cmd.inform('text="%s"' % t)

        bypassState = []
        for name, state in Bypass.get():
            bypassState.append("%s,%s" % (name, "True" if state else "False"))
        cmd.inform("bypassed=" + ", ".join(sorted(bypassState)))
        #
        # doCalibs
        #
        if sopState.doCalibs.cmd and sopState.doCalibs.cmd.isAlive():
            msg = []

            msg.append("doCalibs=active")
            msg.append("nBias=%d,%d" % (sopState.doCalibs.nBiasDone, sopState.doCalibs.nBias))
            msg.append("nDark=%d,%d" % (sopState.doCalibs.nDarkDone, sopState.doCalibs.nDark))
            msg.append("darkTime=%g" % sopState.doCalibs.darkTime)
            msg.append("nFlat=%d,%d" % (sopState.doCalibs.nFlatDone, sopState.doCalibs.nFlat))
            msg.append("flatTime=%g" % sopState.doCalibs.flatTime)
            msg.append("guiderFlatTime=%g" % sopState.doCalibs.guiderFlatTime)
            msg.append("nArc=%d,%d" % (sopState.doCalibs.nArcDone, sopState.doCalibs.nArc))
            msg.append("arcTime=%g" % sopState.doCalibs.arcTime)

            cmd.inform('text="%s"' % " ".join(msg))
        else:
            cmd.inform('text="doCalibs=inactive"')
        #
        # doScience
        #
        if sopState.doScience.cmd and sopState.doScience.cmd.isAlive():
            msg = []

            msg.append("doScience=active")
            msg.append("nExp=%d,%d" % (sopState.doScience.nExpDone, sopState.doScience.nExp))
            msg.append("expTime=%g" % sopState.doScience.expTime)

            cmd.inform('text="%s"' % " ".join(msg))
        else:
            cmd.inform('text="doScience=inactive"')
        #
        # gotoField
        #
        if sopState.gotoField.cmd and sopState.gotoField.cmd.isAlive():
            msg = []

            msg.append("gotoField=active")
            msg.append("slew=%s" % ("yes" if sopState.gotoField.doSlew else "no"))
            msg.append("hartmann=%s" % ("yes" if sopState.gotoField.doHartmann else "no"))
            msg.append("nArc=%d,%d" % (sopState.gotoField.nArcDone, sopState.gotoField.nArc))
            msg.append("arcTime=%g" % sopState.gotoField.arcTime)
            msg.append("nFlat=%d,%d" % (sopState.gotoField.nFlatDone, sopState.gotoField.nFlat))
            msg.append("flatTime=%g" % sopState.gotoField.flatTime)
            msg.append("guider=%s" % ("yes" if sopState.gotoField.doGuider else "no"))
            msg.append("guiderExpTime=%g" % sopState.gotoField.guiderTime)
            msg.append("guiderFlatTime=%g" % sopState.gotoField.guiderFlatTime)

            cmd.inform('text="%s"' % " ".join(msg))
        else:
            cmd.inform('text="gotoField=inactive"')

        if threads:
            try:
                actorState.ignoreAborting = True
                getStatus = MultiCommand(cmd, timeout=5.0)

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

        if finish:
            cmd.finish("")

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def classifyCartridge(cartridge):
    """Return the survey type corresponding to this cartridge"""

    if fakeMarvels:
        cmd.warn('text="We are lying about this being a Marvels cartridge"')
        return sopActor.MARVELS
    else:
        if cartridge <= 0:
            return sopActor.UNKNOWN
        
        return sopActor.MARVELS if cartridge in range(1, 10) else sopActor.BOSS
