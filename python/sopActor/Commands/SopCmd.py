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

if False:
    oldPostcondition = sopActor.Postcondition
    print "Reloading sopActor";
    reload(sopActor)
    sopActor.Postcondition = oldPostcondition

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class SopPostcondition(Postcondition):
    """This class is used to pass preconditions for a command to MultiCmd.  Only if required() returns True
is the command actually scheduled and then run"""
    
    def __init__(self, queueName, msgId=None, timeout=None, **kwargs):
        Postcondition.__init__(self, queueName, msgId, timeout, **kwargs)
        self.queueName = queueName

    def required(self):
        """Here is the real logic.  We're thinking of running a command to get the system into a
desired state, but if it's already in that state no command is required; so return False"""

        if self.queueName in (sopActor.FF_LAMP,
                              sopActor.HGCD_LAMP, sopActor.NE_LAMP, sopActor.WHT_LAMP, sopActor.UV_LAMP):
            
            assert self.msgId == Msg.LAMP_ON

            if self.kwargs.get('on'):                    # we want to turn them on
                return not self.lampIsOn(self.queueName) # op is required if they are not already on
            else:
                return self.lampIsOn(self.queueName)
        elif self.queueName in (sopActor.FFS,):
            assert self.msgId == Msg.FFS_MOVE

            if self.kwargs.get('open'): # we want to open them
                return not self.ffsAreOpen() # op is required if they are not already open
            else:
                return self.ffsAreOpen()

        return True
    #
    # Commands to get state from e.g. the MCP
    #
    def ffsAreOpen(self):
        """Return True if flat field petals are open; False if they are close, and None if indeterminate"""

        ffsStatus = myGlobals.actorState.models["mcp"].keyVarDict["ffsStatus"]

        open, closed = 0, 0
        for s in ffsStatus:
            if s == None:
                raise RuntimeError, "Unable to read FFS status"

            open += int(s[0])
            closed += int(s[1])

        if open == 8:
            return True
        elif closed == 8:
            return False
        else:
            return None

    def lampIsOn(self, queueName):
        """Return True iff some lamps are on"""

        if queueName == sopActor.FF_LAMP:
            status = myGlobals.actorState.models["mcp"].keyVarDict["ffLamp"]
        elif queueName == sopActor.HGCD_LAMP:
            status = myGlobals.actorState.models["mcp"].keyVarDict["hgCdLamp"]
        elif queueName == sopActor.NE_LAMP:
            status = myGlobals.actorState.models["mcp"].keyVarDict["neLamp"]
        elif queueName == sopActor.UV_LAMP:
            return myGlobals.actorState.models["mcp"].keyVarDict["uvLampCommandedOn"]
        elif queueName == sopActor.WHT_LAMP:
            return myGlobals.actorState.models["mcp"].keyVarDict["whtLampCommandedOn"]
        else:
            print "Unknown lamp queue %s" % queueName
            return False

        if status == None:
            raise RuntimeError, ("Unable to read %s lamp status" % queueName)

        on = 0
        for i in status:
            on += i

        return True if on == 4 else False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class SopCmd(object):
    """ Wrap commands to the sop actor"""

    def __init__(self, actor):
        self.actor = actor
        #
        # Declare keys that we're going to use
        #
        self.keys = keys.KeysDictionary("sop_sop", (1, 1),
                                        keys.Key("clear", help="Clear a flag"),
                                        keys.Key("narc", types.Int(), help="Number of arcs to take"),
                                        keys.Key("nbias", types.Int(), help="Number of biases to take"),
                                        keys.Key("ndark", types.Int(), help="Number of darks to take"),
                                        keys.Key("nflat", types.Int(), help="Number of flats to take"),
                                        keys.Key("nStep", types.Int(), help="Number of dithered exposures to take"),
                                        keys.Key("nTick", types.Int(), help="Number of ticks to move collimator"),
                                        keys.Key("arcTime", types.Float(), help="Exposure time for arcs"),
                                        keys.Key("darkTime", types.Float(), help="Exposure time for flats"),
                                        keys.Key("expTime", types.Float(), help="Exposure time"),
                                        keys.Key("guiderTime", types.Float(), help="Exposure time for guider"),
                                        keys.Key("fiberId", types.Int(), help="A fiber ID"),
                                        keys.Key("flatTime", types.Float(), help="Exposure time for flats"),
                                        keys.Key("cartridge", types.Int(), help="A cartridge ID"),
                                        keys.Key("inEnclosure", help="We are in the enclosure"),
                                        keys.Key("guiderFlatTime", types.Float(), help="Exposure time for guider flats"),
                                        keys.Key("hartmann", help="Take a Hartmann sequence"),
                                        keys.Key("keepQueues", help="Restart thread queues"),
                                        keys.Key("openFFS", help="Open flat field screen"),
                                        keys.Key("startGuider", help="Start the guider"),
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
             "[<narc>] [<nbias>] [<ndark>] [<nflat>] [<arcTime>] [<darkTime>] [<flatTime>] [<guiderFlatTime>] [inEnclosure] [<cartridge>]",
             self.doCalibs),
            ("doScience", "<expTime>", self.doScience),
            ("ditheredFlat", "[sp1] [sp2] [<expTime>] [<nStep>] [<nTick>]", self.ditheredFlat),
            ("fk5InFiber", "<fiberId>", self.fk5InFiber),
            ("hartmann", "[sp1] [sp2] [<expTime>]", self.hartmann),
            ("lampsOff", "", self.lampsOff),
            ("ping", "", self.ping),
            ("restart", "[<threads>] [keepQueues]", self.restart),
            ("gotoField", "[<arcTime>] [<flatTime>] [<guiderFlatTime>] [hartmann] [openFFS] [startGuider]",
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
        for ss in ("ffs", "ff_lamp", "hgcd_lamp", "ne_lamp", "uv_lamp", "wht_lamp", "boss", "gcamera", "tcc"):
            Bypass.set(ss, False, define=True)
    #
    # Define commands' callbacks
    #
    def doCalibs(self, cmd):
        """Take a set of calibration frames"""

        narc = int(cmd.cmd.keywords["narc"].values[0])   if "narc" in cmd.cmd.keywords else 0
        nbias = int(cmd.cmd.keywords["nbias"].values[0]) if "nbias" in cmd.cmd.keywords else 0
        ndark = int(cmd.cmd.keywords["ndark"].values[0]) if "ndark" in cmd.cmd.keywords else 0
        nflat = int(cmd.cmd.keywords["nflat"].values[0]) if "nflat" in cmd.cmd.keywords else 0
        arcTime = float(cmd.cmd.keywords["arcTime"].values[0]) if "arcTime" in cmd.cmd.keywords else 4
        darkTime = float(cmd.cmd.keywords["darkTime"].values[0]) if "darkTime" in cmd.cmd.keywords else -1
        flatTime = float(cmd.cmd.keywords["flatTime"].values[0]) if "flatTime" in cmd.cmd.keywords else 30
        guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) if \
                         "guiderFlatTime" in cmd.cmd.keywords else 0.5
        cartridge = int(cmd.cmd.keywords["cartridge"].values[0]) if "cartridge" in cmd.cmd.keywords else 0
        inEnclosure = True if "inEnclosure" in cmd.cmd.keywords else False
        startGuider = True if "startGuider" in cmd.cmd.keywords else False

        if narc + nbias + ndark + nflat == 0:
            cmd.fail('text="You must take at least one arc, bias, dark, or flat exposure"')
            return

        if ndark and darkTime < 0:
            cmd.fail('text="Please decide on a value for darkTime"')
            return

        actorState = myGlobals.actorState
        #
        # Lookup the current cartridge if we're taking guider flats
        #
        if nflat > 0 and guiderFlatTime > 0:
            try:
                cartridge = int(actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
            except TypeError:
                cmd.warn('text="No cartridge is known to be loaded; not taking guider flats"')

        if isMarvelsCartridge(cartridge):
            flatTime = 0                # no need to take a BOSS flat

        actorState.queues[sopActor.MASTER].put(Msg.DO_CALIB, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState,
                                               narc=narc, nbias=nbias, ndark=ndark, nflat=nflat,
                                               flatTime=flatTime, arcTime=arcTime, darkTime=darkTime,
                                               cartridge=cartridge, guiderFlatTime=guiderFlatTime,
                                               inEnclosure=inEnclosure, startGuider=startGuider)

    def doScience(self, cmd):
        """Take a set of science frames"""

        expTime = float(cmd.cmd.keywords["expTime"].values[0])

        actorState = myGlobals.actorState
        actorState.queues[sopActor.MASTER].put(Msg.DO_SCIENCE, cmd,
                                               replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState, expTime=expTime)

    def lampsOff(self, cmd, finish=True):
        """Turn all the lamps off"""

        actorState = myGlobals.actorState

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

        if Bypass.set(subSystem, doBypass) is None:
            cmd.fail('text="%s is not a recognised and bypassable subSystem"' % subSystem)
            return

        self.status(cmd)

    def ditheredFlat(self, cmd, finish=True):
        """Take a set of nStep dithered flats, moving the collimator by nTick between exposures"""

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

        actorState = myGlobals.actorState
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

        expTime = float(cmd.cmd.keywords["expTime"].values[0]) if "expTime" in cmd.cmd.keywords else 4
        sp1 = "sp1" in cmd.cmd.keywords
        sp2 = "sp2" in cmd.cmd.keywords
        if not sp1 and not sp2:
            sp1 = True; sp2 = True; 

        actorState = myGlobals.actorState
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

        arcTime = float(cmd.cmd.keywords["arcTime"].values[0]) if "arcTime" in cmd.cmd.keywords else 4
        flatTime = float(cmd.cmd.keywords["flatTime"].values[0]) if "flatTime" in cmd.cmd.keywords else 30
        doHartmann = True if "hartmann" in cmd.cmd.keywords else False
        openFFS = True if "openFFS" in cmd.cmd.keywords else False
        doGuider = True if "startGuider" in cmd.cmd.keywords else False
        guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) \
                         if "guiderFlatTime" in cmd.cmd.keywords else 5
        guiderTime = float(cmd.cmd.keywords["guiderTime"].values[0]) if "guiderTime" in cmd.cmd.keywords else 5
        openFFSAtEnd = openFFS or doGuider

        doArc = True if arcTime > 0 else False
        doFlat = True if flatTime > 0 else False
        closeFFSAtStart = True if (doArc or doFlat or doHartmann or doGuider) else False

        if doGuider:
            try:
                cartridge = int(actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
                if False:
                    if isMarvelsCartridge(cartridge):
                        flatTime = 0                # no need to take a BOSS flat
            except TypeError:
                cmd.warn('text="No cartridge is known to be loaded; disabling guider"')
                doGuider = False

        doGuiderFlat = True if (doGuider and guiderFlatTime > 0) else False

        pointingInfo = actorState.models["platedb"].keyVarDict["pointingInfo"]
        boresight_ra = pointingInfo[3]
        boresight_dec = pointingInfo[4]

        if False:
            cmd.warn('text="FAKING RA DEC"')
            boresight_ra = 16*15
            boresight_dec = 50
        #
        # Define the command that we use to communicate our state to e.g. STUI
        #
        slewCompleted, hartmannCompleted, flatCompleted, arcCompleted, startedGuider = [0]*5
        def informGUI(txt='text="gotoField=slew,%%d,%d, hartmann,%%d,%d, '
                      'flat,%%d,%d, arc,%%d,%d, guider,%%d,%d"' % (1, doHartmann,
                                                                   (doFlat or doGuiderFlat), doArc, doGuider)):
            cmd.inform(txt % (slewCompleted, hartmannCompleted, flatCompleted, arcCompleted, startedGuider))
        #
        # Try to guess how long the slew will take
        #
        if False:
            import time; print "start slew", time.ctime()
            slewDurationKey = actorState.models["tcc"].keyVarDict["slewDuration"]
            cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd,
                                                cmdStr="track %f, %f icrs /rottype=object/rotang=0.0" % \
                                                (boresight_ra, boresight_dec), timeLim=4,
                                                keyVars=[slewDurationKey])
            print cmdVar.getLastKeyVarData
            slewDuration = cmdVar.getLastKeyVarData(slewDurationKey)[0]
            print slewDuration
            import time; print "end attempted slew", time.ctime()
        else:
            slewDuration = 180

        informGUI()
        
        multiCmd = MultiCommand(cmd, slewDuration + actorState.timeout)

        if True:
            multiCmd.append(sopActor.TCC, Msg.SLEW, actorState=actorState, ra=boresight_ra, dec=boresight_dec)
        else:
            cmd.warn('text="RHL skipping slew"')

        if doFlat or doGuiderFlat:
            multiCmd.append(SopPostcondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=True))
        if doArc or doHartmann:
            multiCmd.append(SopPostcondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=True))
            multiCmd.append(SopPostcondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=True))
            multiCmd.append(SopPostcondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
        if closeFFSAtStart:
            multiCmd.append(SopPostcondition(sopActor.FFS      , Msg.FFS_MOVE, open=False))

        if not multiCmd.run():
            cmd.fail('text="Failed to close screens, warm up lamps, and slew to field"')
            return

        slewCompleted = True
        informGUI()
        #
        # OK, we're there.  Time to do calibs etc.
        #
        if doHartmann:
            multiCmd = MultiCommand(cmd, actorState.timeout)

            multiCmd.append(sopActor.BOSS, Msg.HARTMANN)

            multiCmd.append(SopPostcondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=True))
            multiCmd.append(SopPostcondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=True))
            multiCmd.append(SopPostcondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.FFS      , Msg.FFS_MOVE, open=False))

            if not multiCmd.run():
                cmd.fail('text="Failed to do Hartmann sequence"')
                return

            hartmannCompleted = True
            informGUI()
        #
        # Calibs
        #
        readoutTime = 90
        pendingReadout = False          # there is no pending readout
        if doFlat or doGuiderFlat:
            multiCmd = MultiCommand(cmd, actorState.timeout)

            multiCmd.append(SopPostcondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=True))
            multiCmd.append(SopPostcondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.FFS      , Msg.FFS_MOVE, open=False))

            if not multiCmd.run():
                cmd.fail('text="Failed to prepare for flats"')
                return
            #
            # Now take the exposure
            #
            multiCmd = MultiCommand(cmd, flatTime + actorState.timeout)
            
            if doFlat:
                pendingReadout = True
                multiCmd.append(sopActor.BOSS, Msg.EXPOSE,
                                expTime=flatTime, expType="flat", readout=False)
            if doGuiderFlat:
                multiCmd.append(sopActor.GCAMERA, Msg.EXPOSE,
                                expTime=guiderFlatTime, expType="flat", cartridge=cartridge)
                
            if not multiCmd.run():
                cmd.fail('text="Failed to take flats"')
                return

            flatCompleted = True
            informGUI()

        if doArc:
            multiCmd = MultiCommand(cmd, actorState.timeout + (readoutTime if pendingReadout else 0))

            if pendingReadout:
                multiCmd.append(sopActor.BOSS, Msg.EXPOSE, expTime=-1, readout=True)
                pendingReadout = False
                
            multiCmd.append(SopPostcondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=True))
            multiCmd.append(SopPostcondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=True))
            multiCmd.append(SopPostcondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.FFS      , Msg.FFS_MOVE, open=False))

            if not multiCmd.run():
                cmd.fail('text="Failed to prepare for arcs"')
                return
            #
            # Now take the exposure
            #
            pendingReadout = True
            if not MultiCommand(cmd, arcTime + actorState.timeout, sopActor.BOSS, Msg.EXPOSE,
                                expTime=arcTime, expType="arc", readout=False).run():
                cmd.fail('text="Failed to take arcs"')
                return

            arcCompleted = True
            informGUI()
        #
        # Readout any pending data and prepare to guide
        #
        if pendingReadout:
            readoutMultiCmd = MultiCommand(cmd, readoutTime + actorState.timeout)

            readoutMultiCmd.append(sopActor.BOSS, Msg.EXPOSE, expTime=-1, readout=True)
            pendingReadout = False

            readoutMultiCmd.start()
        else:
            readoutMultiCmd = None

        multiCmd = MultiCommand(cmd, actorState.timeout + (readoutTime if pendingReadout else 0))

        multiCmd.append(SopPostcondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False))
        multiCmd.append(SopPostcondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False))
        multiCmd.append(SopPostcondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=False))
        multiCmd.append(SopPostcondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
        multiCmd.append(SopPostcondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
        if openFFSAtEnd:
            multiCmd.append(sopActor.FFS, Msg.FFS_MOVE, open=True)

        if not multiCmd.run():
            cmd.fail('text="Failed to prepare to guide"')
            return
        #
        # Start the guider
        #
        if doGuider:
            multiCmd = MultiCommand(cmd, actorState.timeout + guiderTime)

            for w in ("axes", "focus", "scale"):
                multiCmd.append(sopActor.GUIDER, Msg.ENABLE, what=w, on=False)

            multiCmd.append(sopActor.GUIDER, Msg.START, on=True, expTime=guiderTime, oneExposure=True)

            multiCmd.append(SopPostcondition(sopActor.FF_LAMP  , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.HGCD_LAMP, Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.NE_LAMP  , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.WHT_LAMP , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.UV_LAMP  , Msg.LAMP_ON, on=False))
            multiCmd.append(SopPostcondition(sopActor.FFS      , Msg.FFS_MOVE, open=True))

            if not multiCmd.run():
                cmd.fail('text="Failed to start guiding"')
                return

            startedGuider = True
            informGUI()
        #
        # Catch the last readout's completion
        #
        if readoutMultiCmd and not readoutMultiCmd.finish():
            cmd.fail('text="Failed to readout last exposure"')
            return
        #
        # We're done
        #

        cmd.finish('text="on field')

    def gotoInstrumentChange(self, cmd):
        """Go to the instrument change position"""
        
        actorState = myGlobals.actorState
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

    def status(self, cmd):
        """Return sop status"""

        actorState = myGlobals.actorState

        if "geek" in cmd.cmd.keywords:
            for t in threading.enumerate():
                cmd.inform('text="%s"' % t)

            cmd.finish()
            return

        getStatus = MultiCommand(cmd, timeout=1.0)

        for tid in actorState.threads.keys():
            getStatus.append(tid, Msg.STATUS)

        bypassState = []
        for name, state in Bypass.get():
            bypassState.append("%s,%s" % (name, "True" if state else "False"))
        cmd.inform("bypassed=" + ", ".join(bypassState))

        if getStatus.run():
            cmd.finish()
        else:
            cmd.fail("")

def isMarvelsCartridge(cartridge):
    """Return True iff the cartridge number corresponds to a MARVELS cartridge"""

    return True if cartridge in range(1, 10) else False
