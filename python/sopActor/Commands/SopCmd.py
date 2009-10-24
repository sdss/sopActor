#!/usr/bin/env python

""" Wrap top-level ICC functions. """

import pdb
import re, sys, time
import Queue, threading

import opscore.protocols.keys as keys
import opscore.protocols.types as types

from opscore.utility.qstr import qstr

from sopActor import *
import sopActor
import sopActor.myGlobals as myGlobals
from sopActor import MultiCommand

class SopCmd(object):
    """ Wrap commands to the sop actor"""

    def __init__(self, actor):
        self.actor = actor
        #
        # Declare keys that we're going to use
        #
        self.keys = keys.KeysDictionary("sop_sop", (1, 1),
                                        keys.Key("narc", types.Int(), help="Number of arcs to take"),
                                        keys.Key("nbias", types.Int(), help="Number of biases to take"),
                                        keys.Key("ndark", types.Int(), help="Number of darks to take"),
                                        keys.Key("nflat", types.Int(), help="Number of flats to take"),
                                        keys.Key("nStep", types.Int(), help="Number of dithered exposures to take"),
                                        keys.Key("nTick", types.Int(), help="Number of ticks to move collimator"),
                                        keys.Key("arcTime", types.Float(), help="Exposure time for arcs"),
                                        keys.Key("darkTime", types.Float(), help="Exposure time for flats"),
                                        keys.Key("expTime", types.Float(), help="Exposure time"),
                                        keys.Key("fiberId", types.Int(), help="A fiber ID"),
                                        keys.Key("flatTime", types.Float(), help="Exposure time for flats"),
                                        keys.Key("cartridge", types.Int(), help="A cartridge ID"),
                                        keys.Key("guiderFlatTime", types.Float(), help="Exposure time for guider flats"),
                                        keys.Key("keepQueues", help="Restart thread queues"),
                                        keys.Key("openFFS", help="Open flat field screen"),
                                        keys.Key("startGuider", help="Start the guider"),
                                        keys.Key("sp1", help="Select SP1"),
                                        keys.Key("sp2", help="Select SP2"),
                                        keys.Key("geek", help="Show things that only some of us love"),
                                        keys.Key("threads", types.String()*(1,), help="Threads to restart; default: all"),
                                        keys.Key("scale", types.Float(), help="Current scale from \"tcc show scale\""),
                                        keys.Key("delta", types.Float(), help="Delta scale (percent)"),
                                        keys.Key("absolute", help="Set scale to provided value"),
                                        )
        #
        # Declare commands
        #
        self.vocab = [
            ("doCalibs",
             "[<narc>] [<nbias>] [<ndark>] [<nflat>] [<arcTime>] [<darkTime>] [<flatTime>] [<guiderFlatTime>] [openFFS] [<cartridge>]",
             self.doCalibs),
            ("doScience", "[<expTime>]", self.doScience),
            ("ditheredFlat", "[sp1] [sp2] [<expTime>] [<nStep>] [<nTick>]", self.ditheredFlat),
            ("fk5InFiber", "<fiberId>", self.fk5InFiber),
            ("hartmann", "[sp1] [sp2] [<expTime>]", self.hartmann),
            ("lampsOff", "", self.lampsOff),
            ("ping", "", self.ping),
            ("restart", "[<threads>] [keepQueues]", self.restart),
            ("gotoField", "[openFFS] [startGuider]", self.gotoField),
            ("gotoInstrumentChange", "", self.gotoInstrumentChange),
            ("setScale", "<delta>|<scale>", self.setScale),
            ("scaleChange", "<delta>|<scale>", self.scaleChange),
            ("status", "[geek]", self.status),
            ]
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
        flatTime = float(cmd.cmd.keywords["flatTime"].values[0]) if "flatTime" in cmd.cmd.keywords else 20
        guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) if \
                         "guiderFlatTime" in cmd.cmd.keywords else 0.5
        cartridge = int(cmd.cmd.keywords["cartridge"].values[0]) if "cartridge" in cmd.cmd.keywords else 0
        openFFS = True if "openFFS" in cmd.cmd.keywords else False
        startGuider = True if "startGuider" in cmd.cmd.keywords else False

        if narc + nbias + ndark + nflat == 0:
            cmd.fail('text="You must take at least one arc, bias, dark, or flat exposure"')
            return

        if ndark and darkTime < 0:
            cmd.fail('text="Please decide on a value for darkTime"')
            return

        actorState = sopActor.myGlobals.actorState
        #
        # Lookup the current cartridge if we're taking guider flats
        #
        if nflat > 0 and guiderFlatTime > 0:
            try:
                cartridge = int(actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
            except TypeError:
                cmd.warn('text="No cartridge is known to be loaded; not taking guider flats"')

        actorState.queues[sopActor.MASTER].put(Msg.DO_CALIB, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState,
                                               narc=narc, nbias=nbias, ndark=ndark, nflat=nflat,
                                               flatTime=flatTime, arcTime=arcTime, darkTime=darkTime,
                                               cartridge=cartridge, guiderFlatTime=guiderFlatTime, openFFS=openFFS,
                                               startGuider=startGuider)

    def doScience(self, cmd):
        """Take a set of science frames"""

        expTime = float(cmd.cmd.keywords["expTime"].values[0])

        actorState = sopActor.myGlobals.actorState
        actorState.queues[sopActor.MASTER].put(Msg.DO_SCIENCE, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState, expTime=expTime)

    def lampsOff(self, cmd, finish=True):
        """Turn all the lamps off"""

        actorState = myGlobals.actorState

        multiCmd = MultiCommand(cmd, actorState.timeout)

        multiCmd.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)

        if multiCmd.run():
            if finish:
                cmd.finish('text="Turned lamps off"')
        else:
            if finish:
                cmd.fail('text="Some lamps failed to turn off"')

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
        """Take three arc exposures with the Hartmann screens out, left in, and right in"""

        expTime = float(cmd.cmd.keywords["expTime"].values[0]) if "expTime" in cmd.cmd.keywords else 4
        sp1 = "sp1" in cmd.cmd.keywords
        sp2 = "sp2" in cmd.cmd.keywords

        actorState = myGlobals.actorState
        actorState.queues[sopActor.MASTER].put(Msg.HARTMANN, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState, expTime=expTime, sp1=sp1, sp2=sp2)

    def fk5InFiber(self, cmd):
        fiberId = int(cmd.cmd.keywords["fiberId"].values[0])

        cmd.finish('text="fiber=%d"' % fiberId)

    def gotoField(self, cmd):
        """Slew to the current cartridge/pointing"""
        
        actorState = myGlobals.actorState

        openFFS = True if "openFFS" in cmd.cmd.keywords else False

        pointingInfo = actorState.models["platedb"].keyVarDict["pointingInfo"]
        boresight_ra = pointingInfo[3]
        boresight_dec = pointingInfo[4]

        if False:
            cmd.warn('text="FAKING RA DEC"')
            boresight_ra = 16*15
            boresight_dec = 50
        #
        # Try to guess how long the slew will take
        #
        if False:
            import time; print "start slew", time.ctime()
            slewDurationKey = actorState.models["tcc"].keyVarDict["slewDuration"]
            #import pdb; pdb.set_trace()
            cmdVar = actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd,
                                                cmdStr="track %f, %f icrs /rottype=object/rotang=0.0" % \
                                                (boresight_ra, boresight_dec), timeLim=4, keyVars=[slewDurationKey])
            print cmdVar.getLastKeyVarData
            slewDuration = cmdVar.getLastKeyVarData(slewDurationKey)[0]
            print slewDuration
            import time; print "end attempted slew", time.ctime()
        else:
            slewDuration = 180

        multiCmd = MultiCommand(cmd, slewDuration + actorState.timeout)

        multiCmd.append(actorState.queues[sopActor.TCC      ], Msg.SLEW, actorState=actorState,
                        ra=boresight_ra, dec=boresight_dec)
        multiCmd.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=True)
        multiCmd.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=True)
        multiCmd.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=True)
        multiCmd.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.FFS      ], Msg.FFS_MOVE, open=False)

        if not multiCmd.run():
            cmd.fail('text="Failed to close screens, warm up lamps, and slew to field"')
            return
        
        multiCmd = MultiCommand(cmd, slewDuration + actorState.timeout)

        multiCmd.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)
        multiCmd.append(actorState.queues[sopActor.FFS      ], Msg.FFS_MOVE, open=openFFS)

        if not multiCmd.run():
            cmd.fail('text="Failed to open screens"')
            return

        cmd.finish('text="on field')

    def gotoInstrumentChange(self, cmd):
        """Go to the instrument change position"""
        
        actorState = myGlobals.actorState
        #
        # Try to guess how long the slew will take
        #
        slewDuration = 180

        multiCmd = MultiCommand(cmd, slewDuration + actorState.timeout)

        multiCmd.append(actorState.queues[sopActor.TCC], Msg.SLEW, actorState=actorState, az=121, alt=90, rot=0)

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
        """Return guide status status"""

        actorState = myGlobals.actorState

        if "geek" in cmd.cmd.keywords:
            for t in threading.enumerate():
                cmd.inform('text="%s"' % t)

            cmd.finish()
            return

        getStatus = MultiCommand(cmd, timeout=1.0)

        for tid in actorState.threads.keys():
            getStatus.append(actorState.queues[tid], Msg.STATUS)

        if getStatus.run():
            cmd.finish()
        else:
            cmd.fail("")
