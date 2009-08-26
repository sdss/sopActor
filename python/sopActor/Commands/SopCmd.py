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
                                        keys.Key("arcTime", types.Float(), help="Exposure time for arcs"),
                                        keys.Key("darkTime", types.Float(), help="Exposure time for flats"),
                                        keys.Key("flatTime", types.Float(), help="Exposure time for flats"),
                                        keys.Key("guiderFlatTime", types.Float(), help="Exposure time for guider flats"),
                                        keys.Key("leaveScreenClosed", help="Don't open FF screens at end of command"),
                                        keys.Key("geek", help="Show things that only some of us love"),
                                        keys.Key("threads", types.String()*(1,), help="Threads to restart; default: all"),
                                        )
        #
        # Declare commands
        #
        self.vocab = [
            ("doCalibs",
             "[<narc>] [<nbias>] [<ndark>] [<nflat>] [<arcTime>] [<darkTime>] [<flatTime>] [<guiderFlatTime>] [leaveScreenClosed]", self.doCalibs),
            ("bias", "", self.bias),
            ("lampsOff", "", self.lampsOff),
            ("ping", "", self.ping),
            ("restart", "[<threads>]", self.restart),
            ("status", "[geek]", self.status),
            ]
    #
    # Define commands' callbacks
    #
    def bias(self, cmd):
        cmd.inform('text="Taking a bias exposure"')

        actorState = myGlobals.actorState
        queue = actorState.queues[sopActor.MASTER]

        if not MultiCommand(cmd, actorState.timeout,
                            actorState.queues[sopActor.BOSS], Msg.EXPOSE,
                            expTime=0.0, expType="bias").run():
            cmd.fail("text=\"Failed to take bias\"")
            return                

        cmd.finish('text="bias exposure finished"')

    def doCalibs(self, cmd):
        """Take a set of calibration frames"""

        narc = int(cmd.cmd.keywords["narc"].values[0])   if "narc" in cmd.cmd.keywords else 0
        nbias = int(cmd.cmd.keywords["nbias"].values[0]) if "nbias" in cmd.cmd.keywords else 0
        ndark = int(cmd.cmd.keywords["ndark"].values[0]) if "ndark" in cmd.cmd.keywords else 0
        nflat = int(cmd.cmd.keywords["nflat"].values[0]) if "nflat" in cmd.cmd.keywords else 0
        arcTime = float(cmd.cmd.keywords["arcTime"].values[0]) if "arcTime" in cmd.cmd.keywords else 2
        darkTime = float(cmd.cmd.keywords["darkTime"].values[0]) if "darkTime" in cmd.cmd.keywords else -1
        flatTime = float(cmd.cmd.keywords["flatTime"].values[0]) if "flatTime" in cmd.cmd.keywords else 10
        guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) if \
                         "guiderFlatTime" in cmd.cmd.keywords else 5
        leaveScreenClosed = ("leaveScreenClosed" in cmd.cmd.keywords)

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
        cartridge = 0
        if nflat > 0 and guiderFlatTime > 0:
            try:
                cartridge = int(actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
            except TypeError:
                cmd.warn('text="No cartridge is known to be loaded; not taking guider flats"')

        actorState.queues[sopActor.MASTER].put(Msg.DO_CALIB, cmd, replyQueue=actorState.queues[sopActor.MASTER],
                                               actorState=actorState,
                                               narc=narc, nbias=nbias, ndark=ndark, nflat=nflat,
                                               flatTime=flatTime, arcTime=arcTime, darkTime=darkTime,
                                               cartridge=cartridge, guiderFlatTime=guiderFlatTime,
                                               leaveScreenClosed=leaveScreenClosed)

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
                cmd.finish()
        else:
            if finish:
                cmd.fail('text="Some lamps failed to turn off"')

    def ping(self, cmd):
        """ Top-level 'ping' command handler. Query the actor for liveness/happiness. """

        cmd.finish('text="Yawn; how soporific"')

    def restart(self, cmd):
        """Restart the worker threads"""

        threads = cmd.cmd.keywords["threads"].values if "threads" in cmd.cmd.keywords else None

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

        actorState.actor.startThreads(actorState, cmd, restart=True, restartThreads=threads)

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
