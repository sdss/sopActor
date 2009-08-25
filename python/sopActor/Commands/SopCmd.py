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
import masterThread


class MultiCommand(object):
    def __init__(self, cmd, timeout, *args, **kwargs):
        self.cmd = cmd
        self.replyQueue = sopActor.Queue(0)
        self.timeout = timeout
        self.commands = []

        if args:
            self.append(*args, **kwargs)

    def append(self, queue, msg, timeout=None, **kwargs):
        if timeout is not None and timeout > self.timeout:
            self.timeout = timeout
            
        self.commands.append((queue, Msg(msg, cmd=self.cmd, replyQueue=self.replyQueue, **kwargs)))

    def run(self):
        self.replyQueue.flush()

        for queue, msg in self.commands:
            queue.put(msg)

        failed = False
        try:
            for i in range(len(self.commands)): # check for all commanded subsystems to report status
                try:
                    msg = self.replyQueue.get(timeout=self.timeout)

                    if not msg.success:
                        failed = True
                except Queue.Empty:
                    self.cmd.warn('text="Queue is empty"')
                    return False
        except Queue.Empty:
            cmd.warn('text="Unexpectedly empty queue[MASTER]"')
            return False

        return not failed

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
            ("restart", "", self.restart),
            ("status", "", self.status),
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
        flatTime = float(cmd.cmd.keywords["flatTime"].values[0]) if "flatTime" in cmd.cmd.keywords else 10
        arcTime = float(cmd.cmd.keywords["arcTime"].values[0]) if "arcTime" in cmd.cmd.keywords else 2
        guiderFlatTime = float(cmd.cmd.keywords["guiderFlatTime"].values[0]) if \
                         "guiderFlatTime" in cmd.cmd.keywords else 5
        leaveScreenClosed = ("leaveScreenClosed" in cmd.cmd.keywords)

        if narc + nbias + ndark + nflat == 0:
            cmd.fail('text="You must take at least one arc, bias, dark, or flat exposure"')
            return

        actorState = myGlobals.actorState
        queue = actorState.queues[sopActor.MASTER]
        #
        # Lookup the current cartridge if we're taking guider flats
        #
        if nflat > 0 and guiderFlatTime > 0:
            try:
                cartridge = int(sopActor.myGlobals.actorState.models["guider"].keyVarDict["cartridgeLoaded"][0])
            except TypeError:
                cmd.warn('text="No cartridge is known to be loaded; not taking guider flats"')
                cartridge = 0
                cmd.warn('text="FAKING CARTRIDGE"'); cartridge=1

                
        #
        # Close the petals
        #
        if not MultiCommand(cmd, actorState.timeout,
                            actorState.queues[sopActor.FFS], Msg.FFS_MOVE, open=False).run():
            cmd.fail('text="Failed to close the flat field screen"')
            return
        #
        # Biases
        #
        if nbias + ndark > 0:
            turnLampsOff = MultiCommand(cmd, actorState.timeout)

            turnLampsOff.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=False)
            turnLampsOff.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=False)
            turnLampsOff.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=False)
            turnLampsOff.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)
            turnLampsOff.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)

            if not turnLampsOff.run():
                cmd.fail('text="Failed to prepare for bias/dark exposures"')
                return

            for n in range(nbias):
                cmd.inform('text="Taking a bias exposure"')

                if not MultiCommand(cmd, actorState.timeout,
                                    actorState.queues[sopActor.BOSS], Msg.EXPOSE,
                                    expTime=0.0, expType="bias").run():
                    cmd.fail("text=\"Failed to take bias\"")
                    return                

                cmd.inform('text="bias exposure finished"')

            for n in range(ndark):
                cmd.inform('text="Taking a %gs dark exposure"' % darkTime)

                if not MultiCommand(cmd, actorState.timeout,
                                    actorState.queues[sopActor.BOSS], Msg.EXPOSE,
                                    expTime=darkTime, expType="dark").run():
                    cmd.fail("text=\"Failed to take dark\"")
                    return                

                cmd.inform('text="bias exposure finished"')
        #
        # Flats
        #
        if nflat > 0:
            turnFFOn = MultiCommand(cmd, actorState.timeout)

            turnFFOn.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=True)
            turnFFOn.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=False)
            turnFFOn.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=False)
            turnFFOn.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)
            turnFFOn.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)

            if not turnFFOn.run():
                cmd.fail('text="Failed to prepare for flat exposure"')
                return

            for n in range(nflat):
                cmd.inform('text="Taking a %gs flat exposure"' % (flatTime))

                doFlat = MultiCommand(cmd, actorState.timeout)
                doFlat.append(actorState.queues[sopActor.BOSS], Msg.EXPOSE, expTime=flatTime, expType="flat",
                              timeout=flatTime + 180)

                if guiderFlatTime > 0 and cartridge > 0:
                    doFlat.append(actorState.queues[sopActor.GCAMERA], Msg.EXPOSE,
                                  expTime=guiderFlatTime, expType="flat", cartridge=cartridge)

                if not doFlat.run():
                    cmd.fail("text=\"Failed to take flat field\"")
                    return                

                cmd.inform('text="flat exposure finished"')
        #
        # Arcs
        #
        if narc > 0:
            turnArcsOn = MultiCommand(cmd, actorState.timeout)

            turnArcsOn.append(actorState.queues[sopActor.FF_LAMP  ], Msg.LAMP_ON, on=False)
            turnArcsOn.append(actorState.queues[sopActor.HGCD_LAMP], Msg.LAMP_ON, on=True)
            turnArcsOn.append(actorState.queues[sopActor.NE_LAMP  ], Msg.LAMP_ON, on=True)
            turnArcsOn.append(actorState.queues[sopActor.UV_LAMP  ], Msg.LAMP_ON, on=False)
            turnArcsOn.append(actorState.queues[sopActor.WHT_LAMP ], Msg.LAMP_ON, on=False)

            if not turnArcsOn.run():
                cmd.fail('text="Failed to prepare for arc exposure"')
                return

            for n in range(narc):
                cmd.inform('text="Take a %gs arc exposure here"' % (arcTime))
                if not MultiCommand(cmd, queue,
                                    actorState.queues[sopActor.BOSS], Msg.EXPOSE,
                                    expTime=arcTime, expType="arc").run():
                    cmd.fail("text=\"Failed to take arc\"")
                    return                
        #
        # We're done.  Return telescope to desired state
        #
        self.lampsOff(cmd, finish=False)

        if not leaveScreenClosed:
            if not MultiCommand(cmd, actorState.timeout,
                                actorState.queues[sopActor.FFS], Msg.FFS_MOVE, open=True).run():
                cmd.fail("text=Failed to open the flat field screen")
                return

        cmd.finish()

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

        for t in threading.enumerate():
            print "thread = ", t
        print

        cmd.finish('text="Yawn; how soporific"')

    def restart(self, cmd):
        """Restart the worker threads"""

        actorState = myGlobals.actorState

        if actorState.restartCmd:
            actorState.restartCmd.finish("text=\"secundum verbum tuum in pace\"")
            actorState.restartCmd = None

        actorState.actor.startThreads(actorState, cmd, restart=True)
        #
        # We can't finish this command now as the threads may not have died yet,
        # but we can remember to clean up _next_ time we restart
        #
        cmd.inform("text=\"Restarting threads\"")
        actorState.restartCmd = cmd

    def status(self, cmd):
        """Return guide status status"""

        actorState = myGlobals.actorState
        getStatus = MultiCommand(cmd, timeout=1.0)

        for tid in actorState.threads.keys():
            getStatus.append(actorState.queues[tid], Msg.STATUS)

        if getStatus.run():
            cmd.finish()
        else:
            cmd.fail("")
