#!/usr/bin/env python
"""An actor to run the guider"""

import inspect, os, re, sys
import Queue, threading

import opscore.actor.model
import opscore.actor.keyvar

import actorcore.Actor
import actorcore.CmdrConnection

import actorkeys

import masterThread
import bossThread
import gcameraThread
import ffsThread
import lampThreads
        
from sopActor import *
import sopActor.myGlobals
#
# Import sdss3logging before logging if you want to use it
#
if False:
    import opscore.utility.sdss3logging as sdss3logging
import logging

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class State(object):
    """An object to hold globally useful state"""

    def __init__(self, actor):
        self.actor = actor
        self.dispatcher = self.actor.cmdr.dispatcher
        self.reactor = self.dispatcher.reactor
        self.models = {}
        self.restartCmd = None

    def __str__(self):
        msg = "%s %s %s" % (self.actor, self.actor.cmdr.dispatcher, self.dispatcher.reactor)

        return msg
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class Sop(actorcore.Actor.Actor):
    def __init__(self, name, configFile, debugLevel=30):
        actorcore.Actor.Actor.__init__(self, name, configFile)

        self.logger.setLevel(debugLevel)
        #self.logger.propagate = True

        self.cmdr = actorcore.CmdrConnection.Cmdr(name, self)
        self.cmdr.connectionMade = self.connectionMade
        self.cmdr.connect()

        sopActor.myGlobals.actorState = State(self)
        actorState = sopActor.myGlobals.actorState
        #
        # Load other actor's models so we can send it commands
        #
        for actor in ["guider", "mcp", "tcc"]:
            actorState.models[actor] = opscore.actor.model.Model(actor)
        #
        # spawn off the threads that sequence actions (e.g. take an exposure; move telescope)
        # and talk to the gcamera
        # 
        actorState.timeout = 60         # timeout on message queues

        Sop.startThreads(actorState, startQueues=True)
        #
        # Handle the hated ini file
        #
        import ConfigParser
        #
        # Finally start the reactor
        #
        self.run()

    def connectionMade(self):
        self.bcast.warn("Sop is connected.")
        
    @staticmethod
    def startThreads(actorState, cmd=None, startQueues=False, restart=False):
        """Start or restart the worker threads and queues"""

        try:
            actorState.threads
        except AttributeError:
            restart = False
        
        if not restart:
            actorState.queues = {}
            actorState.threads = {}

            startQueues = True

        for tname, tid, threadModule, target in [("master",  sopActor.MASTER,    masterThread,  masterThread.main),
                                                 ("boss",    sopActor.BOSS,      bossThread,    bossThread.main),
                                                 ("gcamera", sopActor.GCAMERA,   gcameraThread, gcameraThread.main),
                                                 ("ff",      sopActor.FF_LAMP,   lampThreads,   lampThreads.ff_main),
                                                 ("hgcd",    sopActor.HGCD_LAMP, None,          lampThreads.hgcd_main),
                                                 ("ne",      sopActor.NE_LAMP,   None,          lampThreads.ne_main),
                                                 ("uv",      sopActor.UV_LAMP,   None,          lampThreads.uv_main),
                                                 ("wht",     sopActor.WHT_LAMP,  None,          lampThreads.wht_main),
                                                 ("ffs",     sopActor.FFS,       ffsThread,     ffsThread.main),
                                                 ]:
            if startQueues:
                actorState.queues[tid] = sopActor.Queue(0)

            if tname == "master":       # don't start the master thread, we're not using it
                continue

            if restart:
                if threadModule:
                    reload(threadModule)

                for t in threading.enumerate():
                    if re.search(r"^%s(-\d+)?$" % tname, t.name): # a thread of the proper type
                        actorState.queues[tid].flush()
                        actorState.queues[tid].put(Msg(Msg.EXIT, cmd=cmd))

                        t.join(1.0)

                def updateName(g):
                    """re.sub callback to convert master -> master-1; master-3 -> master-4"""
                    try:
                        n = int(g.group(2))
                    except TypeError:
                        n = 0

                    return "%s-%d" % (g.group(1), n + 1)

                tname = re.sub(r"^([^\d]*)(?:-(\d*))?$", updateName, actorState.threads[tid].name)

            actorState.threads[tid] = threading.Thread(target=target, name=tname,
                                                       args=[actorState.actor, actorState.queues])
            actorState.threads[tid].daemon = True

        for t in actorState.threads.values():
            t.start()
#
# To work
#
if __name__ == "__main__":
    sop = Sop("sop", "sopActor", debugLevel=5)
