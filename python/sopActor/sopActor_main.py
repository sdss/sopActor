#!/usr/bin/env python
"""An actor to run SOP."""

import inspect, os, re, sys
import Queue, threading

import opscore.actor.model
import opscore.actor.keyvar

import actorcore
import actorcore.Actor
import actorcore.CmdrConnection

import actorkeys

import masterThread
import bossThread
import apogeeThread
import guiderThread
import gcameraThread
import ffsThread
import lampThreads
import tccThread
import scriptThread
import slewThread

from sopActor import *
from bypass import Bypass
import sopActor.myGlobals
#
# Import sdss3logging before logging if you want to use it
#
if True:
    import opscore.utility.sdss3logging as sdss3logging
import logging

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class State(object):
    """An object to hold globally useful state"""

    def __init__(self, actor):
        self.actor = actor
        self.dispatcher = self.actor.cmdr.dispatcher
        self.models = {}
        self.restartCmd = None
        self.aborting = False
        self.ignoreAborting = False

    def __str__(self):
        msg = "%s %s" % (self.actor, self.actor.cmdr.dispatcher)

        return msg


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class Sop(actorcore.Actor.Actor):
    def __init__(self, name, configFile, debugLevel=30):
        self.headURL = "$HeadURL$"
        actorcore.Actor.Actor.__init__(self, name, configFile)

        self.logger.setLevel(debugLevel)
        #self.logger.propagate = True

        sopActor.myGlobals.bypass = Bypass()
        
        sopActor.myGlobals.actorState = State(self)
        actorState = sopActor.myGlobals.actorState
        #
        # Load other actor's models so we can send them commands
        #
        for actor in ["boss", "guider", "platedb", "mcp", "sop", "tcc", "apogee"]:
            actorState.models[actor] = opscore.actor.model.Model(actor)

        from sopActor.utils.guider import GuiderState
        from sopActor.utils.gang import ApogeeGang

        actorState.guiderState = GuiderState(actorState.models["guider"])
        actorState.apogeeGang = ApogeeGang()
        
        actorState.actor.commandSets["SopCmd"].initCommands()
        
        #
        # spawn off the threads that sequence actions (e.g. take an exposure; move telescope)
        # and talk to the gcamera
        # 
        actorState.timeout = 60         # timeout on message queues

        Sop.startThreads(actorState, restartQueues=True)
        #
        # Handle the hated ini file
        #
        # read the warmupTimes and convert e.g. "Ne" to sopActor.NE_LAMP
        warmupList = self.config.get('lamps', "warmupTime").split()
        sopActor.myGlobals.warmupTime = {}
        for i in range(0, len(warmupList), 2):
            k, v = warmupList[i:i+2]
            sopActor.myGlobals.warmupTime[{"ff" : sopActor.FF_LAMP,
                                           "hgcd" : sopActor.HGCD_LAMP,
                                           "ne" : sopActor.NE_LAMP,
                                           "wht" : sopActor.WHT_LAMP,
                                           "uv" : sopActor.UV_LAMP
                                       }[k.lower()]] = float(v)

        
        #
        # Finally start the reactor
        #
        self.run()

    def periodicStatus(self):
        pass
    
    @staticmethod
    def startThreads(actorState, cmd=None, restartQueues=False, restart=False, restartThreads=None):
        """Start or restart the worker threads and queues; restartThreads is a list of names to restart"""

        try:
            actorState.threads
        except AttributeError:
            restart = False
        
        if not restart:
            actorState.queues = {}
            actorState.threads = {}

            restartQueues = True

        newQueues = {}
        threadsToStart = []
        for tname, tid, threadModule, target in [("master",  sopActor.MASTER,    masterThread,  masterThread.main),
                                                 ("boss",    sopActor.BOSS,      bossThread,    bossThread.main),
                                                 ("apogee",  sopActor.APOGEE,    apogeeThread,  apogeeThread.main),
                                                 ("apogeeScript",  sopActor.APOGEE_SCRIPT, apogeeThread,  apogeeThread.script_main),
                                                 ("script",  sopActor.SCRIPT,    scriptThread,  scriptThread.main),
                                                 ("guider",  sopActor.GUIDER,    guiderThread,  guiderThread.main),
                                                 ("gcamera", sopActor.GCAMERA,   gcameraThread, gcameraThread.main),
                                                 ("ff",      sopActor.FF_LAMP,   lampThreads,   lampThreads.ff_main),
                                                 ("hgcd",    sopActor.HGCD_LAMP, None,          lampThreads.hgcd_main),
                                                 ("ne",      sopActor.NE_LAMP,   None,          lampThreads.ne_main),
                                                 ("uv",      sopActor.UV_LAMP,   None,          lampThreads.uv_main),
                                                 ("wht",     sopActor.WHT_LAMP,  None,          lampThreads.wht_main),
                                                 ("ffs",     sopActor.FFS,       ffsThread,     ffsThread.main),
                                                 ("tcc",     sopActor.TCC,       tccThread,     tccThread.main),
                                                 ('slew',    sopActor.SLEW,      slewThread,    slewThread.main)
                                                 ]:
            if restartThreads and tname not in restartThreads:
                continue

            newQueues[tid] = sopActor.Queue(tname, 0) if restartQueues else actorState.queues[tid]

            if restart:
                if threadModule:
                    reload(threadModule)

                for t in threading.enumerate():
                    if re.search(r"^%s(-\d+)?$" % tname, t.name): # a thread of the proper type
                        actorState.queues[tid].flush()
                        actorState.queues[tid].put(Msg(Msg.EXIT, cmd=cmd))

                        t.join(1.0)
                        if t.isAlive():
                            if cmd:
                                cmd.inform('text="Failed to kill %s"' % tname)

                def updateName(g):
                    """re.sub callback to convert master -> master-1; master-3 -> master-4"""
                    try:
                        n = int(g.group(2))
                    except TypeError:
                        n = 0

                    return "%s-%d" % (g.group(1), n + 1)

                tname = re.sub(r"^([^\d]*)(?:-(\d*))?$", updateName, actorState.threads[tid].name)

            if cmd:
                cmd.inform('text="Starting %s"' % tname)

            actorState.queues[tid] = sopActor.Queue(tname, 0) # remove any unprocessed Msg.EXITs
                
            actorState.threads[tid] = threading.Thread(target=target, name=tname,
                                                       args=[actorState.actor, newQueues])
            actorState.threads[tid].daemon = True

            threadsToStart.append(actorState.threads[tid])
        #
        # Switch to the new queues now we've sent EXIT to the old ones
        #
        for tid, q in newQueues.items():
            actorState.queues[tid] = q

        for t in threadsToStart:
            t.start()
#
# To work
#
if __name__ == "__main__":
    sop = Sop("sop", "sopActor", debugLevel=5)
