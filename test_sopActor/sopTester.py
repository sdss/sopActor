"""
To help testing the sop threads.
"""
import ConfigParser
import threading,Queue
import time
import sys
import unittest
import re
import os

from actorcore import TestHelper

import sopActor
from sopActor import Bypass
from sopActor.Commands import SopCmd
from sopActor.utils.tcc import TCCState
from sopActor.utils.gang import ApogeeGang
from sopActor.utils.guider import GuiderState
import sopActor.myGlobals as myGlobals

def FakeThread(actor,queues):
    """A thread that just outputs what it was told to do, for any message."""
    # how to get the actual name and ID of this thread.
    name = threading.current_thread().name
    tid = [t for t in queues if name == queues[t].name][0]
    # Let me strip out Msg members that are always there, 
    # to find useful parts of messages that are the command arguments.
    genericMsg = sopActor.Msg(None,None)
    genericMsg.senderName = ''
    genericMsg.senderName0 = ''
    genericMsg.senderQueue = None
    genericMsg.replyQueue = None
    redundant = set(dir(genericMsg))
    while True:
        try:
            msg = queues[tid].get(timeout=2.)
            if msg.type == sopActor.Msg.EXIT:
                #msg.cmd.inform('text="Exiting thread %s"'%name)
                return
            else:
                unique = set(dir(msg)) - redundant
                txt = ' '.join(['='.join((str(u),str(getattr(msg,u)))) for u in unique])
                msg.cmd.call('%s %s %s'%(name,str(msg.type),txt))
                msg.replyQueue.put(sopActor.Msg.DONE, cmd=msg.cmd, success=True)
        except Queue.Empty:
            actor.bcast.diag('text="%s alive"' % name)

def updateModel(name,model):
    """Update the named actorState model with new parameters."""
    myGlobals.actorState.models[name] = TestHelper.Model(name,model)


class SopTester(TestHelper.ActorTester):
    def setUp(self):
        """Set up things that all SOP tests need."""
        self.name = 'sop'
        # so we can call SopCmds.
        self.actor = TestHelper.FakeActor('sop','sopActor')
        super(SopTester,self).setUp()
        myGlobals.actorState = self.actorState
        actorState = myGlobals.actorState
        actorState.tccState = TCCState(actorState.models["tcc"])
        actorState.guiderState = GuiderState(actorState.models["guider"])
        actorState.apogeeGang = ApogeeGang()
        # TBD: waiting on a better FakeActor, so I can actually use these.
        #actorState.actor.commandSets['SopCmd'].initCommands()
        #actorState.actor.commandSets['SopCmd'].updateCartridge(11)
        
        actorState.timeout = 10
        actorState.aborting = False
        self._load_lamptimes()
        self._clear_bypasses()

        # so we can call sopCmds
        self.sopCmd = SopCmd.SopCmd(self.actor)
        self.sopCmd.initCommands()

    def _load_lamptimes(self):
        """Load the lamp timeouts from the conf file."""
        self.config = ConfigParser.ConfigParser()
        self.config.read('../etc/sop.cfg')
        # The below is stolen from sopActor_main.py.
        # TBD: it would be nice if we could reuse it directly somehow...
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
        # and now override them with more sensible values for testing
        sopActor.myGlobals.warmupTime[sopActor.NE_LAMP] = 1.5
        # needs to be long enough that we get just one status message.
        sopActor.myGlobals.warmupTime[sopActor.HGCD_LAMP] = 8
        # so that we don't get an occasional status message.
        sopActor.myGlobals.warmupTime[sopActor.FF_LAMP] = .5

    def _clear_bypasses(self):
        """Clear all bypasses, so they don't screw up other tests."""
        self.cmd.verbose = False
        for name in Bypass._bypassed:
            Bypass._bypassed[name] = False
        self.cmd.clear_msgs()
        self.cmd.verbose = self.verbose

    def _update_cart(self, nCart, survey):
        """Update cartridge without being verbose, and clear those messages."""
        self.cmd.verbose = False
        self.sopCmd.updateCartridge(nCart, survey)
        self.cmd.clear_msgs()
        self.cmd.verbose = self.verbose

#...

class SopThreadTester(SopTester,unittest.TestCase):
    """
    sopActor test suites should subclass this and unittest, in that order.
    """
    def __init__(self, *args, **kwargs):
        """Load up the cmd calls for this test class."""
        unittest.TestCase.__init__(self, *args, **kwargs)
        # -1 is the test function, -2 is test class, -3 (or 0) should be main
        class_name = self.id().split('.')[-2]
        self._load_cmd_calls(class_name)
        # lets us see really long list/list diffs
        self.maxDiff = None
    
    def _load_cmd_calls(self,class_name):
        """Load the cmd calls for this test class."""
        cmdFile = os.path.join('cmd_calls',class_name+'.txt')
        self.class_calls = {}
        name = ''
        # Don't like using re, but ConfigParser barfs on these sorts of files.
        # Define a group on the stuff inside the brackets.
        header = re.compile(r'\[(.*)\]')
        with open(cmdFile) as f:
            data = f.readlines()
        for line in data:
            line = line.strip()
            if line == '':
                # Blank lines either separate blocks, or new headers.
                # assume a new block follows, and clean up later if necessary.
                self.class_calls[name].append([])
                continue
            if line[0] == '#':
                # Ignore comments.
                continue
            re_match = header.match(line)
            if re_match:
                # remove empty lists due to blank line separating test function headers
                if name and self.class_calls[name] == []:
                    self.class_calls[name].remove(-1)
                name = re_match.groups(0)[0]
                # NOTE: If this happens, then we've duplicated a cmd list.
                assert name not in self.class_calls
                self.class_calls[name] = [[]]
            else:
                self.class_calls[name][-1].append(line)
    
    def setUp(self):
        """
        Set up things that all thread testers need.
        Requires that self.useThreads (list of triplets of tname,tid,target) 
        be defined first, so it can actually start the threads.
        """
        super(SopThreadTester,self).setUp()
        actorState = myGlobals.actorState
        
        test_name = self.id().split('.')[-1]
        self.test_calls = self.class_calls.get(test_name,None)

        self.pre_threads = threading.activeCount()
        actorState.threads = {}
        actorState.queues = {}
        for tname, tid, target in self.useThreads:
            actorState.queues[tid] = sopActor.Queue(tname,0)
            actorState.threads[tid] = threading.Thread(target=target, name=tname, args=[actorState.actor,actorState.queues])
            actorState.threads[tid].daemon = True
        # Can't start these until after I've built the queues.
        for t in actorState.threads:
            actorState.threads[t].start()

    def _check_cmd(self,*args,**kwargs):
        super(SopThreadTester,self)._check_cmd(*args,**kwargs)
        # if we're expecting a reply, we should get one.
        if 'reply' in kwargs:
            queue = myGlobals.actorState.queues[kwargs['reply'][0]]
            expected = kwargs['reply'][1]
            actual = queue.get(block=False)
            self.assertEqual(actual.type,expected)
        # by now, all the queues should be empty.
        self.assert_queues_empty()

    def assert_queues_empty(self):
        """Assert that all queues are empty."""
        for tid in myGlobals.actorState.queues:
            self.assert_empty(myGlobals.actorState.queues[tid])

    def assert_empty(self,queue):
        """Assert that a queue is empty, without waiting."""
        with self.assertRaises(queue.Empty):
            msg = queue.get(block=False)
            print '%s got: %s'%(queue,msg)

    def killQueues(self):
        """Stop all running threads."""
        for tid in myGlobals.actorState.queues:
            myGlobals.actorState.queues[tid].flush()
            myGlobals.actorState.queues[tid].put(sopActor.Msg(sopActor.Msg.EXIT, cmd=self.cmd))
        
    def tearDown(self):
        self.killQueues()
        sys.stderr.flush()
        sys.stdout.flush()
        # give a newline after everything's done.
        if self.verbose:
            print

# NOTE: commented out, as it doesn't actually test the thing I want it to test: 
# the failure of RO.AddCallback on ApogeeCB.listenToReads.
# class ThreadExitTester(SopThreadTester):
#     """Overload setUp to start just one thread, to check for a clean exit."""
#     def tearDown(self):
#         """Don't want to kill threads here, because that's what we're testing!"""
#         pass
#     def test_exit(self):
#         """Send thread EXIT commands, wait for them to die, check that they died cleanly."""
#         # Have to ensure that the thread actually does something.
#         for tid in myGlobals.actorState.queues:
#             msg = sopActor.Msg(sopActor.Msg.STATUS, cmd=self.cmd, replyQueue=sopActor.Queue("(replyQueue)", 0))
#             myGlobals.actorState.queues[tid].put(msg)
#         time.sleep(2)
        
#         self.killQueues()
#         i=0
#         while threading.activeCount() > self.pre_threads:
#             print 'waiting',i
#             i += 1
#             time.sleep(0.5)
#         # should have exactly one info from status, and one from exit.
#         self._check_levels(0,1+1,0,0)
