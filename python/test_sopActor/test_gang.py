"""
Test the gang connector logic in gang.py
"""
import unittest

from sopActor.utils import gang
import sopActor.myGlobals as myGlobals
from sopActor import Bypass
from opscore.actor.model import Model
from actorcore import Actor

class dummy(object):
    pass

class Cmd(object):
    def __init__(self):
        """Save the level of any messages that pass through."""
        self.messages = ''
    def _msg(self,txt,level):
        print level,txt
        self.messages += level
    def warn(self,txt):
        self._msg(txt,'w')

cmd = Cmd()

class ActorState(object):
    dispatcherSet = False
    def __init__(self):
        self.models = {}
        #import pdb
        #pdb.set_trace()
        if self.dispatcherSet:
            Model.setDispatcher(cmd)
            self.dispatcherSet = True
        self.actor = Actor.Actor('mcp')
        self.models['mcp'] = Model('mcp')
        #self.actor = dummy()
        self.actor.bcast = Cmd()

def setGang(state):
    myGlobals.actorState.models['mcp'](state)


class Test_gang(unittest.TestCase):
    def setUp(self):
        myGlobals.actorState = ActorState()
        self.apogeeGang = gang.ApogeeGang()
        
    def tearDown(self):
        pass
    
    def testAtPodium(self):
        """Test that we are at the podium, not the cartridge."""
        setGang(4)
        self.assertTrue(self.apogeeGang.atPodium())
        self.assertTrue(self.apogeeGang.atPodium(sparseOK=True))
        self.assertTrue(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())
    
    def testAtSparse(self):
        """Test that we are at the podium sparse port, not the cartridge."""
        setGang(12)
        self.assertFalse(self.apogeeGang.atPodium())
        self.assertTrue(self.apogeeGang.atPodium(sparseOK=True))
        self.assertFalse(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())
    
    def testAtDense(self):
        """Test that we are at the podium dense port, not the cartridge."""
        setGang(20)
        self.assertTrue(self.apogeeGang.atPodium())
        self.assertTrue(self.apogeeGang.atPodium(sparseOK=True))
        self.assertTrue(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())

    def testAt1m(self):
        """Test that we are at the podium 1m port, not the cartridge."""
        setGang(36)
        self.assertFalse(self.apogeeGang.atPodium())
        self.assertFalse(self.apogeeGang.atPodium(sparseOK=True))
        self.assertTrue(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())

    def testAtCart(self):
        """Test that we are at the cartridge, not the podium."""
        setGang(2)
        self.assertFalse(self.apogeeGang.atPodium())
        self.assertFalse(self.apogeeGang.atPodium(sparseOK=True))
        self.assertFalse(self.apogeeGang.atPodium(one_mOK=True))
        self.assertTrue(self.apogeeGang.atCartridge())
    
    def testDisconnected(self):
        """Test that the gang is disconnected."""
        setGang(1)
        self.assertFalse(self.apogeeGang.atPodium())
        self.assertFalse(self.apogeeGang.atPodium(sparseOK=True))
        self.assertFalse(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())


if __name__ == '__main__':
    unittest.main()
