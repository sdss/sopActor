"""
Test the gang connector logic in gang.py
"""
import unittest

import sopTester

from sopActor.utils import gang
import sopActor.myGlobals as myGlobals

def setGang(state):
    myGlobals.actorState.models['mcp'].keyVarDict['apogeeGang'].set([str(state),])

class Test_gang(sopTester.SopTester,unittest.TestCase):
    def setUp(self):
        self.verbose = False
        super(Test_gang,self).setUp()
        self.apogeeGang = gang.ApogeeGang()

    def test_at_podium(self):
        """Test that we are at the podium, not the cartridge."""
        setGang(4)
        self.assertTrue(self.apogeeGang.atPodium())
        self.assertTrue(self.apogeeGang.atPodium(sparseOK=True))
        self.assertTrue(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())
    
    def test_at_sparse(self):
        """Test that we are at the podium sparse port, not the cartridge."""
        setGang(20)
        self.assertFalse(self.apogeeGang.atPodium())
        self.assertTrue(self.apogeeGang.atPodium(sparseOK=True))
        self.assertFalse(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())
    
    def test_at_dense(self):
        """Test that we are at the podium dense port, not the cartridge."""
        setGang(12)
        self.assertTrue(self.apogeeGang.atPodium())
        self.assertTrue(self.apogeeGang.atPodium(sparseOK=True))
        self.assertTrue(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())

    def test_at_1m(self):
        """Test that we are at the podium 1m port, not the cartridge."""
        setGang(36)
        self.assertFalse(self.apogeeGang.atPodium())
        self.assertFalse(self.apogeeGang.atPodium(sparseOK=True))
        self.assertTrue(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())

    def test_at_cart(self):
        """Test that we are at the cartridge, not the podium."""
        setGang(2)
        self.assertFalse(self.apogeeGang.atPodium())
        self.assertFalse(self.apogeeGang.atPodium(sparseOK=True))
        self.assertFalse(self.apogeeGang.atPodium(one_mOK=True))
        self.assertTrue(self.apogeeGang.atCartridge())
    
    def test_disconnected(self):
        """Test that the gang is disconnected."""
        setGang(1)
        self.assertFalse(self.apogeeGang.atPodium())
        self.assertFalse(self.apogeeGang.atPodium(sparseOK=True))
        self.assertFalse(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())

    def test_at_podium_bypassed(self):
        """gang at podium, but that location is bypassed."""
        setGang(4)
        myGlobals.bypass.set('gangToCart',True)
        self.assertFalse(self.apogeeGang.atPodium())
        self.assertFalse(self.apogeeGang.atPodium(sparseOK=True))
        self.assertFalse(self.apogeeGang.atPodium(one_mOK=True))
        self.assertTrue(self.apogeeGang.atCartridge())

    def test_at_cart_bypassed(self):
        """gang at cart, but that location is bypassed."""
        setGang(2)
        myGlobals.bypass.set('gangToPodium',True)
        self.assertTrue(self.apogeeGang.atPodium())
        self.assertTrue(self.apogeeGang.atPodium(sparseOK=True))
        self.assertTrue(self.apogeeGang.atPodium(one_mOK=True))
        self.assertFalse(self.apogeeGang.atCartridge())

if __name__ == '__main__':
    unittest.main()
