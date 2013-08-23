"""
Test the Hartmann collimation routine converted from idlspec2d combsmallcollimate.
"""
import unittest

from sopActor.utils import boss_collimate

class Cmd(object):
    def __init__(self):
        """Save the level of any messages that pass through."""
        self.messages = ''
    def _msg(self,txt,level):
        print level,txt
        self.messages += level
    def inform(self,txt):
        self._msg(txt,'i')
    def diag(self,txt):
        self._msg(txt,'d')
    def warn(self,txt):
        self._msg(txt,'w')
    def fail(self,txt):
        self._msg(txt,'f')
    def error(self,txt):
        self._msg(txt,'e')

def get_expnum(filename):
    return int(filename.split('.fit')[0].split('-')[-1])

def get_mjd(filename):
    return int(filename.split('/')[2])

class Test_boss_collimate(unittest.TestCase):
    def setUp(self):
        self.cmd = Cmd()
        self.hart = boss_collimate.Hartmann()
        self.ffsOpen = ''
        self.ffsSomeClosed = ''
        self.notHartmann = '/data/spectro/56492/sdR-r2-00165003.fit.gz'
        self.NeOff = ''
        self.HgCdOff = ''
        self.focused1 = '/data/spectro/56492/sdR-r2-00165006.fit.gz'
        self.focused2 = '/data/spectro/56492/sdR-r2-00165007.fit.gz'
        self.notFocused1 = ''
        self.notFocused2 = ''
        
        self.inFocusMsg = 'i'*2*4+'ii'*2
        self.OutFocusMsg = 'wi'+'i'*2*3+'ii'*2
        self.BadHeaderMsg = 'wwef'*2
        
    def tearDown(self):
        # delete plot files, if they were created.
        pass
    
    def testNotHartmann(self):
        """Test with a file that isn't a Hartmann image."""
        exp1 = get_expnum(self.notHartmann)
        self.hart.collimate(self.cmd,exp1)
        self.assertEqual(self.cmd.messages,self.badHeaderMsg)
        
    @unittest.skip('need test files!')
    def testNoNe(self):
        """Test with a file that had all Ne lamps off."""
        exp1 = get_expnum(self.NeOff_file)
        self.hart.collimate(self.cmd,exp1)
        self.assertEqual(self.cmd.messages,self.badHeaderMsg)
    
    @unittest.skip('need test files!')
    def testNoFFS(self):
        """Test with a file that had all FFS open."""
        exp1 = get_expnum(self.ffsOpen_file)
        self.hart.collimate(self.cmd,exp1)
        self.assertEqual(self.cmd.messages,'wef'*2)
        pass
    
    @unittest.skip('need test files!')
    def testNoLight(self):
        """Test with a file that has no signal."""
        exp1 = get_expnum(self.noLight_file)
        self.hart.collimate(self.cmd,exp1)
        self.assertEqual(self.cmd.messages,self.noLightMsg)
        pass
    
    @unittest.skip('need test files!')
    def testHartmannOneFile(self):
        """Test collimating left expnum, the subsequent expnum is right."""
        exp1 = get_expnum(self.notFocused1)
        self.hart.collimate(self.cmd,exp1,plot=True)
        self.assertEqual(self.cmd.messages,self.OutFocusMsg)
        # TBD: get correct result from combsmallcollimate.pro        
    
    def testHartmannTwoFiles(self):
        """Test collimating with files in correct order (left->right)."""
        exp1 = get_expnum(self.focused1)
        exp2 = get_expnum(self.focused2)
        self.hart.collimate(self.cmd,exp1,exp2,plot=True)
        self.assertEqual(self.cmd.messages,self.inFocusMsg)
        # TBD: get correct result from combsmallcollimate.pro
        
    def testReverseOrder(self):
        """Test collimating with files in reversed order (right->left)."""
        exp1 = get_expnum(self.focused1)
        exp2 = get_expnum(self.focused2)
        self.hart.collimate(self.cmd,exp2,exp1,plot=True) # note reversed order!
        self.assertEqual(self.cmd.messages,self.inFocusMsg)
        # TBD: get correct result from combsmallcollimate.pro
        
if __name__ == '__main__':
    unittest.main()
