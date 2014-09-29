"""
Hold state about running commands, e.g. 'running', 'done', 'failed', ...
Also hold keywords for those commands as we pass them around.
"""
from opscore.utility.qstr import qstr

import sopActor.myGlobals as myGlobals
import sopActor

def getDefaultArcTime(survey):
    """Get the default arc time for this survey"""
    if survey == sopActor.BOSS or survey == sopActor.MANGA:
        return 4
    else:
        return 0

def getDefaultFlatTime(survey):
    """Get the default flat time for this survey"""
    if survey == sopActor.BOSS or survey == sopActor.MANGA:
        return 30
    else:
        return 0

class CmdState(object):
    validStageStates = ('starting', 'prepping', 'running', 'done', 'failed', 'aborted')
    """
    A class that's intended to hold command state data.
    
    Specify the various sub-stages of the command to allow stage states to be
    output for each of those substages.
    
    When creating a new CmdState subclass, specify keywords with their default values
    for uncomplicated things (e.g. exposure time), and set class variables and define
    getUserKeys to output more complicated things (e.g. nExposures done vs. requested).
    Be careful that your getUserKeys names don't clobber other keywords.
    """

    def __init__(self, name, allStages, keywords={}, hiddenKeywords=()):
        """
        Specify keywords with their default values: these will both be output automatically
        when the state changes.
        """
        self.name = name
        self.cmd = None
        self.cmdState = "idle"
        self.stateText="OK"
        self.keywords = dict(keywords)
        self.hiddenKeywords = hiddenKeywords
        self.reset_keywords()
        self.reset_nonkeywords()
        self.allStages = allStages

        self.setStages(allStages)

    def reset_keywords(self):
        """Reset all the keywords to their default values."""
        for k, v in self.keywords.iteritems():
            setattr(self, k, v)
    
    def reset_nonkeywords(self):
        """Reset all non-keyword values to their defaults."""
        pass
    
    def set(self, name, value):
        """Sets self.name == value. if Value is None, use the default value."""
        assert name in self.keywords, qstr("%s is not in keyword list: %s"%(name,str(self.keywords)))
        if value is not None:
            setattr(self,name,value)
        else:
            setattr(self,name,self.keywords[name])
    
    def _getCmd(self, cmd=None):
        if cmd:
            return cmd
        if self.cmd:
            return self.cmd
        return myGlobals.actorState.actor.bcast
    
    def setStages(self, activeStages):
        """Set the list of stages that are applicable, set their state to idle."""
        if activeStages:
            self.activeStages = activeStages
        else:
            self.activeStages = self.allStages
        self.stages = dict(zip(self.activeStages, ["idle"] * len(self.activeStages)))
    
    def reinitialize(self,cmd=None,stages=None,output=True):
        """Re-initialize this cmdState, keeping the stages list as is."""
        self.stateText="OK"
        self.reset_keywords()
        self.reset_nonkeywords()
        if cmd is not None:
            self.cmd = cmd
        if stages is not None:
            self.setStages(stages)
        else:
            self.setStages(self.activeStages)
        if output:
            self.genCommandKeys()

    def setupCommand(self, cmd, activeStages=[], name=''):
        """
        Setup the command for use, clearing stageStates, assigning new stages,
        and outputting the currently-valid commandkeys and states.
        """
        if name:
            self.name = name
        self.cmd = cmd
        self.stateText="OK"
        self.setStages(activeStages)
        #for s in self.activeStages:
        #    self.stages[s] = "pending" if s in activeStages else "off"
        self.genCommandKeys()

    def setCommandState(self, state, genKeys=True, stateText=None):
        self.cmdState = state
        if stateText:
            self.stateText=stateText

        if genKeys:
            self.genKeys()

    def setStageState(self, name, stageState, genKeys=True):
        """Set a stage to a new state, and output the stage state keys."""
        assert name in self.stages, "stage %s is unknown, out of %s"%(name,repr(self.stages))
        assert stageState in self.validStageStates, "state %s is unknown, out of %s" % (stageState,repr(self.validStageStates))
        self.stages[name] = stageState

        if genKeys:
            self.genCmdStateKeys()

    def abortStages(self):
        """ Mark all unstarted stages as aborted. """
        for s in self.activeStages:
            if not self.stages[s] in ("pending", "done", "failed"):
                self.stages[s] = "aborted"
        self.genCmdStateKeys()

    def genCmdStateKeys(self, cmd=None):
        cmd = self._getCmd(cmd)
        cmd.inform("%sState=%s,%s,%s" % (self.name, qstr(self.cmdState),
                                         qstr(self.stateText),
                                         ",".join([qstr(self.stages[sname]) \
                                                       for sname in self.activeStages])))

    def genCommandKeys(self, cmd=None):
        """ Return a list of the keywords describing our command. """

        cmd = self._getCmd(cmd)
        cmd.inform("%sStages=%s" % (self.name,
                                    ",".join([qstr(sname) \
                                                  for sname in self.activeStages])))
        self.genCmdStateKeys(cmd=cmd)

    def getUserKeys(self):
        return []
    
    def genStateKeys(self, cmd=None):
        cmd = self._getCmd(cmd)

        msg = []
        for keyName, default in self.keywords.iteritems():
            val = getattr(self, keyName)
            if type(default) == str:
                val = qstr(val)
                default = qstr(default)
            msg.append("%s_%s=%s,%s" % (self.name, keyName,
                                        val, default))
        if msg:
            cmd.inform("; ".join(msg))

        try:
            userKeys = self.getUserKeys()
        except:
            userKeys = []
            cmd.warn('text="failed to fetch all keywords for %s"' % (self.name))

        if userKeys:
            cmd.inform(";".join(userKeys))
        
    def genKeys(self, cmd=None, trimKeys=False):
        """ generate all our keywords. """
        if not trimKeys or trimKeys == self.name:
            self.genCommandKeys(cmd=cmd)
            self.genStateKeys(cmd=cmd)
        
    def isSlewingDisabled_BOSS(self):
        """Return True if the BOSS state is safe to start a slew."""
        safe_state = ('READING', 'IDLE', 'DONE', 'ABORTED')
        boss_state = myGlobals.actorState.models["boss"].keyVarDict["exposureState"][0]
        text = "; exposureState=%s"%boss_state
        if boss_state not in safe_state:
            return True, text
        else:
            return False, text

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

# Now define the actual command states we'll be using:

class GotoGangChangeCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'gotoGangChange',
                          ['domeFlat', 'slew'],
                          keywords=dict(alt=45.0))
        self.expType = "object"

class DoApogeeDomeFlatCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doApogeeDomeFlat',
                          ['domeFlat'],
                          keywords=dict(expTime=50.0))
        self.expType = "object"


class HartmannCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'hartmann',
                          ['left','right','cleanup'],
                          keywords=dict(expTime=4))

class GotoFieldCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'gotoField',
                          ['slew', 'hartmann', 'calibs', 'guider', 'cleanup'],
                          keywords=dict(arcTime=4,
                                        flatTime=30,
                                        guiderTime=5.0,
                                        guiderFlatTime=0.5))
        
    def reset_nonkeywords(self):
        self.fakeAz = None
        self.fakeAlt = None
        self.fakeRotOffset = 0.0
        self.ra = 0
        self.dec = 0
        self.rotang = 0
        self.keepOffsets = None
        self.doSlew = True
        self.doHartmann = True
        self.doCalibs = True
        self.didArc = False
        self.didFlat = False
        self.doGuiderFlat = True
        self.doGuider = True
    
class DoBossCalibsCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doBossCalibs',
                          ['bias', 'dark', 'flat', 'arc', 'cleanup'],
                          keywords=dict(darkTime=900.0,
                                        flatTime=30.0,
                                        arcTime=4.0,
                                        guiderFlatTime=0.5))
    
    def reset_nonkeywords(self):
        self.nBias = 0; self.nBiasDone = 0;
        self.nDark = 0; self.nDarkDone = 0;
        self.nFlat = 0; self.nFlatDone = 0;
        self.nArc = 0; self.nArcDone = 0;
    
    def cals_remain(self):
        return self.nBiasDone < self.nBias or self.nDarkDone < self.nDark or \
                self.nFlatDone < self.nFlat or self.nArcDone < self.nArc

    
    def getUserKeys(self):
        msg = []
        msg.append("nBias=%d,%d" % (self.nBiasDone, self.nBias))
        msg.append("nDark=%d,%d" % (self.nDarkDone, self.nDark))
        msg.append("nFlat=%d,%d" % (self.nFlatDone, self.nFlat))
        msg.append("nArc=%d,%d" % (self.nArcDone, self.nArc))
        return ["%s_%s" % (self.name, m) for m in msg]

class DoApogeeScienceCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doApogeeScience',
                          ['expose'],
                          keywords=dict(ditherSeq="ABBA",
                                        expTime=500.0,
                                        comment="",
                                        seqCount=2))
        self.seqDone = 0
        self.exposureSeq = self.ditherSeq*self.seqCount
        self.index = 0
        self.expType = "object"

    def getUserKeys(self):
        msg = []
        msg.append('%s_sequenceState="%s",%d' % (self.name,
                                                 self.exposureSeq,
                                                 self.index))
        return msg

    def isSlewingDisabled(self):
        """If slewing is disabled, return a string describing why, else False."""
        if self.cmd and self.cmd.isAlive():
            return 'slewing disallowed for APOGEE, blocked by active doApogeeScience sequence'
        else:
            return False

class DoApogeeSkyFlatsCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doApogeeSkyFlats',
                          ['expose'],
                          keywords=dict(ditherSeq="ABBA",
                                        expTime=150.0))
        self.seqCount = 0
        self.seqDone = 0
        self.exposureSeq = "ABBA"
        self.index = 0
        self.expType = "object"

class DoBossScienceCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doBossScience',
                          ['expose'],
                          keywords=dict(expTime=900.0))
        self.nExp = 0
        self.nExpDone = 0
        self.nExpLeft = 0

    def getUserKeys(self):
        msg = []
        msg.append("%s_nExp=%d,%d" % (self.name, self.nExpDone, self.nExp))
        return msg

    def isSlewingDisabled(self):
        """If slewing is disabled, return a string describing why, else False."""
        exp_state,exp_text = self.isSlewingDisabled_BOSS()
        text = "slewing disallowed for BOSS, with %d science exposures left%s" % (self.nExpLeft,exp_text)
        if (self.cmd and self.cmd.isAlive() and (exp_state or self.nExpLeft > 1)):
            return text
        else:
            return False

class DoMangaSequenceCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doMangaSequence',
                          ['expose','calibs','dither'],
                          keywords=dict(expTime=900.0,
                                        dithers='NSE',
                                        count=3))
        self.reset_ditherSeq()
        
    def reset_nonkeywords(self):
        self.dithersDone = 0
        self.nSet = 0
        self.index = 0

    def set_mangaDither(self):
        """Setup to use this for MaNGA dither observations."""
        self.keywords=dict(expTime=900.0,
                           dithers='NSE',
                           count=3)
        if not (self.cmd and self.cmd.isAlive()):
            self.reset_ditherSeq()

    def set_mangaStare(self):
        """Setup to use this for MaNGA Stare observations."""
        self.keywords=dict(expTime=900.0,
                           dithers='CCC',
                           count=1)
        if not (self.cmd and self.cmd.isAlive()):
            self.reset_ditherSeq()

    def reset_ditherSeq(self):
        """Reset dither sequence based on dithers and count."""
        self.ditherSeq = self.dithers*self.count
        
    def getUserKeys(self):
        msg = []
        msg.append("%s_ditherSeq=%s,%s,%s" % (self.name, self.ditherSeq, self.dithersDone, self.index))
        return msg
    
    def isSlewingDisabled(self):
        if (self.cmd and self.cmd.isAlive()):
            return "slewing disallowed for MaNGA, with a sequence in progress."
        else:
            return False

class DoMangaDitherCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doMangaDither',
                          ['expose','dither'],
                          keywords=dict(expTime=900.0,
                                        dither='C'))

    def reset_nonkeywords(self):
        self.readout = True

    def isSlewingDisabled(self):
        """If slewing is disabled, return a string describing why, else False."""
        exp_state,exp_text = self.isSlewingDisabled_BOSS()
        if (self.cmd and self.cmd.isAlive() and exp_state):
            return "slewing disallowed for MaNGA, with 1 science exposures left%s"%exp_text
        else:
            return False

class DoApogeeMangaDitherCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doApogeeMangaDither',
                          ['expose','dither'],
                          keywords=dict(mangaExpTime=900.0,
                                        apogeeExpTime=450.0,
                                        mangaDither='C',
                                        comment=''))

    def reset_nonkeywords(self):
        self.readout = True

    def set_apogeeLead(self):
        """Setup to use this for APOGEE lead observations."""
        self.keywords=dict(mangaExpTime=900.0,
                           apogeeExpTime=500.0,
                           mangaDither='C',
                           comment='')

    def set_manga(self):
        """Setup to use this for MaNGA (stare or dither) observations."""
        self.keywords=dict(mangaExpTime=900.0,
                           apogeeExpTime=450.0,
                           mangaDither='C',
                           comment='')

    def isSlewingDisabled(self):
        """If slewing is disabled, return a string describing why, else False."""
        exp_state,exp_text = self.isSlewingDisabled_BOSS()
        if (self.cmd and self.cmd.isAlive() and exp_state):
            return "slewing disallowed for APOGEE&MaNGA, with 1 science exposures left%s"%exp_text
        else:
            return False

class DoApogeeMangaSequenceCmd(CmdState):
    def __init__(self):
        CmdState.__init__(self, 'doApogeeMangaSequence',
                          ['expose','calibs','dither'],
                          keywords=dict(mangaExpTime=900.0,
                                        apogeeExpTime=450.0,
                                        mangaDithers='NSE',
                                        count=2,
                                        comment=''))
        self.reset_ditherSeq()
    
    def set_apogeeLead(self):
        """Setup to use this for APOGEE lead observations."""
        self.keywords=dict(mangaExpTime=900.0,
                           apogeeExpTime=500.0,
                           mangaDithers='CC',
                           count=2,
                           comment='')
        self.readout = True
        if not (self.cmd and self.cmd.isAlive()):
            self.reset_ditherSeq()

    def set_mangaDither(self):
        """Setup to use this for MaNGA dither observations."""
        self.keywords=dict(mangaExpTime=900.0,
                           apogeeExpTime=450.0,
                           mangaDithers='NSE',
                           count=2,
                           comment='')
        self.readout = False
        if not (self.cmd and self.cmd.isAlive()):
            self.reset_ditherSeq()

    def set_mangaStare(self):
        """Setup to use this for MaNGA stare observations."""
        self.keywords=dict(mangaExpTime=900.0,
                           apogeeExpTime=450.0,
                           mangaDithers='CCC',
                           count=2,
                           comment='')
        self.readout = False
        if not (self.cmd and self.cmd.isAlive()):
            self.reset_ditherSeq()

    def reset_nonkeywords(self):
        self.nSet = 0
        self.index = 0
        self.reset_ditherSeq()
    
    def reset_ditherSeq(self):
        """Reset dither sequence based on dithers,count parameters."""
        self.mangaDitherSeq = self.mangaDithers*self.count
        # Note: Two APOGEE exposures are taken for each MaNGA exposure.
        
    def getUserKeys(self):
        msg = []
        msg.append("%s_ditherSeq=%s,%s" % (self.name, self.mangaDitherSeq, self.index))
        return msg
    
    def isSlewingDisabled(self):
        exp_state,exp_text = self.isSlewingDisabled_BOSS()
        if (self.cmd and self.cmd.isAlive() and exp_state):
            return "slewing disallowed for APOGEE&MaNGA, with a sequence in progress."
        else:
            return False
