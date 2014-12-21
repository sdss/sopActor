import sopActor.myGlobals as myGlobals

import logging
import re

class TCCState(object):
    """Listen to TCC keywords to analyse when the TCC's done a move or halt that might invalidate guiding"""

    instName = None

    goToNewField = False
    halted = False
    moved = False
    slewing = False
    badStat = 0

    axisBadStatusMask = 0

    def __init__(self, tccModel):
        """Register keywords that we need to pay attention to"""

        #NEWTCC: tccStatus is deprecated in the new TCC.
        tccModel.keyVarDict["axisBadStatusMask"].addCallback(self.listenToBadStatusMask, callNow=True)

        tccModel.keyVarDict["moveItems"].addCallback(self.listenToMoveItems, callNow=False)
        tccModel.keyVarDict["inst"].addCallback(self.listenToInst, callNow=False)
        tccModel.keyVarDict["slewEnd"].addCallback(self.listenToSlewEnd, callNow=False)
        tccModel.keyVarDict["tccStatus"].addCallback(self.listenToTccStatus, callNow=False)

        tccModel.keyVarDict["azStat"].addCallback(self.listenToAxisStat, callNow=False)
        tccModel.keyVarDict["altStat"].addCallback(self.listenToAxisStat, callNow=False)
        tccModel.keyVarDict["rotStat"].addCallback(self.listenToAxisStat, callNow=False)

    @staticmethod
    def axisMask(axisName):
        d = dict(AZ=1<<0,
                 ALT=1<<1,
                 ROT=1<<2)

        return d[axisName.upper()]
	
    @staticmethod
    def resetAxisStat():
        TCCState.badStat = 0
	
    @staticmethod
    def listenToInst(keyVar):
        inst = keyVar[0]
        if inst != TCCState.instName:
            TCCState.instName = inst

    @staticmethod
    def listenToBadStatusMask(keyVar):
        mask = keyVar[0]
        TCCState.axisBadStatusMask = mask

        logging.info('set axisBadStatusMask to %08x' % (mask))

    @staticmethod
    def listenToMoveItems(keyVar):
        """ Figure out if the telescope has been moved by examining the TCC's MoveItems key.

        The MoveItems keys always comes with one of:
          - Moved, indicating an immediate offset
              We guestimate the end time, and let the main loop wait on it.
          - SlewBeg, indicating the start of a real slew or a computed offset.
          - SlewEnd, indicating the end of a real slew or a computed offset
        """

        # TBD: NOTE: This method seems excessive. Can't we just check SlewBeg itself?

        #
        # Ughh.  Look for those keys somewhere else on the same reply; Craig swears that this is safe...
        #
        key = [k for k in keyVar.reply.keywords if re.search(r"^(moved|slewBeg|slewEnd)$", k.name, re.IGNORECASE)]
        if not key:
            print "Craig lied to me"
            return
        key = key[0]
        #
        # Parse moveItems
        #
        mi = keyVar[0]

        if not mi:
            mi = "XXXXXXXXX"

        newField = False
        if mi[1] == 'Y':
            what = 'Telescope has been slewed'
            newField = True
        elif mi[3] == 'Y':
            what = 'Object offset was changed'
        elif mi[4] == 'Y':
            what = 'Arc offset was changed'
        elif mi[5] == 'Y':
            what = 'Boresight position was changed'
        elif mi[6] == 'Y':
            what = 'Rotation was changed'
        elif mi[8] == 'Y':
            what = 'Calibration offset was changed'
        else:
            # Ignoring:
            #    mi[0] - Object name
            #    mi[2] - Object magnitude
            #    mi[7] - Guide offset
            return
        #
        # respond to what's been ordered
        #
        if key.name == "Moved":         # Has an uncomputed offset just been issued?
            # telescope has started moving, and may still be moving
            TCCState.goToNewField = False
            TCCState.halted = False
            TCCState.moved = True
            TCCState.slewing = False

        elif key.name == "SlewBeg":     # Has a slew/computed offset just been issued? 
            TCCState.goToNewField = newField
            TCCState.halted = False
            TCCState.moved = False
            TCCState.slewing = True

        elif key.name == "SlewEnd":     # A computed offset finished
            listenToSlewEnd(None)
        else:
            print "Impossible key:", key.name

        print "TCCState.slewing", TCCState.slewing

    @staticmethod
    def listenToSlewEnd(keyVar):
        """Listen for SlewEnd commands"""

        TCCState.goToNewField = False
        TCCState.halted = False
        TCCState.moved = False
        TCCState.slewing = False

    @staticmethod
    def listenToTccStatus(keyVar):
        """ Figure out if the telescope has been halted by examining the TCC's TCCStatus key.
        """

        axisStat = keyVar.valueList[0]

        if axisStat is not None and 'H' in axisStat:
            TCCState.goToNewField = False
            TCCState.halted = True

    @staticmethod
    def listenToAxisStat(keyVar):
        """ Figure out if the axes are active by examining the TCC's <Axis>Stat keys.
        """

        axisStat = keyVar.valueList[3]

        if axisStat is not None:
            axisMask = TCCState.axisMask(keyVar.name[:-4])
            # NOTE: this may cause an exception when starting sopActor, as
            # axisBadStatusMask may not be populated yet.
            if not (axisStat & TCCState.axisBadStatusMask): # or Bypass.get(name="axes"):
                logging.info("clearing some axis failure (%s: %08x)" % (keyVar.name, axisStat))
                TCCState.badStat &= ~axisMask
            else:
                logging.info("noting some axis failure (%s: %08x)" % (keyVar.name, axisStat))
                TCCState.badStat |= axisMask
                TCCState.goToNewField = False
                TCCState.halted = True
                TCCState.slewing = False

    @staticmethod
    def obs2Sky(cmd, az=None, alt=None, rotOffset=0.0):
        """ return ra, dec, rot for the current telescope position. """


        tccDict = myGlobals.actorState.models['tcc'].keyVarDict
        axePos = tccDict['axePos']
        gotoAz = az if az != None else axePos[0]
        gotoAlt = alt if alt != None else axePos[1]

        cmd.warn('text="FAKING slew position from az, alt, and rotator offset: %0.1f %0.1f %0.1f"'
                 % (gotoAz, gotoAlt, rotOffset))
        cmdVar = myGlobals.actorState.actor.cmdr.call(actor="tcc", forUserCmd=cmd,
                                                      cmdStr=("convert %0.5f,%0.6f obs icrs" %
                                                              (gotoAz, gotoAlt)))
        if cmdVar.didFail:
            return 0,0,0
        else:
            convPos = tccDict['convPos']
            convAng = tccDict['convAng']

            rotPos = 180.0-convAng[0].getPos()
            rotPos += rotOffset
                
            # I think I need to do _something with the axePos rotation angle.
            return convPos[0].getPos(), convPos[1].getPos(), rotPos
        

