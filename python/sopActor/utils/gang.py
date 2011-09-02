import sopActor.myGlobals as myGlobals
from sopActor import Bypass

class ApogeeGang(object):
    """ Encapsulate the APOGEE gang connector. """

    GANG_UNKNOWN      = "unknown gang position"
    GANG_ON_CARTRIDGE = "gang on cartridge"
    GANG_ON_PODIUM    = "gang on podium"
    GANG_AT_SPARSE    = "gang at sparse cals"
    GANG_DISCONNECTED = "gang disconnected"
    
    def __init__(self):
        pass

    def getPhysicalPos(self):
        """ Return the gang position as indicated by the MCP apogeeGang keyword

        The mcp puts out an Int described as an Enum, which we map.
        We no longer need to do this....
        """

        gangMap = {0:self.GANG_DISCONNECTED,
                   1:self.GANG_ON_PODIUM,
                   2:self.GANG_ON_CARTRIDGE,
                   3:self.GANG_AT_SPARSE}
        
        mcpModel = myGlobals.actorState.models["mcp"]
        gangPos = mcpModel.keyVarDict["apogeeGang"]
        if not gangPos.isCurrent:
            return self.GANG_UNKNOWN

        return gangMap[int(gangPos[0])]
    
    def getPhysicalPosByBits(self):
        """ Return the gang position as indicated by the physical switches.

        For now there is no direct MCP support, so we look at the Allen-Bradley ab_I0_L0 bits directly:
          disconnected: 0x0020181b
          cartridge   : 0x0020081b
          podium      : 0x0020101b
          sparse      : 0x0020001b
        """

        mcpModel = myGlobals.actorState.models["mcp"]
        if not mcpModel.keyVarDict["ab_I1_L0"].isCurrent:
            return self.GANG_UNKNOWN
        
        CART_MASK   = 0x00000800
        PODIUM_MASK = 0x00001000
        GANG_MASK   = CART_MASK | PODIUM_MASK
        gangBits = long(mcpModel.keyVarDict["ab_I1_L0"][0]) & GANG_MASK

        bcast = myGlobals.actorState.actor.bcast
        bcast.warn('text="gangBits=%08x"' % (gangBits))
        
        if gangBits & CART_MASK and gangBits & PODIUM_MASK:
            return self.GANG_AT_SPARSE
        if gangBits & CART_MASK:
            return self.GANG_ON_CARTRIDGE
        if gangBits & PODIUM_MASK:
            return self.GANG_ON_PODIUM
        return self.GANG_UNKNOWN
        
    def getPos(self):
        bcast = myGlobals.actorState.actor.bcast

        if Bypass.get(name='gangCart'):
            bcast.warn('text="Lying about the APOGEE gang connector being on the podium"')
            return self.GANG_ON_PODIUM
        elif Bypass.get(name='gangPodium'):
            bcast.warn('text="Lying about the APOGEE gang connector being on the cartridge"')
            return self.GANG_ON_CARTRIDGE
        else:
            return self.getPhysicalPos()
            
    def atPodium(self):
        return self.getPos() == self.GANG_ON_PODIUM
        
    def atCartridge(self):
        return self.getPos() == self.GANG_ON_CARTRIDGE

    
