import sopActor.myGlobals as myGlobals

class ApogeeGang(object):
    """ Encapsulate the APOGEE gang connector. """

    GANG_UNKNOWN      = "unknown gang position"
    GANG_ON_CARTRIDGE = "gang on cartridge"
    GANG_ON_PODIUM    = "gang on podium"
    GANG_AT_SPARSE    = "gang at sparse cals"
    
    def __init__(self):
        self.manualPos = None

    def forcePos(self, forcedPos):
        self.manualPos = forcedPos
        
    def bypass(self, subSystem, doBypass):
        bypass2state = dict(gangValid=self.GANG_UNKNOWN,
                            gangPodium=self.GANG_ON_CARTRIDGE,
                            gangCart=self.GANG_ON_PODIUM,
                            gang=self.GANG_UNKNOWN)

        # Clearing _any_ gang bypass clears all of them.
        if not doBypass:
            self.forcePos(None)
        else:
            self.forcePos(bypass2state.get(subSystem, None))
    
    def getPhysicalPos(self):
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
        if self.manualPos:
            return self.manualPos
        else:
            return self.getPhysicalPos()
            
    def atPodium(self):
        return self.getPos() == self.GANG_ON_PODIUM
        
    def atCartridge(self):
        return self.getPos() == self.GANG_ON_CARTRIDGE

    
