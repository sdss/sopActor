
class GuiderState(object):
    """Listen to guider keywords to listen for cartridge changes, etc. """

    def __init__(self, guiderModel):
        """Register keywords that we need to pay attention to"""

        self.currentCartridge = -1
        self.cartridgeChangeCallback = None

        guiderModel.keyVarDict["cartridgeLoaded"].addCallback(self.listenToCartridgeLoaded, callNow=True)
        
    def setCartridgeLoadedCallback(self, cb):
        self.cartridgeChangeCallback = cb
        if self.cartridgeChangeCallback:
            self.cartridgeChangeCallback(self.currentCartridge)
        
    def listenToCartridgeLoaded(self, cartridgeLoaded):
        cartridge = cartridgeLoaded.valueList[0]
        #if cartridge == self.currentCartridge:
        #    return

        self.currentCartridge = cartridge
        if self.cartridgeChangeCallback:
            self.cartridgeChangeCallback(self.currentCartridge)
            
