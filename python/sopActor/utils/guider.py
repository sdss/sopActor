class GuiderState(object):
    """Listen to guider keywords to listen for cartridge changes, etc. """

    def __init__(self, guiderModel):
        """
        Register keywords that we need to pay attention to, and prepare for the
        full callback functions to be added later (e.g., during SopCmd init).
        """
        self.guiderModel = guiderModel

        self.currentCartridge = -1
        self.cartridgeChangeCallback = None

        self.plateType = 'None'
        self.surveyMode = 'None'
        self.surveyCallback = None

        guiderModel.keyVarDict['loadedNewCartridge'].addCallback(
            self.listenToLoadedNewCartridge, callNow=True)

    def _setKeywords(self):
        """Set the cartridge, plateType, surveyMode from the guider model."""

        self.currentCartridge = self.guiderModel.keyVarDict['cartridgeLoaded'][0]
        survey = self.guiderModel.keyVarDict['survey']
        self.plateType = survey[0]
        self.surveyMode = survey[1]

    def setLoadedNewCartridgeCallback(self, cb):
        """Set a method to call when loadedNewCartridge has been output."""

        self.cartridgeChangeCallback = cb
        self.cartridgeChangeCallback(self.currentCartridge, self.plateType, self.surveyMode)

    def listenToLoadedNewCartridge(self, cartridgeLoaded):
        """Grab the cartridge loaded and survey values and pass them on to the defined callback."""

        self._setKeywords()
        if self.cartridgeChangeCallback:
            self.cartridgeChangeCallback(self.currentCartridge, self.plateType, self.surveyMode)
