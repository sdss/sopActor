
class GuiderState(object):
    """Listen to guider keywords to listen for cartridge changes, etc. """

    def __init__(self, guiderModel):
        """
        Register keywords that we need to pay attention to, and prepare for the
        full callback functions to be added later (e.g., during SopCmd init).
        """
        self.updated = True # so we'll do the callback once on init.

        self.currentCartridge = -1
        self.cartridgeChangeCallback = None
        guiderModel.keyVarDict["cartridgeLoaded"].addCallback(self.listenToCartridgeLoaded, callNow=True)

        self.plateType = 'None'
        self.surveyMode = 'None'
        self.surveyCallback = None
        guiderModel.keyVarDict["survey"].addCallback(self.listenToSurvey, callNow=True)

    @property
    def updated(self):
        """
        Have both necessary keywords been updated?
        Setter sets both to same value (helpful for clearing)

        When the guider runs loadCartridge, SOP needs both the survey and
        cartridgeLoaded keywords to correctly process it.
        This helps handle that, by letting us only run a callback once both
        have been updated.
        """
        return self.surveyUpdated and self.cartridgeUpdated
    @updated.setter
    def updated(self,value):
        self.surveyUpdated = value
        self.cartridgeUpdated = value

    def setCartridgeLoadedCallback(self, cb):
        """Set a method to call when cartridgeLoaded has been updated."""
        self.cartridgeChangeCallback = cb
        if self.cartridgeChangeCallback and self.updated:
            self.updated = False
            self.cartridgeChangeCallback(self.currentCartridge, self.plateType, self.surveyMode)

    def listenToCartridgeLoaded(self, cartridgeLoaded):
        """Grab the cartridge loaded value and pass it and survey on to the defined callback."""
        cartridge = cartridgeLoaded.valueList[0]
        self.cartridgeUpdated = True
        self.currentCartridge = cartridge

        if self.cartridgeChangeCallback and self.updated:
            self.updated = False
            self.cartridgeChangeCallback(self.currentCartridge, self.plateType, self.surveyMode)

    def setSurveyCallback(self, cb):
        """Set a method to call when survey has been updated."""
        self.surveyCallback = cb
        if self.surveyCallback and self.updated:
            self.updated = False
            self.surveyCallback(self.currentCartridge, self.plateType, self.surveyMode)

    def listenToSurvey(self, surveyUpdate):
        """Grab the survey value and pass it and cartridge loaded on to the defined callback."""
        survey = surveyUpdate.valueList
        self.surveyUpdated = True
        self.plateType = survey[0]
        self.surveyMode = survey[1]

        if self.surveyCallback and self.updated:
            self.updated = False
            self.surveyCallback(self.currentCartridge, self.plateType, self.surveyMode)

