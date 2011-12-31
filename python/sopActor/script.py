class Script(object):
    def __init__(self, cmd, scriptName):
        self.cmd = cmd
        self.scriptLines = None
        self.atStep = 0
        self.doRun = True
        
    def genStartKeys(self):
        pass

    def runNextStep(self):
        if not self.doRun:
            return self.finish(success=False)

        if self.atStep >= len(self.scriptLines):
            return self.finish(success=True)

        
            
            
