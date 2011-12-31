#!/usr/bin/env python

import re, sys, time
import threading

import sopActor.myGlobals as myGlobals

class Script(object):
    def __init__(self, cmd, scriptName):
        self.cmd = cmd
        self.name = scriptName
        self.scriptLines = None
        self.atStep = 0
        self.doRun = True

    def laodFromScriptFile(self):
        pass

    def genStartKeys(self):
        pass

    def fetchNextStep(self):
        """ return (actor, command) or the next step, or None if done. """

        if not self.doRun:
            return self.finish(success=False)

        if self.atStep >= len(self.scriptLines):
            return self.finish(success=True)

