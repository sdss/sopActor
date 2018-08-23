#!/usr/bin/env python

import os
import re
import sys

from opscore.utility.qstr import qstr


class Script(object):
    """
    Load and run a list of commands, including optional maximum timeouts for each.
    
    Default scripts reside in SOPACTOR_DIR/scripts
    """

    def __init__(self, cmd, scriptName, loadFromText=None):
        self.cmd = cmd
        self.name = scriptName
        self.scriptLines = None
        self.atStep = 0
        self.state = 'junk'
        if loadFromText:
            self.loadFromText(loadFromText)
        else:
            self.loadFromScriptFile(self.resolveFilename(self.name))
        self.genStartKeys()

    def loadFromText(self, rawScript):
        if type(rawScript) == str:
            rawScript = rawScript.split('\n')
        rawScript = [s.strip() for s in rawScript]
        rawScript = [s for s in rawScript if len(s) > 0 and s[0] != '#']

        self.scriptLines = []
        for i, l in enumerate(rawScript):
            # print "parsing: %d %s" % (i, l)
            mat = re.search(
                '^(?P<maxTime>\d+\.\d+ +)?(?P<actor>[a-zA-Z][a-zA-Z0-9_]*) +(?P<cmd>.*)', l)
            if not mat:
                raise RuntimeError('failed to parse script line %d: %s' % (i, l))

            matDict = mat.groupdict()
            maxTime = matDict['maxTime']
            scriptLine = [
                matDict['actor'], matDict['cmd'],
                float(maxTime) if maxTime != None else 0.0
            ]
            self.scriptLines.append(scriptLine)

        self.atStep = 0
        self.state = 'idle'

    def loadFromScriptFile(self, path):
        try:
            sfile = open(path, 'r')
        except Exception, e:
            raise RuntimeError('could not load scriptfile %s: %s' % (path, e))

        rawScript = [s.strip() for s in sfile.readlines()]
        self.loadFromText(rawScript)

    def resolveFilename(self, scriptName):
        path = os.path.join(os.environ['SOPACTOR_DIR'], 'scripts', scriptName + '.inp')
        return path

    def stopScript(self):
        self.state = 'stopped'

    def abortScript(self):
        self.state = 'aborted'

    def scriptLineAsString(self, sl):
        return qstr(' '.join([sl[0], sl[1]]))

    def genStatus(self):
        atStep = min(self.atStep, len(self.scriptLines) - 1)

        self.cmd.respond(
            'scriptState=%s,%d,%d,%s,%s' % (self.name, atStep + 1, len(
                self.scriptLines), self.state, self.scriptLineAsString(self.scriptLines[atStep])))

    def genStartKeys(self):
        self.genStatus()
        for i, l in enumerate(self.scriptLines):
            self.cmd.respond(
                'scriptLine=%s,%d,%0.1f,%s' % (self.name, i + 1, l[2], self.scriptLineAsString(l)))

    def fetchNextStep(self):
        """ return (actor, command) or the next step, or None if done. """

        if self.state == 'idle':
            self.state = 'running'

        if self.state in ('aborted', 'stopped', 'done'):
            self.genStatus()
            return None

        if self.atStep >= len(self.scriptLines):
            self.state = 'done'
            self.genStatus()
            return None

        line = self.scriptLines[self.atStep]
        self.genStatus()
        self.atStep += 1

        return line


if __name__ == '__main__':

    class fakeCmd(object):

        def respond(self, s):
            print 'cmd.respond: %s' % (s)

    testScripts = [
        """ 1.0 guider ping """,
        """ guider ping """,
        """ apogeecal shutter close
        15.0 apogee dark time=10.0 ; comment="what is this?"
        10.0 apogeecal shutter open
        """,
    ]

    cmd = fakeCmd()
    for i, s in enumerate(testScripts):
        try:
            script = Script(cmd, 'test%d' % (i), s)

            while True:
                scriptLine = script.fetchNextStep()
                print 'got %s' % (scriptLine)
                if not scriptLine:
                    break

        except Exception, e:
            print 'blammo: %s' % (e)
            raise

        print
