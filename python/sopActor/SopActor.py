#!/usr/bin/env python
# encoding: utf-8
#
# SopActor.py
#
# Created by José Sánchez-Gallego on 1 Jun 2016.
# Licensed under a 3-clause BSD license.
#
# Revision history:
#    1 Jun 2016 J. Sánchez-Gallego
#       Initial version of SopActor.py using the SDSSActor class


from __future__ import division
from __future__ import print_function


import abc
import os

import opscore.actor.model
import opscore.actor.keyvar

import actorcore.Actor

# modules that have threads to run
import masterThread
import bossThread
import apogeeThread
import guiderThread
import gcameraThread
import ffsThread
import lampThreads
import tccThread
import scriptThread
import slewThread

import sopActor
from sopActor import myGlobals
from sopActor.utils.guider import GuiderState
from sopActor.utils.gang import ApogeeGang

from bypass import Bypass


class SopActor(actorcore.Actor.SDSSActor):
    """The SOP actor main class."""

    __metaclass__ = abc.ABCMeta

    @staticmethod
    def newActor(location=None, **kwargs):
        """Return the version of the actor based on our location."""

        # Determines the location (this method is defined in SDSSActor)
        location = SopActor._determine_location(location)

        # Creates the appropriate object depending on the location
        if location == 'APO':
            return SopActorAPO('sop', productName='sopActor', **kwargs)
        elif location == 'LCO':
            return SopActorLCO('sop', productName='sopActor', **kwargs)
        elif location == 'LOCAL':
            return SopActorLocal('sop', productName='sopActor', **kwargs)
        else:
            raise KeyError('Don\'t know my location: cannot '
                           'return a working Actor!')

    def __init__(self, name, productName=None, configFile=None, debugLevel=30,
                 makeCmdrConnection=True):

        actorcore.Actor.Actor.__init__(self, name, productName=productName,
                                       configFile=configFile,
                                       makeCmdrConnection=makeCmdrConnection)

        self.headURL = '$HeadURL$'

        self.logger.setLevel(debugLevel)
        self.logger.propagate = True

        sopActor.myGlobals.bypass = Bypass()

        # Define the Thread list
        self.threadList = [
            ('master', sopActor.MASTER, masterThread),
            ('boss', sopActor.BOSS, bossThread),
            ('apogee', sopActor.APOGEE, apogeeThread),
            ('apogeeScript', sopActor.APOGEE_SCRIPT, apogeeThread),
            ('script', sopActor.SCRIPT, scriptThread),
            ('guider', sopActor.GUIDER, guiderThread),
            ('gcamera', sopActor.GCAMERA, gcameraThread),
            ('ff', sopActor.FF_LAMP, lampThreads.ff_main),
            ('hgcd', sopActor.HGCD_LAMP, lampThreads.hgcd_main),
            ('ne', sopActor.NE_LAMP, lampThreads.ne_main),
            ('uv', sopActor.UV_LAMP, lampThreads.uv_main),
            ('wht', sopActor.WHT_LAMP, lampThreads.wht_main),
            ('ffs', sopActor.FFS, ffsThread),
            ('tcc', sopActor.TCC, tccThread),
            ('slew', sopActor.SLEW, slewThread)]

        # Explicitly load other actor models.
        self.models = {}
        for actor in ['boss', 'guider', 'platedb', 'mcp',
                      'sop', 'tcc', 'apogee', 'hartmann']:
            self.models[actor] = opscore.actor.model.Model(actor)

        self.actorState = actorcore.Actor.ActorState(self, self.models)
        self.actorState.guiderState = GuiderState(self.models['guider'])
        self.actorState.apogeeGang = ApogeeGang()
        myGlobals.actorState = self.actorState

        # This is the default set of commands, valid both at APO and LCO
        self.actorState.actor.commandSets['SopCmd'].initCommands()

        self.actorState.timeout = 60  # timeout on message queues

        self._readWarmUpTimes()

    def periodicStatus(self):
        """Run some command periodically"""
        pass

    def connectionMade(self):
        """Runs this after a connection is made from the hub."""
        pass


class SopActorAPO(SopActor):
    """APO version of this actor."""
    location = 'APO'

    def _readWarmUpTimes(self):
        """Sets the warm up times for the lamps from the config file."""

        warmupList = self.config.get('lamps', 'warmupTime').split()
        sopActor.myGlobals.warmupTime = {}
        for i in range(0, len(warmupList), 2):
            k, v = warmupList[i:i + 2]
            sopActor.myGlobals.warmupTime[{'ff': sopActor.FF_LAMP,
                                           'hgcd': sopActor.HGCD_LAMP,
                                           'ne': sopActor.NE_LAMP,
                                           'wht': sopActor.WHT_LAMP,
                                           'uv': sopActor.UV_LAMP
                                           }[k.lower()]] = float(v)


class SopActorLCO(SopActor):
    """LCO version of this actor."""
    location = 'LCO'

    def _readWarmUpTimes(self):
        """Sets the warm up times for the lamps from the config file."""
        pass


class SopActorLocal(SopActor):
    '''Local Version of this actor '''
    location = 'LOCAL'

    def _readWarmUpTimes(self):
        """Sets the warm up times for the lamps from the config file."""

        warmupList = self.config.get('lamps', 'warmupTime').split()
        sopActor.myGlobals.warmupTime = {}
        for i in range(0, len(warmupList), 2):
            k, v = warmupList[i:i + 2]
            sopActor.myGlobals.warmupTime[{'ff': sopActor.FF_LAMP,
                                           'hgcd': sopActor.HGCD_LAMP,
                                           'ne': sopActor.NE_LAMP,
                                           'wht': sopActor.WHT_LAMP,
                                           'uv': sopActor.UV_LAMP
                                           }[k.lower()]] = float(v)
