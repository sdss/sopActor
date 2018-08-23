#!/usr/bin/env python2
# encoding: utf-8

# Created by Brian Cherinka on 2016-06-09 14:28:43
# Licensed under a 3-clause BSD license.

# Revision History:
#     Initial Version: 2016-06-09 14:28:43 by Brian Cherinka
#     Last Modified On: 2016-06-09 14:28:43 by Brian

from __future__ import absolute_import, division, print_function

import sys

from sopActor import Msg, Queue, SopActor


# start a new SopActor
if __name__ == '__main__':

    if len(sys.argv) > 1:
        location = sys.argv[1]
    else:
        location = None

    sop = SopActor.SopActor.newActor(location=location)
    sop.run(Msg=Msg, queueClass=Queue)
