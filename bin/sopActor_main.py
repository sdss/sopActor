#!/usr/bin/env python2
# encoding: utf-8


# Created by Brian Cherinka on 2016-06-09 14:28:43
# Licensed under a 3-clause BSD license.

# Revision History:
#     Initial Version: 2016-06-09 14:28:43 by Brian Cherinka
#     Last Modified On: 2016-06-09 14:28:43 by Brian


from __future__ import print_function, division, absolute_import
from sopActor import Msg, SopActor, Queue

# start a new SopActor
if __name__ == "__main__":
    sop = SopActor.SopActor.newActor()
    sop.run(Msg=Msg, queueClass=Queue)


