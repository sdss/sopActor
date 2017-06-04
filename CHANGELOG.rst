.. _sopActor-changelog:

==========
Change Log
==========

This document records the main changes to the sopActor code.

.. _changelog-v3_9:
v3_9 (unreleased)
-----------------

Changed
^^^^^^^
* Modified warm up time for HgCd lamp to 120 seconds.

Fixed
^^^^^
* Ticket #2707: Unclean Stop for MaNGA sequence. Fixes a problem in which stopping a doApogeeMangaSequence or doMangaSequence caused the BOSS exposure to be left on a legible but not readout state.
* Ticket #2715: Add MaStar survey mode
* Ticker #2763: Failure to update proper dither sequence when count modified during readout of last exposure
* Ticket #2483: Refactored SopActor to use SDSSActor, and moved sopActor_main to bin



.. x.y.z (unreleased)
.. ------------------
..
.. A short description
..
.. Added
.. ^^^^^
.. * TBD
..
.. Changed
.. ^^^^^^^
.. * TBD
..
.. Fixed
.. ^^^^^
.. * TBD
