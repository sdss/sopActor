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
* Ticket `#2707 <https://trac.sdss.org/ticket/2707>`_: Unclean Stop for MaNGA sequence. Fixes a problem in which stopping a doApogeeMangaSequence or doMangaSequence caused the BOSS exposure to be left on a legible but not readout state.
* Ticket `#2715 <https://trac.sdss.org/ticket/2715>`_: Add MaStar survey mode
* Ticker `#2763 <https://trac.sdss.org/ticket/2763>`_: Failure to update proper dither sequence when count modified during readout of last exposure
* Ticket `#2483 <https://trac.sdss.org/ticket/2483>`_: Refactored SopActor to use SDSSActor, and moved sopActor_main to bin.
Ticket `#2203 <https://trac.sdss.org/ticket/2203>`_: 3-minute timeout occurs after stopping gotoField command. ``CmdState.stop_tcc()`` now issues ``tcc track /stop`` instead of ``tcc axis stop``.


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
