.. _sopActor-changelog:

==========
Change Log
==========

This document records the main changes to the sopActor code.

.. _changelog-v3_9:
v3_9 (2017-06-11)
-----------------

Added
^^^^^

* Better reporting  of current dither index in double length APOGEE-MaNGA sequences. ``doApogeeMangaSequence_ditherSeq`` is now output after each MaNGA exposure.
* Outputting new keyword of estimated time remaining for apogee and manga dither sequences.

Changed
^^^^^^^
* Modified warm up time for HgCd lamp to 120 seconds.

Fixed
^^^^^
* Ticket `#2707 <https://trac.sdss.org/ticket/2707>`_: Unclean Stop for MaNGA sequence. Fixes a problem in which stopping a doApogeeMangaSequence or doMangaSequence caused the BOSS exposure to be left on a legible but not readout state.
* Ticket `#2715 <https://trac.sdss.org/ticket/2715>`_: Add MaStar survey mode
* Ticker `#2763 <https://trac.sdss.org/ticket/2763>`_: Failure to update proper dither sequence when count modified during readout of last exposure
* Ticket `#2483 <https://trac.sdss.org/ticket/2483>`_: Refactored SopActor to use SDSSActor, and moved sopActor_main to bin.
* Ticket `#2203 <https://trac.sdss.org/ticket/2203>`_: 3-minute timeout occurs after stopping gotoField command. ``CmdState.stop_tcc()`` now issues ``tcc track /stop`` instead of ``tcc axis stop``.
* Ticket `#2701 <https://trac.sdss.org/ticket/2701>`_: SOP Actions when hartmann fails on "gotoField". Collimator correction is always applied. gotoField for APOGEE-led plates do not fail even if the hartmann fails.
* Ticket `#2748 <https://trac.sdss.org/ticket/2748>`_: Don't allow a slew during MaNGA post-calibration. Slews are disabled during ``do_boss_calibs`` until the readout of the last exposure (usually an arc).
* Ticket `#2808 <https://trac.sdss.org/ticket/2808>`_: fixes a problem in which ``gotoInstrumentChange`` and ``gotoStow`` could not be stopped from STUI.
* Ticket `#2805 <https://trac.sdss.org/ticket/2805>`_: STUI SOP should display estimated time remaining for dither sets.


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
