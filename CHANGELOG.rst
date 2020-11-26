.. _sopActor-changelog:

==========
Change Log
==========

4.0.3 (unreleased)
------------------

Added
^^^^^
* Added ``noAPOGEE`` to disable APOGEE exposures during a BOSS-APOGEE sequence.
* ``noBOSS`` and ``noAPOGEE`` now update the refraction balance [COS-17].
* Reset the correct plateType and surveyMode when clearing a bypass (#10).

Changed
^^^^^^^
* Remove deprecated bypasses.
* Rename ``nDithers`` in ``doApogeeBossScience`` to ``nExposures``.


4.0.2 (2020-11-09)
------------------

Added
^^^^^
* ``noBOSS`` bypass to disable BOSS exposures during a sequence. Only works with BOSS-APOGEE (SDSS-V) sequences (#9).


4.0.1 (2020-10-24)
------------------

Fixed
^^^^^
* Remove check of SP2 hartmann value.


4.0.0 (2020-10-17)
------------------

Added
^^^^^
* ``BHM-MWM`` plate type and ``BHM lead`` and ``MWM lead`` survey modes. Go to field and science sequence is similar to APOOGE-2/MaNGA.

Changed
^^^^^^^
* BHM-MWM observations do not dither with the APOGEE spectrograph.


3.12.7 (2020-01-20)
-------------------

Fixed
^^^^^
* Avoid a problem with APOGEE goto field stops that do not have a ``hartmann`` stage.


3.12.6 (2020-01-12)
-------------------

Fixed
^^^^^
* A series of bugs that prevented a ``gotoField`` sequence to be stopped successfully. This includes a race problem in which while stopping a slew, the hartmann would start before it could be cancelled. Another bug prevented the slew to be stopped in some circumstances. In combination with ``hartmannActor 1.7.1``, which allows to stop the hartmann collimate sequence, this should fix `#2203 <https://trac.sdss.org/ticket/2203>`__ and `#2919 <https://trac.sdss.org/ticket/2919>`__.


3.12.5 (2019-10-19)
-------------------

Fixed
^^^^^
* Incorrect reference to ``BOSS_ACTOR``.


3.12.4 (2019-10-16)
-------------------

Changed
^^^^^^^
* Better support for MaNGA dither plates with exposure time different than 900 seconds. In particular this is needed for some ``MaNGA Globular`` plates.


3.12.3 (2019-04-19)
-------------------

Added
^^^^^
* ``MaNGA Globular`` survey mode (which is just an alias of ``MaNGA Dither``) with bypasses and tests.

Fixed
^^^^^
* Some ``MaStar`` short exposure tests were still using the old 30s exposure time..


3.12.2 (2019-01-22)
-------------------

Added
^^^^^
* ``guider_decenter`` bypass.
* Clear BOSS queue of all scheduled tasks when aborting a MaStar sequence with short exposures.

Changed
^^^^^^^
* Set ``readoutDuration`` to 82 seconds.

Fixed
^^^^^
* Fixed `#2933 <https://trac.sdss.org/ticket/2933>`__: ``manga_lead`` was being reset when a new sequence was commanded.
* Fixed `#2919 <https://trac.sdss.org/ticket/2919>`__: clear all MaStar short exposures when aborting a sequence.


3.12.1 (2018-12-12)
-------------------

Changed
^^^^^^^
* Set the default offset for ``MaStar`` plates to 20 arcsec in altitude.

Fixed
^^^^^
* Some tests broken by the introduction of the ``offset`` stage in ``doBossCalibs``.


3.12.0 (2018-12-11)
-------------------

Added
^^^^^
* Implement MaStar-led observations with short exposures (PR `#5 <https://github.com/sdss/sopActor/pull/5>`__).
* Adds offset to ``doBossCalibs`` for MaStar post-cals (PR `#6 <https://github.com/sdss/sopActor/pull/6>`__).


3.11.1 (2018-09-26)
-------------------

Added
^^^^^
* Fix syntax error in previous release.


3.11.0 (2018-09-26)
-------------------

Added
^^^^^
* Support for MaNGA short exposure time (`#2 <https://github.com/sdss/sopActor/issues/2>`_).
* Pass ``bypass="ffs"`` to ``hartmannActor`` when FFS are bypassed (`#1 <https://github.com/sdss/sopActor/issues/1>`__).


3.10.2 (2018-04-02)
-------------------

Fixed
^^^^^
* Ticket `#2859 <https://trac.sdss.org/ticket/2859>`_: GOTOField should stop to take hartmann if the slew field.
* Lots of `nInfo` in tests that were out of date after adding the `availableScripts` output to the status.


3.10.1 (2018-02-01)
-------------------

Fixed
^^^^^
* Forgot to remove the ``dev`` suffix in the version when released ``3.10.0``.


3.10.0 (2018-02-01)
-------------------

Added
^^^^^
* Using new versioning pattern, ``X.Y.Z``.
* ``sopActor_main.py`` now accepts a location value. If no value is provided the behaviour is the same as in the past.
* PEP8 beautified ``SopCmd.py``.
* Added ``stopScript`` command and other changes to allow STUI to show a script widget in the SOP GUI window. Fixes `#2842 <https://trac.sdss.org/ticket/2842>`_.


v3_9_5 (2017-11-06)
-------------------

Added
^^^^^
* Dither sequences for DoMangaSequence and DoApogeeMangaSequence are now modifiable.
* Added ``MaNGA 10min`` survey mode for IC342 observations.

Changed
^^^^^^^
* APOGEE-MaNGA, APOGEE-led fail on gotoField if blue ring is out of range.

Fixed
^^^^^
* Ticket `#2460 <https://trac.sdss.org/ticket/2460>`_: Cannot modify MaNGA Dithers within SOP.
* Ticket `#2860 <https://trac.sdss.org/ticket/2810>`_: multicommand timeouts crash SOP.
* Ticket `#2707 <https://trac.sdss.org/ticket/2707>`_: unclean stop of MaNGA sequences.


v3_9_4 (2017-06-12)
-------------------

Fixed
^^^^^
* Fixed a bug with outputting of the doApogeeScience_index keyword in the CmdState getUserKeys


v3_9_3 (2017-06-11)
-------------------

Fixed
^^^^^
* The previous fix to ``gotoInstrumentChange`` or ``gotoStow`` was incomplete. Let's see if this does the trick.


v3_9_2 (2017-06-11)
-------------------

Fixed
^^^^^
* Fixes a bug aborting ``gotoInstrumentChange`` or ``gotoStow``.


v3_9_1
------

This version was skipped.


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
