"""
Allow SOP to ignore errors and fake state by setting a bypass.
"""


class Bypass(object):
    """
    Provide bypasses for subsystems

    A failure code from a bypassed subsystem will not cause a MultiCommand to fail
    """

    def __init__(self):
        """Define what can be bypassed on init, and clear them all."""

        self._bypassed = {}

        for ss in ('ffs', 'lamp_ff', 'lamp_hgcd', 'lamp_ne', 'axes', 'slewToField',
                   'guiderDark', 'guider_decenter', 'gangToCart', 'gangToPodium',
                   'isBHM', 'isBHMMWM', 'isBHMLead', 'isMWMLead', 'noBOSS', 'noAPOGEE'):
            self._bypassed[ss] = False

        # The bypasses in these groups are mutually-contradictory.
        self.cartBypasses = ('isBHM', 'isBHMMWM', 'isBHMLead', 'isMWMLead',
                             'noBOSS', 'noAPOGEE')
        self.gangBypasses = ('gangToPodium', 'gangToCart')

    def set(self, name, bypassed=True):
        """
        Turn a bypass on or off (default on).

        For mutually-contradictory bypasses, clear the appropriate other ones.
        """
        if name in self._bypassed:
            if self.is_cart_bypass(name) and bypassed:
                self.clear_cart_bypasses()
            if self.is_gang_bypass(name) and bypassed:
                self.clear_gang_bypasses()
            self._bypassed[name] = bypassed
        else:
            return None

        return bypassed

    def get(self, name=None, cmd=None):
        """
        Get the value of a named bypass, or the output of all bypasses.

        Include cmd to warn() when the a system fails but is bypassed
        """
        if name:
            bypassed = self._bypassed.get(name, False)
            if bypassed and cmd:
                cmd.warn('text="System %s failed but is bypassed"' % name)

            return bypassed

    def is_cart_bypass(self, name):
        """Return true if this is a cartridge bypass."""
        return name in self.cartBypasses

    def is_gang_bypass(self, name):
        """Return true if this is a cartridge bypass."""
        return name in self.gangBypasses

    def clear_cart_bypasses(self):
        """Clear all cartridge bypasses."""
        for name in self.cartBypasses:
            self._bypassed[name] = False

    def clear_gang_bypasses(self):
        """Clear all gang connector bypasses."""
        for name in self.gangBypasses:
            self._bypassed[name] = False

    def get_bypassedNames(self):
        """Return an alphabetized list of currently-bypassed systems, for keyword output."""
        values = [k for k, v in sorted(self._bypassed.items()) if v]
        return values

    def get_bypass_list(self):
        """Return a tuple of two lists (bypassNames, bypassedState) for keyword output."""
        keys = sorted(self._bypassed)
        values = [int(v) for k, v in sorted(self._bypassed.items())]
        return keys, values
