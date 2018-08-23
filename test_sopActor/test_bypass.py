"""
Test the basics of the bypass system, which is now a separate class to be
instantiated, instead of a staic class with static methods.
"""

import unittest

import sopActor
from actorcore import TestHelper


# import sopTester


class TestBypass(unittest.TestCase):

    def setUp(self):
        self.bypass = sopActor.bypass.Bypass()
        self.userKeys = False
        self.verbose = True
        super(TestBypass, self).setUp()

    def test_bypass_init(self):
        """init should have all bypasses defined, and off."""
        for name in TestHelper.sopState['ok']['bypassNames']:
            self.assertIn(name, self.bypass._bypassed)
            self.assertFalse(self.bypass.get(name))

    def _is_cart_bypass(self, name, expect):
        result = self.bypass.is_cart_bypass(name)
        self.assertEqual(result, expect)

    def test_is_cart_bypass_isBoss(self):
        self._is_cart_bypass('isBoss', True)

    def test_is_cart_bypass_gangToPodium(self):
        self._is_cart_bypass('gangToPodium', False)

    def _is_gang_bypass(self, name, expect):
        result = self.bypass.is_gang_bypass(name)
        self.assertEqual(result, expect)

    def test_is_gang_bypass_gangToCart(self):
        self._is_gang_bypass('gangToCart', True)

    def test_is_gang_bypass_gangToPodium(self):
        self._is_gang_bypass('gangToPodium', True)

    def test_is_gang_bypass_isBoss(self):
        self._is_gang_bypass('isBoss', False)

    def test_clear_cart_bypasses(self):
        carts = [
            'isBoss', 'isApogee', 'isMangaDither', 'isMangaStare', 'isManga10', 'isApogeeLead',
            'isApogeeMangaDither', 'isApogeeMangaStare', 'isApogeeManga10'
        ]
        for x in self.bypass._bypassed:
            self.bypass._bypassed[x] = True
        self.bypass.clear_cart_bypasses()
        for x in carts:
            self.assertFalse(self.bypass.get(x))

    def test_clear_gang_bypasses(self):
        gangs = ['gangToPodium', 'gangToCart']
        for x in self.bypass._bypassed:
            self.bypass._bypassed[x] = True
        self.bypass.clear_gang_bypasses()
        for x in gangs:
            self.assertFalse(self.bypass.get(x))

    def _set_bypass(self, name):
        result = self.bypass.set(name)
        self.assertTrue(result)
        self.assertTrue(self.bypass.get(name))

    def test_set_lamp_ff(self):
        self._set_bypass('lamp_ff')

    def test_set_bad(self):
        result = self.bypass.set('NotARealBypass')
        self.assertIsNone(result)

    def _set_cart_bypass(self, name):
        """cart bypasses should clear all other cart bypasses."""
        result = self.bypass.set(name)
        self.assertTrue(result)
        for x in [
                'isBoss', 'isApogee', 'isMangaDither', 'isMangaStare', 'isManga10', 'isApogeeLead',
                'isApogeeMangaDither', 'isApogeeMangaStare', 'isApogeeManga10'
        ]:
            if name != x:
                self.assertFalse(self.bypass.get(x), '%s should not be set' % x)
            else:
                self.assertTrue(self.bypass.get(name), '%s should be set' % name)

    def test_set_isBoss(self):
        self.bypass.set('isApogee')
        self._set_cart_bypass('isBoss')

    def _set_gang_bypass(self, name):
        """Bypassing to one gang position should clear the other."""
        result = self.bypass.set(name)
        self.assertTrue(result)
        for x in ('gangToPodium', 'gangToCart'):
            if name != x:
                self.assertFalse(self.bypass.get(x), '%s should not be set' % x)
            else:
                self.assertTrue(self.bypass.get(name), '%s should be set' % name)

    def test_set_gang_podium(self):
        self.bypass.set('gangToCart')
        self._set_gang_bypass('gangToPodium')

    def test_set_gang_cart(self):
        self.bypass.set('gangToPodium')
        self._set_gang_bypass('gangToCart')

    def test_get_one(self):
        result = self.bypass.get('axes')
        self.assertFalse(result)
        self.bypass.set('axes')
        result = self.bypass.get('axes')
        self.assertTrue(result)

    def test_get_bypass_list_empty(self):
        result = self.bypass.get_bypass_list()
        self.assertEqual(result[0], sorted(TestHelper.sopState['ok']['bypassNames']))
        self.assertEqual(result[1], [0] * len(TestHelper.sopState['ok']['bypassNames']))

    def test_get_bypass_list_two_set(self):
        isSet = ('axes', 'gangToPodium', 'lamp_ff')
        for x in isSet:
            self.bypass.set(x)
        result = self.bypass.get_bypass_list()
        self.assertEqual(result[0], sorted(TestHelper.sopState['ok']['bypassNames']))
        for k, v in zip(result[0], result[1]):
            self.assertIs(type(v), int)
            if k in isSet:
                self.assertEqual(v, 1, '%s should be set' % k)
            else:
                self.assertEqual(v, 0, '%s should not be set' % k)

    def test_get_bypassedNames_empty(self):
        result = self.bypass.get_bypassedNames()
        self.assertEqual(result, [])

    def test_get_bypassedNames_two_set(self):
        self.bypass.set('gangToCart')
        self.bypass.set('axes')
        result = self.bypass.get_bypassedNames()
        self.assertEqual(result, ['axes', 'gangToCart'])


if __name__ == '__main__':
    verbosity = 2

    unittest.main(verbosity=verbosity)
