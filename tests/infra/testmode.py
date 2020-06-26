import os
import unittest


def testmode(mode_for_test):
    def wrapper(func):
        mode = os.environ.get("TNU_TESTMODE", "workspace_access")
        return unittest.skipUnless(mode_for_test in mode, f"Skipping {mode_for_test} test")(func)
    return wrapper
