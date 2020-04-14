import os
import unittest


def controlled_access(f):
    mode = os.environ.get("TNU_TESTMODE", "workspace_access")
    return unittest.skipUnless("controlled_access" in mode, "Skipping controlled access test")(f)
