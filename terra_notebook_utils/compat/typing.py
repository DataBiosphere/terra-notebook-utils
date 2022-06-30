try:
    # python3.10+
    from collections.abc import Iterable
except ImportError:
    # python versions < 3.10
    from collections import Iterable
