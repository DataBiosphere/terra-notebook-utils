import traceback

try:
    assert 1 == 2
except Exception as e:
    traceback.print_exc()
    print()
