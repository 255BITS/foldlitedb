"""Base imports and missing optional runtime errors must stay lightweight."""
import subprocess
import sys


def test_shared_reducer_import_and_missing_runtime():
    result = subprocess.run([sys.executable, "-c", """
import sys
from foldlitedb import Count, Envelope, Mean, Sum
from foldlitedb.reducers import Reducer, descriptor
assert 'torch' not in sys.modules and 'wasmtime' not in sys.modules
assert descriptor()['abi'] == 1
for shortcut in (Mean, Sum, Count, Envelope):
    assert shortcut().descriptor['kind'] == 'rust'
assert 'wasmtime' not in sys.modules
sys.modules['wasmtime'] = None
for construct in (Reducer, lambda: Mean().identity(), lambda: Sum().identity(),
                  lambda: Count().identity(), lambda: Envelope().identity()):
    try:
        construct()
    except RuntimeError as error:
        assert 'foldlitedb[reducers]' in str(error)
    else:
        raise AssertionError('Missing runtime must not silently fall back')
"""], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
