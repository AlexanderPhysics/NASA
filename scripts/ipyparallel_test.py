from __future__ import print_function, division, absolute_import
from ipyparallel.parallel import Client
import time

# connect to cluster running the mpi profile
# connection will fail without explicit mentioning of
# which profile to connect to
rc = Client(profile='mpi')
dview = rc[:]

@dview.remote(block=True)
def getpid():
    import os
    return os.getpid()

print(getpid())

@dview.remote(block=True)
def gethostname():
    import os
    return os.uname()

print(set(gethostname()))

# external imports on engines
# if local=True is not set the serial calculation below will fail
with dview.sync_imports(local=True):
    import numpy as np
    import scipy as sp
    import sympy

starttime = time.time()
serial_result = map(lambda n: sympy.ntheory.isprime(n), xrange(5000000))
serialtime = time.time() - starttime
starttime = time.time()
parallel_result = dview.map_sync(lambda n: sympy.ntheory.isprime(n), xrange(5000000))
paralleltime = time.time() - starttime
correctness = (serial_result == parallel_result)

print('Serial and parallel result are equal: {0}.'.format(correctness))
print('Serial calculation took {0} seconds, parallel took {1} seconds'.format(serialtime, paralleltime))