import numpy as np
import pandas as pd

a = np.array([1,2,3,4,5])

print a * 5

df = pd.DataFrame(a, columns= ["column_1"])

print df