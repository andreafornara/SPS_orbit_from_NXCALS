# %% /home/sterbini/2025_05_22_nxcals_test/miniconda/bin/python
import nx2pd as nx
import pandas as pd
from nxcals.spark_session_builder import get_or_create, Flavor

# few initializations
spark = get_or_create(flavor=Flavor.LOCAL)
sk  = nx.SparkIt(spark)

# the data to be acquisition
t0 = pd.Timestamp('2025-06-23 19:00:00',tz="CET")
t1 = pd.Timestamp('2025-06-23 19:05:00',tz="CET")
nxcals_df = sk.get(t0, t1, ['BPMALPS_%:Orbit:%',
                      'SPS%TGM:%',
                      'SPS.LSA:CYCLE',
                      'SR.BMEAS-B-ST:SamplesFromTrigger:samples'])
# %%
