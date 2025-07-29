# SPS Closed Orbit

In the following we show how to retrieve the SPS closed orbit from NXCALS (from the ALPS system, see https://cds.cern.ch/record/2815318/) and how to reformat it in a pandas dataframe.

### Download the data

We assume that you have access to pcbe-abp-hpc002 and you can source the python distribution 
``` bash
source /home/sterbini/2025_05_22_nxcals_test/miniconda/bin/python
```

You can acquire 'BPMALPS_%:Orbit:%' by executing the 001_acquisition.py.
Prior to the code's execution, you need to `kinit` to have access to the NXCALS resources.

```python
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
```




