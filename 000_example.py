# %%
# ssh to pcbe-abp-hpc002 (if you have not access contact guido.sterbini@cern.ch)
# source /home/sterbini/2025_05_22_nxcals_test/miniconda/bin/activate (if you have not access contact guido.sterbini@cern.ch)
import numpy as np
import sps_alps as alp # clearly you need to be in the correct folder
# import importlib
# importlib.reload(alp)
from matplotlib import pyplot as plt

# %%
# Not using NXCALS YARN 
# timestamps of the get_filtered_nxcals_data function in CET timezone
df = alp.get_filtered_nxcals_data('2025-06-23 19:00:00', 
                                  '2025-06-23 19:05:00', 
                                  'MD_CRAB_26_270_L8823_Q26_2025_V1')
# %%
BPM_df, ctime = alp.process_nxcals(df)
# %% A bit of algebra

# e.g. separating per planes
BPH_df = BPM_df.filter(like='BPH', axis=1)
BPV_df = BPM_df.filter(like='BPV', axis=1)

# averaging along SCs
BPH_avg = BPH_df.iloc[0:1].apply(lambda x: x.mean(), axis=0)
BPH_avg.index = ctime*1000

# RMS along SCs
BPH_rms = BPH_df.apply(alp.column_rms)
BPH_rms.index = ctime*1000
BPH_rms.index.name = 'CTIME'

# compute the average between 1015 and 1055 ms (I started from 1014 to include 1015)
BPH_avg.loc[1014:1055].mean()

# %%
