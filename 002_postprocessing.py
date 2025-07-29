# %%
# load datafram from pkl
import pandas as pd
nxcals_df = pd.read_pickle('nxcals_data.pkl')
# %%
import matplotlib.pyplot as plt
plt.plot(nxcals_df['SR.BMEAS-B-ST:SamplesFromTrigger:samples'].iloc[0][0])
# %%
import numpy as np
for ii in nxcals_df.columns:
    if 'positions' in ii:
        print(ii)
        nxcals_df[ii] = nxcals_df[ii].apply(np.array)
cycles = []
my_ALPS = ['BPMALPS_1', 'BPMALPS_2', 'BPMALPS_3',
        'BPMALPS_4', 'BPMALPS_5', 'BPMALPS_6']
for my_cycle in nxcals_df.index:
    my_dict = {}
    print(f'Processing cycle: {my_cycle}')
    for ALPS in my_ALPS:
        positions =np.reshape(np.transpose(
            nxcals_df.loc[my_cycle, f'{ALPS}:Orbit:positions'][0]
            ),
            nxcals_df.loc[my_cycle, f'{ALPS}:Orbit:positions'][1])
        for nn, channelName in enumerate(nxcals_df.loc[my_cycle, f'{ALPS}:Orbit:channelNames'][0]):
            print(f'Channel: {channelName}, nn: {nn}')
            my_dict[channelName] =positions[nn,:]
    my_dict['cyclestamp'] = my_cycle      
    cycles.append(my_dict)
# %%
BPM_df = pd.DataFrame(cycles); 
BPM_df = BPM_df.set_index('cyclestamp')
BPM_df.index.name = None
#BPM_df =BPM_df.apply(lambda x: x[0], axis=1)
BPV_df = BPM_df.filter(like='BPV', axis=1)
BPH_df = BPM_df.filter(like='BPH', axis=1)
# %%
# %%
from matplotlib import pyplot as plt
samples = len(nxcals_df['SR.BMEAS-B-ST:SamplesFromTrigger:samples'].iloc[0][0])
t = np.linspace(0, samples/1000, samples, endpoint=False)
samples = len(BPM_df['BPH.63208.H'].iloc[0])
ctime_inj_s = 1.185
tt = np.linspace(0+ctime_inj_s, samples/100+ctime_inj_s, samples, endpoint=False)
plt.plot(tt, BPM_df['BPH.63208.H'].iloc[0],'o',alpha=.1, label='BPH.63208.H')

plt.plot(t, np.array(nxcals_df['SR.BMEAS-B-ST:SamplesFromTrigger:samples'].iloc[0][0]) /10000, label='SR.BMEAS-B-ST:SamplesFromTrigger:samples')
plt.xlabel('Time [s]')
plt.ylabel('Position [mm]')
plt.title('Assuming start of the ALPS at CTIME = 1185 ms')
plt.legend(loc='upper left')
plt.grid(True)
#
# %%
BPM_df[['BPH.63208.H']].iloc[0]
# %%
