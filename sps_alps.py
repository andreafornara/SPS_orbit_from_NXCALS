import pandas as pd
import nx2pd as nx
from nxcals.spark_session_builder import get_or_create, Flavor

def get_filtered_nxcals_data(t0_str, t1_str, cycle_name):
    """
    Fetch and filter NXCALS data between two timestamps for a given cycle name.

    Parameters:
    - t0_str (str): Start timestamp string in CET, e.g., '2025-06-23 19:00:00'
    - t1_str (str): End timestamp string in CET, e.g., '2025-06-23 19:05:00'
    - cycle_name (str): The cycle name to filter on.

    Returns:
    - pandas.DataFrame: Filtered NXCALS data.
    """
    # Initialize Spark
    spark = get_or_create(flavor=Flavor.LOCAL)
    sk = nx.SparkIt(spark)

    # Parse timestamps with CET timezone
    t0 = pd.Timestamp(t0_str, tz="CET")
    t1 = pd.Timestamp(t1_str, tz="CET")

    # Define signal list
    signals = [
        'BPMALPS_%:Orbit:%',
        'SPS%TGM:%',
        'SPS.LSA:CYCLE',
        'SR.BMEAS-B-ST:SamplesFromTrigger:samples'
    ]

    # Acquire data
    nxcals_df = sk.get(t0, t1, signals)

    # Apply cycle filter
    filtered_df = nxcals_df[nxcals_df['SPS.LSA:CYCLE'] == cycle_name]

    return filtered_df

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

def process_nxcals(nxcals_df, ctime_inj_s=1.015):
    """
    Process raw nxcals DataFrame containing orbit position data from BPM ALPS.
    
    Parameters:
    -----------
    nxcals_df : pd.DataFrame
        Raw input data with BPM ALPS orbit measurements.
    ctime_inj_s : float
        Injection time in seconds to offset BPM time vector (default 1.015).
    
    Returns:
    --------
    BPM_df : pd.DataFrame
        Processed BPM orbit data, indexed by cycle.
    BPV_df : pd.DataFrame
        Vertical BPM signals.
    BPH_df : pd.DataFrame
        Horizontal BPM signals.
    t : np.ndarray
        Time vector for trigger samples.
    tt : np.ndarray
        Time vector for BPM samples.
    """
    bpm_sample_freq_Hz=100
    
    # Convert columns with 'positions' to arrays
    for col in nxcals_df.columns:
        if 'positions' in col:
            nxcals_df[col] = nxcals_df[col].apply(np.array)
    
    cycles = []
    my_ALPS = ['BPMALPS_1', 'BPMALPS_2', 'BPMALPS_3',
               'BPMALPS_4', 'BPMALPS_5', 'BPMALPS_6']
    
    for my_cycle in nxcals_df.index:
        my_dict = {}
        print(f'Processing cycle: {my_cycle}')
        
        for ALPS in my_ALPS:
            pos_data = nxcals_df.loc[my_cycle, f'{ALPS}:Orbit:positions']
            reshaped_pos = np.reshape(np.transpose(pos_data[0]), pos_data[1])
            
            channel_names = nxcals_df.loc[my_cycle, f'{ALPS}:Orbit:channelNames'][0]
            for nn, channelName in enumerate(channel_names):
                print(f'Channel: {channelName}, nn: {nn}')
                my_dict[channelName] = reshaped_pos[nn, :]
        
        my_dict['cyclestamp'] = my_cycle
        cycles.append(my_dict)
    
    # Create DataFrame from processed cycles
    BPM_df = pd.DataFrame(cycles).set_index('cyclestamp')
    BPM_df.index.name = None
    
    # Filter by vertical and horizontal BPMs
    # BPV_df = BPM_df.filter(like='BPV', axis=1)
    # BPH_df = BPM_df.filter(like='BPH', axis=1)
    
    # Time vectors
    
    bpm_samples = len(BPM_df.iloc[0][BPM_df.columns[0]])
    tt = np.linspace(0 + ctime_inj_s, bpm_samples / bpm_sample_freq_Hz + ctime_inj_s, bpm_samples, endpoint=False)
    
    return BPM_df, tt


def column_rms(col):
    stacked = np.stack(col.values)  
    return np.std(stacked, axis=0)