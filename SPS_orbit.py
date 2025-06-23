# %% 
# /home/sterbini/2025_05_22_nxcals_test/miniconda/bin/python
import os
import getpass
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
username = getpass.getuser()
print(f'Assuming that your kerberos keytab is in the home folder, '
      f'its name is "{getpass.getuser()}.keytab" '
      f'and that your kerberos login is "{username}".')

logging.info('Executing the kinit')
os.system(f'kinit -f -r 5d -kt {os.path.expanduser("~")}/'+
          f'{getpass.getuser()}.keytab {getpass.getuser()}');

# %%
import json
import nx2pd as nx
import pandas as pd

from nxcals.spark_session_builder import get_or_create, Flavor

logging.info('Creating the spark instance')

# Here I am using YARN (to do compution on the cluster)
#spark = get_or_create(flavor=Flavor.YARN_SMALL, master='yarn')

# Here I am using the LOCAL (assuming that my data are small data,
# so the overhead of the YARN is not justified)
# WARNING: the very first time you need to use YARN
# spark = get_or_create(flavor=Flavor.LOCAL)

logging.info('Creating the spark instance')
spark = get_or_create(flavor=Flavor.LOCAL,
conf={'spark.driver.maxResultSize': '8g',
    'spark.executor.memory':'8g',
    'spark.driver.memory': '16g',
    'spark.executor.instances': '20',
    'spark.executor.cores': '2',
    })
sk  = nx.SparkIt(spark)
logging.info('Spark instance created.')

# %%
# Simplest approach
data_list = ["BPMALPS_1:Orbit:positions",
             "BPMALPS_2:Orbit:positions",
             "BPMALPS_3:Orbit:positions",
             "BPMALPS_4:Orbit:positions",
             "BPMALPS_5:Orbit:positions",
             "BPMALPS_6:Orbit:positions",
             "BPMALPS_1:Orbit:channelNames"
             "BPMALPS_2:Orbit:channelNames",
             "BPMALPS_3:Orbit:channelNames",
             "BPMALPS_4:Orbit:channelNames",
             "BPMALPS_5:Orbit:channelNames",
             "BPMALPS_6:Orbit:channelNames",]
t0 = pd.Timestamp('2025-06-23 19:40:00',tz="CET")
t1 = pd.Timestamp('2025-06-23 19:45:00',tz="CET")

# %%
aux = sk.get(t0, t1, data_list)
# %%
import numpy as np
for ii in data_list:
    if 'positions' in ii:
        print(ii)
        aux[ii] = aux[ii].apply(np.array)
        logging.info(f'Converted {ii} to numpy array')
# %%
aux['BPMALPS_6:Orbit:positions'].iloc[0][1]
# %%
