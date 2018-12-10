from lcm import LCM
import pandas as pd
LCM.get_new_lens = profile(LCM.get_new_lens)
LCM._inner = profile(LCM._inner)

#columns = ['song_id','user_id', 'artist_id', 'provider_id', 'ip']
#df = pd.read_csv('listen-20131115.log', names=columns).head(10000)
path = '/Users/remiadon/Downloads/retail.dat'
df = pd.read_csv(path, header=None, squeeze=True, error_bad_lines=False, nrows=int(1e3)).map(str.split)

lcm = LCM(min_supp=2, n_jobs=4, filter_fn=lambda e: e, return_tids=False)
lcm.add(df)

lcm.discover()
