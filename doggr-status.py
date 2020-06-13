import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import date, datetime, timedelta, time
import json
import os
from scipy import stats
from bson import json_util

# client = MongoClient(os.environ['MONGODB_CLIENT'])
client = MongoClient(
    'mongodb+srv://kk6gpv:kk6gpv@cluster0-kglzh.azure.mongodb.net/test?retryWrites=true&w=majority')
db = client.petroleum


class check_status:

    def get_prodinj(self):
        docs = db.doggr.find({'api': self.api})
        for x in docs:
            doc = dict(x)
        prodinj = pd.DataFrame(doc['prodinj'])
        self.prodinj = prodinj.sort_values(by='date')

    def calc_status(self):
        status = {}
        for col in ['air', 'cyclic', 'gas', 'gas_i', 'oil', 'steam', 'water', 'water_i']:
            try:
                stat = self.prodinj[self.prodinj[col] > 0]['date'].max()
                try:
                    np.isnan(stat)
                except:
                    status[col] = stat
            except:
                pass

        try:
            status['last_prod'] = max(
                status.keys(), key=(lambda k: status[k]))
            status['last_month'] = status[status['last_prod']]
            if status['last_month'] >= '2017-11-01T00:00:00':
                status['last_status'] = 'active'
            else:
                status['last_status'] = 'inactive'
        except:
            pass

        db.doggr.update_one({'api': self.api}, {
                            '$set': {'status_calc': status}}, upsert=False)
        print(self.api, ' written')

    def __init__(self, api):
        self.api = api
        self.get_prodinj()
        self.calc_status()


df_ = pd.DataFrame(
    list(
        db.doggr.find(
            {'oil_cum': {'$gt': 0}}, {'api': 1}
        )
    )
)
apis = df_['api'].values

print(len(apis))

for api in apis:
    print(str(api)+' started')
    try:
        check_status(str(api))
    except:
        print(str(api)+' failed')
