import datetime as dt
import sys
import tempfile
from typing import Union

import WindPy
import pandas as pd


class WindWrapper(object):
    def __init__(self):
        self._w = None

    def connect(self):
        with tempfile.TemporaryFile(mode='w') as log_file:
            out = sys.stdout
            sys.stdout = log_file
            self._w = WindPy.w
            self._w.start()
            sys.stdout = out

    def disconnect(self):
        if self._w:
            self._w.close()

    def isconnected(self):
        return self._w.isconnected()

    @staticmethod
    def _api_error(api_data):
        if isinstance(api_data, tuple):
            error_code = api_data[0]
            data = api_data[1]
            has_data = data.values.any()
        else:
            error_code = api_data.ErrorCode
            data = api_data.Data
            has_data = any(data)

        if error_code != 0 or (not has_data):
            raise ValueError("Failed to get data, ErrorCode: {}".format(error_code))

    @staticmethod
    def _standardize_date(date: Union[None, str, dt.date, dt.datetime] = None):
        if not date:
            date = dt.date.today()
        if isinstance(date, (dt.date, dt.datetime)):
            date = date.strftime('%Y-%m-%d')
        return date

    # wrap functions
    def wsd(self, *args, **kwargs) -> pd.DataFrame:
        data = self._w.wsd(*args, **kwargs)
        self._api_error(data)
        df = pd.DataFrame(data.Data, index=data.Codes, columns=data.Times).T
        return df

    def wss(self, *args, **kwargs):
        return self._w.wss(*args, **kwargs)

    def wset(self, *args, **kwargs) -> pd.DataFrame:
        data = self._w.wset(*args, **kwargs)
        self._api_error(data)
        index_val = []
        if 'date' in data.Fields:
            index_val.append('date')
        if 'wind_code' in data.Fields:
            index_val.append('wind_code')
        df = pd.DataFrame(data.Data).T
        df.columns = data.Fields
        if index_val:
            df.set_index(index_val, drop=True, inplace=True)
        return df

    def tdays(self, *args, **kwargs):
        return self._w.tdays(*args, **kwargs)

    def tdaysoffset(self, *args, **kwargs):
        return self._w.tdaysoffset(*args, **kwargs)

    # outright functions
    def get_index_constitute(self, date: Union[None, str, dt.date, dt.datetime] = None,
                             index: str = '000300.SH') -> pd.DataFrame:
        date = self._standardize_date(date)
        data = self.wset("indexconstituent", date=date, windcode=index)
        return data


if __name__ == "__main__":
    wind = WindWrapper()
    wind.connect()
    hs300_constitute = wind.get_index_constitute(index='000300.SH')
    print(hs300_constitute)
    rnd_close = wind.wsd(["000001.SZ", "00002.SZ"], "close", "2019-06-11", "2019-07-10", "")
    print(rnd_close)
