import time
import pandas as pd
import numpy as np


class DataProcessing:
    def __init__(self, data, interval, start_time, end_time):
        self.data = data
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time

    @staticmethod
    def aggregation_functions(x):
        d = {}
        total_count = x['count'].sum()  # m+n

        if x.columns.str.contains('max').any():
            d['max'] = x['max'].max()
        if x.columns.str.contains('min').any():
            d['min'] = x['min'].min()
        if x.columns.str.contains('avg').any():
            # if there are two groups and  n, m are the count and x,y are the mean of each group, the formula for
            # combined mean for the two group is (nx + my)/ (n+m)
            d['avg'] = 0
            total_avg = (x['count'] * x['avg']).sum()  # nx + my
            if total_count != 0:
                d['avg'] = total_avg / total_count
        if x.columns.str.contains('std').any():
            # The formula for combined standard deviation is s^2 = ((n-1)sx^2 + (m-1)sy^2)/(n+m-1) + nm(x-y)^2/(n+m)(
            # n+m-1) where sx and sy are standard deviation of each group
            if total_count <= len(x.index):  # if all the rows of the grouped data have 0 or 1 elements
                d['std'] = 0
            else:
                prev_count = 0
                prev_std = 0
                prev_avg = 0
                std = 0
                for index, row in x.iterrows():
                    if index == 0:
                        prev_std = row['std']
                        prev_count = row['count']
                        prev_avg = row['avg']
                    else:
                        term1 = ((prev_count - 1) * np.square(prev_std)) + ((row['count'] - 1) * np.square(row['std']))
                        term2 = prev_count * row['count']  # nm
                        term3 = np.square(prev_avg - row['avg'])  # (x-y)^2
                        term4 = prev_count + row['count']
                        term5 = term4 - 1

                        if term5 > 0:
                            std = np.sqrt((term1 / term5) + ((term2 * term3) / (term5 * term4)))
                            avg = ((prev_avg * prev_count) + (row['count'] * row['avg'])) / term4

                            prev_std = std
                            prev_count = prev_count + row['count']
                            prev_avg = avg

                d['std'] = std

        d['count'] = int(total_count)
        s = pd.Series(d)
        return s

    @property
    def get_aggregated_data(self):
        print("Getting aggregated data")
        start_time = time.time()
        try:
            df = pd.DataFrame()
            for i in self.data:
                content = self.data[i]
                temp_df = pd.json_normalize(content, sep="/")
                temp_df.insert(0, 'ts', int(i))
                df = pd.concat([df, temp_df], ignore_index=True)

            # only take the subset for a time window
            filter_df = df.loc[(df['ts'] >= self.start_time) & (df['ts'] <= self.end_time)].set_index('ts')

            temp_dict = {str(k): {tuple(k1.split('/')): v1 for k1, v1 in v.items()} for k, v in
                         filter_df.to_dict('index').items()}

            tuples = []
            for k, v in temp_dict.items():
                for tuple_k, value in v.items():
                    if len(tuple_k) == 5:
                        tuples.append((k, tuple_k[0], tuple_k[1], tuple_k[2], tuple_k[3], tuple_k[4], value))
                    else:
                        tuples.append((k, tuple_k[0], tuple_k[1], tuple_k[2], tuple_k[3], 'count', value))

            new_df = pd.DataFrame(tuples, columns=['ts', 'type', 'row', 'col', 'status_code', 'stats', 'value'])
            new_df.insert(column='date_time', loc=1, value=pd.to_datetime(new_df['ts'], unit='ms'))
            new_df['date_time'] = new_df['date_time'].dt.tz_localize('utc').dt.tz_convert('Canada/Eastern')
            new_df.reset_index()

            pivot_df = new_df.pivot(index=['date_time', 'type', 'row', 'col', 'status_code'],
                                    columns='stats',
                                    values='value')
            pivot_df = pivot_df.reset_index(level=[1, 2, 3, 4]).rename_axis([None], axis='columns')

            pivot_df.fillna(0, inplace=True)

            if pivot_df.columns.str.contains('std').any():
                # setting std as 0 if count= 1
                pivot_df.loc[pivot_df['count'] == 1, 'std'] = 0

            # pd.Grouper works faster than resample and group by
            agg_df = pivot_df.groupby([
                pd.Grouper(freq=f'{self.interval}min', origin="end"),
                'type', 'row', 'col', 'status_code'
            ])

            agg_df = agg_df.apply(self.aggregation_functions).reset_index(level=[0, 1, 2, 3, 4])
            print("Execution time for aggregation---%s seconds---" % (time.time() - start_time))

            # change new date-time to unix timestamps(milliseconds)
            ts_series = agg_df['date_time'].values.astype(np.int64) // 10 ** 6
            agg_df.insert(loc=1, column='ts', value=ts_series)

            # drop date_time column and setting timestamp as an index
            agg_df = agg_df.drop(['date_time'], axis=1)

            return agg_df

        except Exception as e:
            print(e)
