import pandas as pd
import numpy as np


def get_range_dates_and_temps(cities, start_date, end_date, min_temp=-20, max_temp=40):
    date_range = pd.date_range(start_date, end_date).tolist()
    range_temps = pd.DataFrame()
    for city in cities:
        # TODO: make temperature look more realistic
        range_temps = range_temps.append(pd.DataFrame(data={'city': np.repeat(city, len(date_range)),
                                                            'date': date_range,
                                                            'temperature': np.random.uniform(min_temp, max_temp,
                                                                                             len(date_range)).round(1)}),
                                         ignore_index=True)
    range_temps = range_temps.sort_values('date')
    return range_temps
