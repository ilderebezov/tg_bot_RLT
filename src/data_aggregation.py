from datetime import datetime, timedelta

import pandas as pd

group_dict = {"hour": "H", "day": "D", "week": "W", "month": "M"}


def aggregation(df_data_in: pd.DataFrame, group_type: str, day_from: str, day_upto: str) -> tuple:
    """
    data aggregation function
    :param df_data_in: DataFrame with work data
    :param group_type: aggregation group type
    :param day_from: aggregation from day
    :param day_upto: aggregation upto day
    :return: data in necessary view
    """
    df_work = df_data_in.groupby(pd.Grouper(key='dt',
                                            axis=0,
                                            freq=group_dict[group_type.lower()]))
    df_work = df_work["value"].sum()
    df_work.index = df_work.index.map(lambda x: x.isoformat())
    return df_work.tolist(), df_work.index.tolist()
