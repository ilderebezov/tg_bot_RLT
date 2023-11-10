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
    df_work = fill_empty_data(df_work=df_work, day_from=day_from, day_upto=day_upto, group_type=group_type)
    df_work.index = df_work.index.map(lambda x: x.isoformat())
    return df_work.tolist(), df_work.index.tolist()


def fill_empty_data(df_work: pd.Series, day_from: str, day_upto: str, group_type: str) -> pd.Series:
    """
     fill empty data
    :param df_work: init pd.DataFrame
    :param day_from: correct from day
    :param day_upto: correct upto day
    :param group_type: aggregation group type
    :return: data without missed data
    """
    if group_type.lower() == "month":
        df_work.index = df_work.index.map(lambda x: x.replace(day=1))
        index_list = df_work.index.tolist()
        index_list[0] = datetime.fromisoformat(day_from)
        df_work = df_work.reindex(index=index_list)
    elif group_type.lower() == "day":
        delta = datetime.fromisoformat(day_upto) - datetime.fromisoformat(day_from)
        days_lst = [(datetime.fromisoformat(day_from) + timedelta(days=day)) for day in range(delta.days + 1)]
        df_work = pd.Series({date_i: (0 if df_work.T.get(date_i) is None else df_work.T.get(date_i))
                             for date_i in days_lst})
    elif group_type.lower() == "hour":
        time_range_lst = pd.date_range(day_from, day_upto, freq='H')
        df_work = pd.Series({date_time: (0 if df_work.T.get(date_time) is None else df_work.T.get(date_time))
                             for date_time in time_range_lst})
    elif group_type.lower() == "week":
        week_lst = [day_week + timedelta(days=-day_week.weekday())
                    if day_week < datetime.fromisoformat(day_from) else datetime.fromisoformat(day_from)
                    for day_week in df_work.index.tolist()]
        df_work.index = week_lst
    return df_work

