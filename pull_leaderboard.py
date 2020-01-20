## Pull weekly performance in the 538 NFL forecasting competition ##
import requests
import time
import random
import pandas as pd
import numpy


## all data will be stored here ##
## three files will be created ##
## Weekly scores ##
## Seasonal Leaderboard ##
## Combined weekly and leaderboard ##

## output where everything goes ##
data_folder = '/YOUR PATH'

## 538s id codes arent persistent across season ##
## create a dictionary containing manually coded id's ##
unique_entrant_ids = {
    'ec7316ae' : '538',
    '6032a90d' : '@greereNFL',
}

## Pull weekly scores ##
## establish existing data ##
try:
    most_recent_df = pd.read_csv(
        '{0}/weekly_538_competition_data.csv'.format(data_folder), index_col=0
    )
    most_recent_season = most_recent_df['season'].max()
    most_recent_week = most_recent_df[most_recent_df['season'] == most_recent_season]['week'].max()
    print('Found existing weekly data')
    print('Pulling from most recent week')
except:
    most_recent_df = None
    most_recent_season = 2018
    most_recent_week = 1
    print('Did not find existing weekly data')
    print('Pulling from begining')

## a temporary container for the data before joining back ##
temp_df = None

## pull data begining at the current week ##
## stop when you hit a 404 ##
keep_looping = True
while keep_looping:
    try:
        ## sleep to be nice to servers ##
        time.sleep((.75 + random.random() * 2))
        print('Pulling week {0}, {1} results...'.format(
            most_recent_week, most_recent_season
        ))
        ## read the json directly from 538's api ##
        data_pull_df = pd.read_json(
            'https://projects.fivethirtyeight.com/nfl-api/{0}/leaderboard_week_{1}.json'.format(
                most_recent_season, most_recent_week
            )
        )
        ## add season, week, and current time ##
        data_pull_df['season'] = most_recent_season
        data_pull_df['week'] = most_recent_week
        data_pull_df['update_time'] = time.time()
        data_pull_df = data_pull_df[[
            'update_time', 'season', 'week', 'code', 'name', 'points', 'rank', 'percentile'
        ]]
        ## export to the temp container ##
        if temp_df is None:
            temp_df = data_pull_df
        else:
            temp_df = pd.concat([
                temp_df, data_pull_df
            ])
        ## progress the loop ##
        if most_recent_week == 18:
            most_recent_season += 1
            most_recent_week = 1
        else:
            most_recent_week += 1
    except:
        ## stop loop if pull failed ##
        keep_looping = False

## if the most recent data contains the current week, drop it to replace with new ##
new_data_max_season = temp_df['season'].max()
new_data_max_season = temp_df['season'].max()

## combine the existing data and the new data ##
if most_recent_df is None:
    ## if no existing data, use new ##
    most_recent_df = temp_df
else:
    ## if the most recent data contains the new pull's starting week, ##
    ## drop it to replace with new ##
    most_recent_df_season_max = most_recent_df['season'].max()
    most_recent_df_week_max = most_recent_df[
        most_recent_df['season'] == most_recent_df_season_max
    ]['week'].max()
    most_recent_df = most_recent_df[~(
        (most_recent_df['season'] == most_recent_df_season_max) &
        (most_recent_df['week'] == most_recent_df_week_max)
    )]
    ## then combine ##
    most_recent_df = pd.concat([most_recent_df, temp_df])

## remove duplicates ##
most_recent_df = most_recent_df.drop_duplicates()

## export weekly file ##
most_recent_df.to_csv('{0}/weekly_538_competition_data.csv'.format(data_folder))


## Pull seasonal leaderboards ##
## this isn't as much data, so will lazily just rebuild from scratch ##

leaderboard_df = None
print('Pulling season leaderboards')
for year in range(
    most_recent_df['season'].min(),
    most_recent_df['season'].max() + 1
):
    temp_seasonal_df = pd.read_json(
        'https://projects.fivethirtyeight.com/nfl-api/{0}/leaderboard.json'.format(year)
    )
    temp_seasonal_df['season'] = year
    if leaderboard_df is None:
        leaderboard_df = temp_seasonal_df
    else:
        leaderboard_df = pd.concat([leaderboard_df,temp_seasonal_df], sort=True)

## export ##
leaderboard_df.to_csv('{0}/538_competition_leaderboard.csv'.format(data_folder))


## create a combined leaderboard and weekly score file ##
## when a week is skipped, the entrant won't have a score ##

## create a frame with all weeks ##
week_df = most_recent_df.copy()[['season','week']].drop_duplicates()
## create a frame with all entrants for all weeks if they played that year ##
entrant_df = pd.merge(
    week_df,
    most_recent_df.copy()[['season','code','name']].drop_duplicates(),
    on=['season'],
    how='left'
)

## join seasonal leaderboard info ##
entrant_df = pd.merge(
    entrant_df,
    leaderboard_df.rename(columns={
        'rank' : 'seasonal_rank',
        'percentile' : 'seasonal_percentile',
        'points' : 'seasonal_points',
    }),
    on=['season','code', 'name'],
    how='left'
)

## add weekly info ##
entrant_df = pd.merge(
    entrant_df,
    most_recent_df.rename(columns={
        'rank' : 'weekly_rank',
        'percentile' : 'weekly_percentile',
        'points' : 'weekly_points',
    }),
    on=['season','code','name','week'],
    how='left'
)

## add zeros for skipped weeks ##
entrant_df['weekly_points'] = entrant_df['weekly_points'].fillna(0)

## sort ##
entrant_df = entrant_df.sort_values(by=['season','week','seasonal_points'])

## add seasonal cumsum ##
entrant_df['seasonal_cumulative_points'] = entrant_df.groupby(['code','season'])['weekly_points'].transform(pd.Series.cumsum)

## add persistent identifiers ##
def add_id(row):
    try:
        row['unique_id'] = unique_entrant_ids[row['code']]
    except:
        row['unique_id'] = numpy.nan
    return row

entrant_df = entrant_df.apply(add_id, axis=1)

## output headers ##
headers = [
    'season',
    'week',
    'code',
    'name',
    'unique_id',
    'weekly_points',
    'weekly_rank',
    'weekly_percentile',
    'seasonal_cumulative_points',
    'seasonal_points',
    'seasonal_rank',
    'seasonal_percentile'
]
entrant_df = entrant_df[headers]

## export all ##
entrant_df.to_csv('{0}/all_entrants.csv'.format(data_folder))

## seperate tracked only ##
tracked_entrant_df = entrant_df.copy()
tracked_entrant_df = tracked_entrant_df.dropna()
tracked_entrant_df = tracked_entrant_df.sort_values(by=['unique_id','season','week'])

## add cumulative count ##
tracked_entrant_df['all_time_cumulative_points'] = tracked_entrant_df.groupby(['unique_id'])['weekly_points'].transform(pd.Series.cumsum)

## export ##
tracked_entrant_df.to_csv('{0}/tracked_entrants.csv'.format(data_folder))
