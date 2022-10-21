import pandas as pd
from operator import attrgetter

# import

df = pd.read_csv('user_log.csv')
df['date'] = pd.to_datetime(df['date'])

df_cohort_visit = df[df['event_type'] == 'visit'][['uid', 'date']].drop_duplicates()
df_cohort_visit['cohort'] = df_cohort_visit.groupby('uid')['date'].transform('min').dt.to_period('W')
df_cohort_visit['date'] = df_cohort_visit['date'].dt.to_period('W')

df_cohort_visit['period'] = (df_cohort_visit['date'] - df_cohort_visit['cohort']).apply(attrgetter('n'))

df_cohort_visit = df_cohort_visit.groupby(['cohort', 'date', 'period']).agg(users=('uid', 'nunique')).reset_index()

df_cohort_visit = df_cohort_visit.pivot_table(index='cohort',
                                              columns='period',
                                              values='users')

df_cohort_visit_retention = round((df_cohort_visit.divide(df_cohort_visit.iloc[:, 0], axis=0) * 100), 1).fillna('')
df_cohort_visit = df_cohort_visit.fillna('')
concat_visit = pd.concat([df_cohort_visit, df_cohort_visit_retention], axis=0)
