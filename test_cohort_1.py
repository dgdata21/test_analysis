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

# df_cohort_visit.to_csv('cohort.csv')
# df_cohort_visit_retention.to_csv('cohort_retention.csv')

# X = int(input())
# H = int(input())
# M = int(input())
# print(int((X//60) + (2*H)))
# print(int(M-5))

a = int(input())
b = int(input())
c = int(input())
d = int(input())
for i in range(c, d + 1):
    print('\t' + str(i), end='')
print(end='\n')
for s in range(a, b + 1):
    print(str(s) + '\t', end='')
    for j in range(c, d + 1):
        print(str(s * j), end='\t')
    print(end='\n')

a, b = input().split()
a = int(a)
b = int(b)
s = 0
for i in range(a, b + 1):
    if i % 2 == 1:
        s += i
print(s)

from statistics import mean
a = int(input())
b = int(input())
f = []
for i in range(a, b + 1):
    if i % 3 == 0:
        f.append(i)
print(mean(f))

s = 'acggtgttat'.lower()
b = s.count('g')
a = s.count('c')
d = b/len(s)*100
e = a/len(s)*100
print(d+e)

s = 'acggtgttat'
d = (s.count('g') + s.count('c'))/len(s)*100
print(d)

s = str(input())
l = len(s) - 1
c = 1
t = ''
if len(s) == 1:
    t = t + s + str(c)
else:
    for i in range(0, l):
        if s[i] == s[i + 1]:
            c += 1
        elif s[i] != s[i + 1]:
            t = t + s[i] + str(c)
            c = 1
    for j in range(l, l + 1):
        if s[-1] == s[-2]:
            t = t + s[j] + str(c)
        elif s[-1] != s[-2]:
            t = t + s[j] + str(c)
            c = 1
print(t)

import pandas as pd
df = pd.DataFrame({
    "Date": [
        "2015-05-08", "2015-05-07", "2015-05-06", "2015-05-05",
        "2015-05-08", "2015-05-07", "2015-05-06", "2015-05-05"],
    "Data": [5, 8, 6, 1, 50, 100, 60, 120],
})
print(df)

# df.groupby('Date')['Data'].transform('sum')
df = df.aggregate('mean')
