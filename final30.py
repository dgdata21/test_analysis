'''
# Цели исследования

В ходе исследования необходимо составить рекомендательную систему,
позволяющую рекомендовать потребителю к основному выбранному продукту,
два дополнительных, потенциально интересные ввиду их популярности.
'''

import pandas as pd
import numpy as np
import itertools
import psycopg2 as ps2
import psycopg2.extras
from collections import Counter
import matplotlib.pyplot as plt
import datetime
from prettytable import PrettyTable
import warnings

warnings.filterwarnings('ignore')

'''**********************************************************************************'''


def connect():
    connection = ps2.connect(dbname='xxxxxxxx', user='xxxxxxxxxxx',
                             host='xxx.xxx.xxx.xxx', password='xxxxxxxxxxxxxxxx')
    return connection


def sql(x):
    cur.execute(x)
    rec = cur.fetchall()
    data = []
    for i in rec:
        data.append(dict(i))
    return data


'''**********************************************************************************'''
conn = connect()
cur = conn.cursor(cursor_factory=ps2.extras.DictCursor)


def get_schema():
    query = '''
select *
from information_schema.tables
where table_schema = 'final'
'''
    return sql(query)


df_data = pd.DataFrame(get_schema())
conn.close()
'''**********************************************************************************'''
conn = connect()
cur = conn.cursor(cursor_factory=ps2.extras.DictCursor)


def get_carts():
    query = '''
select *
from final.carts
'''
    return sql(query)


df_data = pd.DataFrame(get_carts())
conn.close()

'''
#### Описание полученного дата фрейма
Таблица carts состоит из 7 колонок:
- id - id транзакции
- created_at - дата регистрации пользователя
- updated_at - дата внесения каких-либо изменений
- purchased_at - дата покупки продукта
- state - состояние покупки
- user_id - id пользователя
- promo_code_id - id промо-кода

Количество строк - 93113
'''

print('Максимальная дата исследуемого периода:', '\n', df_data['purchased_at'].max(), '\n')
print('Минимальная дата исследуемого периода:', '\n', df_data['purchased_at'].min(), '\n')
'''**********************************************************************************'''
conn = connect()
cur = conn.cursor(cursor_factory=ps2.extras.DictCursor)


def get_cart_items():
    query = '''
select *
from final.cart_items
'''
    return sql(query)

'''
#### Описание полученного дата фрейма
Таблица cart_items содержит 6 колонок:
- id - id транзакции
- cart_id - ключевое поле для связи данной таблицы с таблицей carts
- created_at - дата создания продукта
- updated_at - дата внесения изменений(ввиду отсутствия дополнительной информации
в рамках данного исследования этот пункт будет расцениваться как модернизация продукта)
- resource_type - тип продукта
- resource_id - id продукта
'''


df_data = pd.DataFrame(get_cart_items())
uniq_course_list = df_data[df_data['resource_type'] == 'Course']['resource_id'].unique().tolist()
conn.close()
'''**********************************************************************************'''
conn = connect()
cur = conn.cursor(cursor_factory=ps2.extras.DictCursor)


def courses():
    query = '''
select *
from final.carts c
         join final.cart_items ci on c.id = ci.cart_id
where c.state='successful' and ci.resource_type='Course'
'''
    return sql(query)


df_data = pd.DataFrame(courses())
conn.close()

'''
В случае возникновения необходимости дальнейшего анализа
полученный дата фрейм можно выгрузить в виде файла .csv
'''
# df_data.to_csv('sales.csv')

'''**********************************************************************************'''
print('Общее количество клиентов ,которые покупали курсы:', '\n', df_data['user_id'].nunique(), '\n')
print('Количество уникальных курсов:', '\n', len(uniq_course_list), '\n')
print('Среднее количество купленных курсов на одного пользователя:', '\n',
      round((df_data['resource_id'].count() / len(df_data['user_id'].unique())), 2), '\n')

'''**********************************************************************************'''

'''
Так как необходимо выяснить какой курс рекомендовать в качестве парного, необходимо преобразовать SQL-запрос,
 чтобы в итоговый дата фрейм попали только клиенты, которые покупали не менее двух курсов
'''
conn = connect()
cur = conn.cursor(cursor_factory=ps2.extras.DictCursor)


def courses_two():
    query = '''
with a as
         (select c.user_id                      as uid,
                 count(distinct ci.resource_id) as cnt
          from final.carts c
                   join final.cart_items ci on c.id = ci.cart_id
          where c.state = 'successful'
            and ci.resource_type = 'Course'
          group by 1
          having count(distinct ci.resource_id) > 1
          order by 1)
select distinct a.uid as user_id,
                ci.resource_id
from a
         join final.carts c on c.user_id = a.uid
         join final.cart_items ci on c.id = ci.cart_id
where ci.resource_type = 'Course'
  and c.state = 'successful'
order by 1
'''
    return sql(query)


df_data = pd.DataFrame(courses_two())
conn.close()

'''
В случае возникновения необходимости дальнейшего анализа
полученный дата фрейм можно выгрузить в виде файла .csv
'''
# df_data.to_csv('sales.csv')

print('Количество пользователей, которые купили больше одного курса:', '\n', df_data['user_id'].nunique(), '\n')

'''**********************************************************************************'''

df_data = df_data.groupby(by='user_id')['resource_id'].apply(lambda x: x.tolist())
list_paires = list()
for i in df_data:
    for l in itertools.combinations(i, 2):
        list_paires.append(l)
list_paires = Counter(list_paires).most_common()
list_paires = dict(list_paires)
print('Количество различных пар курсов:', '\n', len(list_paires), '\n')

# Просмотр самой популярной пары курсов
df_data = pd.Series(list_paires).reset_index()
df_data.columns = ['first_course', 'second_course', 'quantity']
print('Самая популярная пара курсов:', '\n', df_data.head(1), '\n')

'''
В случае возникновения необходимости дальнейшего анализа
полученный дата фрейм можно выгрузить в виде файла .csv
'''
# df_data.to_csv('paires.csv')

'''*******************************************************************************************************'''


# Для начала необходимо создать функцию, через которую будет анализироваться созданный словарь
# с парами курсов и частотой покупки этих пар
def recommend(pair):
    # Содается пустой список
    rec_list = []
    # Через цикл перебираются ключи словаря list_paires
    for i in list_paires.keys():
        # В случае нахождения значения(пары курсов - частоты продажи),
        # оно добавляется в список в виде: ключ - пара курсов, значение - частота покупки
        if i[0] == pair:
            rec_list.append((i, list_paires[i]))
    # Далее необходимо отсортировать частоту продажи по убыванию
    sorted_rec_list = sorted(rec_list, key=lambda x: x[1], reverse=True)
    # В конечном итоге необходимо "срезать" только два курса
    return sorted_rec_list[:2]


# Создается дата фрейм, в который будут добавляться данные
df_data = pd.DataFrame(columns=['Course', 'first_recommendation', 'second_recommendation'])
# Через цикл происходит анализ списка, содержащего перечень уникальных курсов
# и в дата фрейм добавляется значение анализируемого курса,
# курса "первой рекомендации" и курса "второй рекомендации"
for i in uniq_course_list:
    if len(recommend(i)) == 2:
        df_data.loc[i] = [i, recommend(i)[0][0][1], recommend(i)[1][0][1]]
    elif len(recommend(i)) == 1:
        df_data.loc[i] = [i, recommend(i)[0][0][1], np.nan]
    else:
        df_data.loc[i] = [i, np.nan, np.nan]
df_data = df_data.sort_values(['Course']).reset_index(drop=True)

'''
В случае возникновения необходимости дальнейшего анализа
полученный дата фрейм можно выгрузить в виде файла .csv
'''
# df_data.to_csv('recommend.csv')

'''
### Описание полученного дата фрейма
Общее количество курсов 127 шт.
В первую рекомендацию попадает на десять курсов меньше - 117 шт.
Во вторую рекомендацию попадает уже на пятнадцать курсов меньше - 112 шт.
Таким образом необходимо проанализировать все курсы, чтобы восполнить "пробелы" по курсам:
772, 833, 902, 1147, 1152, 1160, 1181, 1182, 1184, 1187, 1188, 1198, 1199, 1200, 1201

Ввиду ограниченного набора данных, можно рассмотреть следующие критерии отбора курса для его рекомендации:
1. Как связаны количество внесенных в структуру курса изменений и количество его продаж.
2. Влияет ли "возраст", то есть как давно был "запущен в производство" курс, на объем его продаж.
3. Как часто вносятся изменения в структуру курса - иными словами насколько динамично меняется курс со временем,
что, несомненно отражает степень заинтересованности менеджмента в его продвижении.

Анализ указанных пунктов будет вестись в разрезе курсов, по которым отсутствую рекомендации с одной стороны
и по курсам, имеющим максимальный объем продаж.
Курсы, по которым отсутствуют рекомендации -- "слабые" курсы, их 15. Соответственно курсы, по которым
максимальные продажи, будут отобраны в таком же количестве -- "сильные" курсы.
Дополнительно будут выявлены лидеры продаж за анализируемый период в связке с их текущим положением на рынке.

По итогам анализа в рамках вышеуказанных трех пунктов необходимо отобрать курсы, наиболее подходящие для
рекомендаций по курсам 772, 833, 902, 1147, 1152, 1160, 1181, 1182, 1184, 1187, 1188, 1198, 1199, 1200, 1201.

4. После отбора подходящих курсов следует проанализировать их "жизненный путь", то есть на каком этапе
находится курс - в фазе роста его продаж, в фазе стабилизации или в фазе устойчивого снижения его продаж.
'''

'''*******************************************************************************************************'''

# Создание SQL-запроса
conn = connect()
cur = conn.cursor(cursor_factory=ps2.extras.DictCursor)


def get_course_data():
    query = '''
with c as
         (with a as
                   (select ci.resource_id        as resource_id,
                           min(ci.created_at)    as date_min,
                           max(c.purchased_at)   as date_max,
                           count(ci.updated_at)  as updates,
                           count(c.purchased_at) as qty
                    from final.carts c
                             join final.cart_items ci on c.id = ci.cart_id
                    where ci.resource_type = 'Course'
                        and ci.resource_id = 772
                       or ci.resource_id = 833
                       or ci.resource_id = 902
                       or ci.resource_id = 1147
                       or ci.resource_id = 1152
                       or ci.resource_id = 1160
                       or ci.resource_id = 1181
                       or ci.resource_id = 1182
                       or ci.resource_id = 1184
                       or ci.resource_id = 1187
                       or ci.resource_id = 1188
                       or ci.resource_id = 1198
                       or ci.resource_id = 1199
                       or ci.resource_id = 1200
                       or ci.resource_id = 1201
                    group by ci.resource_id
                    order by 5 asc
--                     limit 25)
                   )
          select a.resource_id,
                 a.qty,
                 a.date_min,
                 a.date_max,
                 a.updates,
                 row_number() over (order by a.qty) as nm
          from a),
     d as
         (with b as
                   (select ci.resource_id        as resource_id,
                           min(ci.created_at)    as date_min,
                           max(c.purchased_at)   as date_max,
                           count(ci.updated_at)  as updates,
                           count(c.purchased_at) as qty
                    from final.carts c
                             join final.cart_items ci on c.id = ci.cart_id
                    where ci.resource_type = 'Course'
                    group by ci.resource_id
                    order by 5 desc
--                     limit 25
                   )
          select b.resource_id,
                 b.qty,
                 b.date_min,
                 b.date_max,
                 b.updates,
                 row_number() over () as nm
          from b)
select c.resource_id    as "id курса(small)",
       c.qty            as "Продажи(small)",
       c.date_min::date as "Дата запуска(small)",
       c.date_max::date as "Последняя продажа(small)",
       c.updates        as "Кол-во измен-й(small)",
       d.resource_id    as "id курса(big)",
       d.qty            as "Продажи(big)",
       d.date_min::date as "Дата запуска(big)",
       d.updates        as "Кол-во измен-й(big)",
       d.date_max::date as "Последняя продажа(big)"
from c
         join d on c.nm = d.nm
    '''
    return sql(query)


df_main = pd.DataFrame(get_course_data())
conn.close()

# Преобразование полученного дата фрейма
df_main['Дата запуска(small)'] = pd.to_datetime(df_main['Дата запуска(small)'])
df_main['Последняя продажа(small)'] = pd.to_datetime(df_main['Последняя продажа(small)'])
df_main['Дата запуска(big)'] = pd.to_datetime(df_main['Дата запуска(big)'])
df_main['Последняя продажа(big)'] = pd.to_datetime(df_main['Последняя продажа(big)'])

df_main['life_time_small'] = (
        df_main['Последняя продажа(small)'] - df_main['Дата запуска(small)']).dt.days
df_main['life_time_big'] = (
        df_main['Последняя продажа(big)'] - df_main['Дата запуска(big)']).dt.days

df_main['rate_small'] = df_main['Кол-во измен-й(small)'] / df_main['life_time_small']
df_main['rate_big'] = df_main['Кол-во измен-й(big)'] / df_main['life_time_big']

'''
В случае возникновения необходимости дальнейшего анализа
полученный дата фрейм можно выгрузить в виде файла .csv
'''
# df_data.to_csv('nan_courses.csv')

'''*********************************************************************************************************'''

fig, ax = plt.subplots(1, 2, figsize=(19.2, 10.8), gridspec_kw={'width_ratios': [1, 1]})
fig.tight_layout(pad=10, w_pad=15)
fig.suptitle('Связь количества изменений с объемом продаж', fontsize=25, fontweight='bold', y=1)
df_main[['id курса(small)', 'Продажи(small)',
         'Кол-во измен-й(small)']].plot(ax=ax[0],
                                        kind='bar',
                                        rot=90,
                                        x='id курса(small)',
                                        fontsize=12,
                                        secondary_y='Кол-во измен-й(small)',
                                        color=('tomato', 'olive'))
df_main[['id курса(big)', 'Продажи(big)',
         'Кол-во измен-й(big)']].plot(ax=ax[1],
                                      kind='bar',
                                      rot=90,
                                      x='id курса(big)',
                                      fontsize=12,
                                      secondary_y='Кол-во измен-й(big)',
                                      color=('slateblue', 'darkorange'))
ax[0].set_title('20% минимально \n продаваемых курсов',
                fontsize=20,
                fontweight='bold',
                fontstyle='italic',
                pad=20)
ax[0].set_xlabel('Номер курса\n\n График 1', fontsize=15)
ax[0].right_ax.set_ylabel('Кол-во измен-й(small)', fontsize=15, color='olive')
ax[0].set_ylabel('Продажи(small)', fontsize=15, color='tomato')
ax[0].grid(True)

ax[1].set_title('20% максимально \n продаваемых курсов',
                fontsize=20,
                fontweight='bold',
                fontstyle='italic',
                pad=20)
ax[1].set_xlabel('Номер курса\n\n График 2', fontsize=15)
ax[1].right_ax.set_ylabel('Кол-во измен-й(big)', fontsize=15, color='darkorange')
ax[1].set_ylabel('Продажи(big)', fontsize=15, color='slateblue')
ax[1].grid(True)
plt.show()

'''
##### Выводы
В рамках этого пункта "слабые" курсы будут условно разделены на две группы. Отнесение к группе выполнено
на основании показателя объема продаж совместно с количеством внесенных изменений:
- у курса очень малые продажи и количество внесенных изменений также мало - "перспективы неясные"
- количество внесенных изменений равнозначно увеличению объема продаж - "перспективный"

Для курсов первой группы как в качестве первой рекомендации так и второй следует выбирать, несомненно, "лидеров".

Для группы "перспективный"  - в качестве первой рекомендации следует выбирать, "лидеров"
В качестве второй - курсы, следующие за "лидерами"

Итак,
772 -- перспективы неясные ------ рекомендуется 551 и 566
833	-- перспективы неясные ------ рекомендуется 551 и 566
902	-- перспективы неясные ------ рекомендуется 551 и 566
1147 -- перспективный ------ рекомендуется 551,566 и 490,794
1152 -- перспективный ------ рекомендуется 551,566 и 490,794
1160 -- перспективный ------ рекомендуется 551,566 и 490,794
1181 -- перспективный ------ рекомендуется 551,566 и 490,794
1182 -- перспективы неясные ------ рекомендуется 551 и 566
1184 -- перспективный ------ рекомендуется 551, 566 и 490,794
1187 -- перспективный ------ рекомендуется 551, 566 и 490,794
1188 -- перспективный ------ рекомендуется 551, 566 и 490,794
1198 -- перспективы неясные ------ рекомендуется 551 и 566
1199 -- перспективы неясные ------ рекомендуется 551 и 566
1200 -- перспективы неясные ------ рекомендуется 551 и 566
1201 -- перспективы неясные ------ рекомендуется 551 и 566
'''

'''*****************************************************************************************'''

fig, ax = plt.subplots(1, 2, figsize=(19.2, 10.8))
fig.tight_layout(pad=10, w_pad=15)
fig.suptitle('Связь периода существования курса с объемом продаж', fontsize=25, fontweight='bold', y=1)
df_main[['id курса(small)', 'life_time_small', 'Продажи(small)']].plot(ax=ax[0],
                                                                       kind='bar',
                                                                       rot=90,
                                                                       x='id курса(small)',
                                                                       secondary_y='Продажи(small)',
                                                                       color=('crimson', 'dodgerblue'))
df_main[['id курса(big)', 'life_time_big', 'Продажи(big)']].plot(ax=ax[1],
                                                                 kind='bar',
                                                                 rot=90,
                                                                 x='id курса(big)',
                                                                 secondary_y='Продажи(big)',
                                                                 color=('g', 'r'))
ax[0].set_title('20% минимально \n продаваемых курсов',
                fontweight='bold',
                fontsize=18,
                pad=20)
ax[1].set_title('20% максимально \n продаваемых курсов',
                fontweight='bold',
                fontsize=18,
                pad=20)
ax[0].set_ylabel('Период существования курса, дн.', fontweight='bold', color='crimson', fontsize=15)
ax[0].set_xlabel('Номер курса\n\n График 3', fontsize=15)
ax[0].right_ax.set_ylabel('Количество продаж, шт.', fontweight='bold', color='dodgerblue', fontsize=15)
ax[0].grid(True)
ax[0].legend(loc=6, fontsize=12, framealpha=1)
ax[0].right_ax.legend(loc=5, fontsize=12, framealpha=1)

ax[1].set_ylabel('Период существования курса, дн.', fontweight='bold', color='g', fontsize=15)
ax[1].set_xlabel('Номер курса\n\n График 4', fontsize=15)
ax[1].right_ax.set_ylabel('Количество продаж, шт.', fontweight='bold', color='r', fontsize=15)
ax[1].grid(True)
ax[1].legend(loc=6, fontsize=12, framealpha=1)
ax[1].right_ax.legend(loc=5, fontsize=12, framealpha=1)
plt.show()

'''
##### Выводы
В рамках данного пункта по объему продаж "слабые" курсы также делятся на две условных группы.
Однако, в части "срока жизни" картина несколько иная. Курсы 1187, 1188 и 1184 предлагаются клиентам
около 60 дней, а объем продаж в разы выше, чем у курсов 902, 833, 1147 и 1152, которые
предлагаются клиентам более 100 дней.
Напрашиваются два вывода:
- с течением временем менеджмент лучше понимает, чем заинтересовать потенциального потребителя;
В группе сильных курсов картина иная. У курсов с почти одинаковым сроком существования объем продаж
может колебаться в двукратном диапазоне -- как, например, у курса 490 и 502.
Но безусловными фаворитами для рекомендаций являются курсы 551 и 566. У них сопоставимый срок предстваленности,
а объем продаж существенно превышает аналогичный показатель по другим курсам из "сильной" группы.

- менеджмент по каким-то причинам не желает выяснять причину малых продаж по курсам, которые давно представлены,
но не приносят желаемых результатов.

Для всех "слабых" курсов как первой, так и второй рекомендацией будет пара 551,566
'''

'''***********************************************************************************************'''

fig, ax = plt.subplots(1, 2, figsize=(19.2, 10.8))
fig.tight_layout(pad=10, w_pad=15)
fig.suptitle('Динамика внесения изменений в курсы за период существования', fontsize=25, fontweight='bold', y=1)
df_main[['id курса(small)', 'life_time_small', 'rate_small']].plot(ax=ax[0],
                                                                   kind='bar',
                                                                   rot=90,
                                                                   x='id курса(small)',
                                                                   secondary_y='rate_small',
                                                                   color=('crimson', 'dodgerblue'))
df_main[['id курса(big)', 'life_time_big', 'rate_big']].plot(ax=ax[1],
                                                             kind='bar',
                                                             rot=90,
                                                             x='id курса(big)',
                                                             secondary_y='rate_big',
                                                             color=('g', 'r'))
ax[0].set_title('20% минимально \n продаваемых курсов',
                fontweight='bold',
                fontsize=18,
                pad=20)
ax[1].set_title('20% максимально \n продаваемых курсов',
                fontweight='bold',
                fontsize=18,
                pad=20)
ax[0].set_ylabel('Период существования курса, дн.', fontweight='bold', color='crimson', fontsize=15)
ax[0].set_xlabel('Номер курса\n\n График 5', fontsize=15)
ax[0].right_ax.set_ylabel('Коэффициент изменений', fontweight='bold', color='dodgerblue', fontsize=15)
ax[0].grid(True)
ax[0].legend(loc=6, fontsize=12, framealpha=1)
ax[0].right_ax.legend(loc=5, fontsize=12, framealpha=1)

ax[1].set_ylabel('Период существования курса, дн.', fontweight='bold', color='g', fontsize=15)
ax[1].set_xlabel('Номер курса\n\n График 6', fontsize=15)
ax[1].right_ax.set_ylabel('Коэффициент изменений', fontweight='bold', color='r', fontsize=15)
ax[1].grid(True)
ax[1].legend(loc=6, fontsize=12, framealpha=1)
ax[1].right_ax.legend(loc=5, fontsize=12, framealpha=1)
plt.show()

'''
##### Выводы
Курсы 1187, 1188, 1184 и 1152 являются наиболее динамично развивающимися. Они модернизируются чаще других.
Пожалуй, менеджмент упорно старается вывести их на более высокие показатели продаж, чем есть.
Курсам 902 и 833, являющимся "старожилами", наоборот уделяется очень мало внимания. По всей вероятности,
принято решение об их бесперспективности. Таким образом, в рамках данного пункта "слабые" курсы следует
разделить на две группы:
- у курса относительно высокий коэффициент изменений - "динамично развивающийся";
- у курса относительно низкий коэффициент изменений - "стагнирующий"

Для группы "динамично развивающийся" в качестве первой рекомендации стоит выделить явных лидеров -- 551,566.
В качестве второй рекомендации - 794,840.

Для группы "стагнирующий" только лидеры -- 551,566

772 -- стагнирующий ------ рекомендуется 551 и 566
833	-- стагнирующий ------ рекомендуется 551 и 566
902	-- стагнирующий ------ рекомендуется 551 и 566
1147 -- стагнирующий ------ рекомендуется 551,566
1152 -- динамично развивающийся ------ рекомендуется 551,566 и 840,794
1160 -- стагнирующий ------ рекомендуется 551,566
1181 -- стагнирующий ------ рекомендуется 551,566
1182 -- стагнирующий ------ рекомендуется 551 и 566
1184 -- динамично развивающийся ------ рекомендуется 551, 566 и 840,794
1187 -- динамично развивающийся ------ рекомендуется 551, 566 и 840,794
1188 -- динамично развивающийся ------ рекомендуется 551, 566 и 840,794
1198 -- стагнирующий ------ рекомендуется 551 и 566
1199 -- стагнирующий ------ рекомендуется 551 и 566
1200 -- стагнирующий ------ рекомендуется 551 и 566
1201 -- стагнирующий ------ рекомендуется 551 и 566
'''

'''****************************************************************************************'''

# Для анализа отбираются курсы, которые в предыдущих пунктах были указаны как рекомендуемые:
# 490, 551, 566, 794, 840

conn = connect()
cur = conn.cursor(cursor_factory=ps2.extras.DictCursor)


def get_most():
    query = '''
with a as
         (select ci.resource_id,
                 to_char(date_trunc('month', c.purchased_at), 'YYYY-MM-DD') as date,
                 count(c.purchased_at) as qty
--                      over (partition by ci.resource_id order by c.purchased_at)
          from final.carts c
                   join final.cart_items ci on c.id = ci.cart_id
          where c.state = 'successful'
            and ci.resource_type = 'Course'
          group by ci.resource_id, c.purchased_at)
select a.resource_id,
       a.date,
       count(a.qty)
from a
where a.resource_id = '551'
   or a.resource_id = '566'
   or a.resource_id = '490'
   or a.resource_id = '840'
   or a.resource_id = '794'
group by a.resource_id, a.date
order by 2
    '''
    return sql(query)


df_most = pd.DataFrame(get_most())

conn.close()

df_most['date'] = pd.to_datetime(df_most['date'])
df_most = df_most.pivot_table(index='date',
                              columns='resource_id',
                              values='count')

'''
В случае возникновения необходимости дальнейшего анализа
полученный дата фрейм можно выгрузить в виде файла .csv
'''
# df_data.to_csv('dinamic.csv')

df_most.plot(figsize=(19.2, 10.8), linewidth=3)
plt.yscale('log')
plt.xlabel('Дата', fontsize=18)
plt.ylabel('Количество продаж(log)', fontsize=18)
plt.title('Динамика продаж за исследуемый период', fontsize=25, fontweight='bold', pad=25)
plt.xticks(fontsize=12)
plt.show()

'''
##### Выводы
Из графика видно, что динамика для курса 551 ниспадающая.
Курсы 794 и 840 продаются только с середины анализируемого периода.
Устойчивую и долгосрочную динамику показывают только курсы 566 и 490.

## Выводы по рекомендациям
На основе проведенного анализа в рамках вышеобозначенных пунктов для курсов, у которых отсутствуют
обе рекомендации, в качестве первой рекомендации следует указать курс 566, в качестве второй 490.

Для курсов, у которых отсуствует вторая рекомендация однозначный вывод сделать сложно, не обладая
информацией о тематике как анализируемого, так и рекомендуемого курсов.
Однако, исходя из стремления менеджмента компании развивать несколько направлений, а не одно,
наиболее привлекательным является курс 490
'''

'''****************************************************************************************************'''

# Составление окончательной таблицы рекомендаций df_final
df_final = pd.DataFrame(columns=['Course', 'first_recommendation', 'second_recommendation'])
for i in uniq_course_list:
    if len(recommend(i)) == 2:
        df_final.loc[i] = [i, recommend(i)[0][0][1], recommend(i)[1][0][1]]
    elif len(recommend(i)) == 1:
        df_final.loc[i] = [i, recommend(i)[0][0][1], np.nan]
    else:
        df_final.loc[i] = [i, np.nan, np.nan]
df_final = df_final.sort_values(['Course']).reset_index(drop=True)

# Первая рекомендация
df_final.loc[((df_final['Course'] == 772) |
              (df_final['Course'] == 833) |
              (df_final['Course'] == 902) |
              (df_final['Course'] == 1160) |
              (df_final['Course'] == 1184) |
              (df_final['Course'] == 1188) |
              (df_final['Course'] == 1198) |
              (df_final['Course'] == 1199) |
              (df_final['Course'] == 1200) |
              (df_final['Course'] == 1201)), 'first_recommendation'] = 566
# Вторая рекомендация
df_final.loc[((df_final['Course'] == 772) |
              (df_final['Course'] == 833) |
              (df_final['Course'] == 902) |
              (df_final['Course'] == 1147) |
              (df_final['Course'] == 1152) |
              (df_final['Course'] == 1160) |
              (df_final['Course'] == 1181) |
              (df_final['Course'] == 1182) |
              (df_final['Course'] == 1184) |
              (df_final['Course'] == 1187) |
              (df_final['Course'] == 1188) |
              (df_final['Course'] == 1198) |
              (df_final['Course'] == 1199) |
              (df_final['Course'] == 1200) |
              (df_final['Course'] == 1201)), 'second_recommendation'] = 490

with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(df_final)

'''
В случае возникновения необходимости дальнейшего анализа
полученный дата фрейм можно выгрузить в виде файла .csv
'''
# df_data.to_csv('final.csv')
'''************************************ FIN *******************************************'''
