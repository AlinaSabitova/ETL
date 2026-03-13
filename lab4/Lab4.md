# Задание 4.1. Построение ETL-пайплайна средствами Dask

# Цель 

Получить практические навыки работы с библиотекой Dask для построения базовых ETL-конвейеров (Extract, Transform, Load) при обработке больших массивов данных, не помещающихся в оперативную память. Изучить принципы «ленивых вычислений» (lazy evaluation), управление памятью и визуализацию ориентированных ациклических графов (DAG).

# Вариант индивидуального задания

| Вариант | Имя файла (Датасет для ETL) |
| :---: | :--- |
| **12** | `Austin, TX House Listings.zip` |

# Подготовка окружения

Установим библиотеки Dask и Graphviz (для визуализации графов):

```
!pip install "dask[complete]" graphviz
```

<img width="1713" height="690" alt="image" src="https://github.com/user-attachments/assets/6a2e53bc-fd1b-4c12-9f08-59fac82c379a" />

Импортируем все нужные библиотеки и инициализируем клиента - создадим 2 воркера, каждый из которых использует 2 потока:

```
import sys
import os
import pandas as pd
import dask.dataframe as dd
import dask.delayed as delayed
from dask.distributed import Client
from dask.diagnostics import ProgressBar

# Инициализация клиента Dask (Оптимизированные настройки без жесткого лимита памяти)
client = Client(n_workers=2, threads_per_worker=2, processes=True)
client
```

<img width="1328" height="718" alt="image" src="https://github.com/user-attachments/assets/d16ece45-3c61-440d-b616-1b134219cddc" />

<img width="1576" height="376" alt="image" src="https://github.com/user-attachments/assets/23b06710-057e-4cfd-8e6b-9768ba0ae7c6" />

Подключимся к гугл диску:

```
from google.colab import drive
drive.mount('/content/drive')
```

<img width="1781" height="138" alt="image" src="https://github.com/user-attachments/assets/7524eae7-53c5-4183-bb03-73f07fbf343d" />

Перейдем в нужную папку:

<img width="1826" height="218" alt="image" src="https://github.com/user-attachments/assets/a300affb-1a0f-4d0f-a0d4-9165a540f8f8" />

# Шаг 1. Extract (Извлечение данных)

Загрузим в csv файл с дата сетом в Dask Dataframe:

```
df = dd.read_csv('austinHousingData.csv',  dtype={'zipcode': 'object', 'latest_saledate': 'object', 'homeImage': 'object'})
df
```

<img width="1762" height="335" alt="image" src="https://github.com/user-attachments/assets/8b6e5093-abda-4cfe-908a-8eac53080133" />

# Шаг 2. Transform (Трансформация и очистка данных)

Проверим датасет на наличие пустых значений.

Сначала посчитаем пропущенные значения:

```
missing_values = df.isnull().sum()
missing_values
```

<img width="794" height="244" alt="image" src="https://github.com/user-attachments/assets/16f6e3b4-7b49-4b2e-8edc-7581b90b9f7f" />

Вычислим процент пропусков:

```
mysize = df.index.size
missing_count = ((missing_values / mysize) * 100)
missing_count
```

<img width="1008" height="250" alt="image" src="https://github.com/user-attachments/assets/0a329764-7a94-477a-9ffc-e72d92e8631d" />

Запустим реальные вычислений для агрегированной статистики

```
with ProgressBar():
  missing_count_percent = missing_count.compute()
missing_count_percent
```

<img width="726" height="729" alt="image" src="https://github.com/user-attachments/assets/8edfc3f4-5d3b-460d-a9e7-811ae4f97504" />

Видим, что в датасете полностью отсутствуют пропущенные значения

Теперь проверим поле zpid на уникальность значений. Сначала подсчитаем уникальные и неуникальные значения (построим графы вычислений):

```
unique_zpids = df['zpid'].nunique()
zpid_freq = df['zpid'].value_counts()

duplicate_rows = (zpid_freq - 1).sum()
```

Затем запустим вычисления:

```
with ProgressBar():
    n_unique = unique_zpids.compute()
    n_duplicate_rows = duplicate_rows.compute() 

print(f"Уникальных zpid: {n_unique}")
print(f"Дублированных строк: {n_duplicate_rows}")
```

<img width="1767" height="471" alt="image" src="https://github.com/user-attachments/assets/47a7ffde-0806-4ab9-808f-95a863566c8c" />

Видим, что все значения уникальны

Далее проверим для всех ли записей датасета есть соответствующая фотография:

Сначала создадим множество из имени файлов в папке с фото:

<img width="1103" height="197" alt="image" src="https://github.com/user-attachments/assets/440bcf1f-e1fb-4f49-b97a-06ca8b9160ef" />

Созадидим план вычислений:
```
# Функция для проверки
def has_photo(image_name):
    if pd.isna(image_name) or image_name == '':
        return False
    return image_name in available_images

# Создаем план вычислений
df['photo_exists'] = df['homeImage'].map(has_photo, meta=('photo_exists', 'bool'))
houses_with_photo = df['photo_exists'].sum()
total_houses = df.index.size
percent_with_photo = (houses_with_photo / total_houses) * 100
```

Запустим их:

<img width="1227" height="548" alt="image" src="https://github.com/user-attachments/assets/4af76a40-04ca-4bee-a29a-4e55a3f4aa51" />

Также видим, что дома без фото в нашем датасете отсутствуют. В таком случае просто уберем столбцы, которые нам не нужны для анализа:

```
cols_to_drop = ['numOfParkingFeatures', 'numOfWindowFeatures',
                'numOfPatioAndPorchFeatures', 'numOfSecurityFeatures',
                'numOfWaterfrontFeatures', 'numOfAccessibilityFeatures',
                'numOfPrimarySchools', 'numOfElementarySchools',
                'numOfMiddleSchools', 'numOfHighSchools',
                'avgSchoolDistance', 'avgSchoolSize', 'MedianStudentsPerTeacher']

df_cleaned = df.drop(columns=cols_to_drop)
```

# Шаг 3. Load (Загрузка / Сохранение результатов)

Сохраним очищенный Dask DataFrame обратно на диск в формате parquet

<img width="1771" height="152" alt="image" src="https://github.com/user-attachments/assets/42db1f91-2278-44a6-8098-8e1c73774a5b" />

Видим, что датасет действительно был загружен на диск:

<img width="1504" height="272" alt="image" src="https://github.com/user-attachments/assets/1651c65b-801e-4ecf-8f6f-d348f4421d29" />

# Шаг 4. Визуализация направленных ациклических графов (DAG)

 Используя декоратор dask.delayed, создадим логику из простых python-функций и визуализируем план выполнения планировщика.


## Простой граф

Сначала создадим простой граф, визуализирующий процесс подсчета стоимости всего жилого фонда в датасете. Зададим 3 простых python-функции, создадим отложенные объекты, запустим вычисления и получим граф

```
# Подсчет общего количества домов
def get_total_houses():
    return len(df)

# Подсчет средней цены
def get_avg_price():
    return df['latestPrice'].mean().compute()

# Вычисление стоимости всего жилого фонда
def calculate_total_value(total, avg):
    return total * avg

# Создание отложенных объектов
x = delayed(get_total_houses)()
y = delayed(get_avg_price)()
z = delayed(calculate_total_value)(x, y)

# Визуализация графа
z.visualize(filename='easy_dag.png')

from IPython.display import Image
display(Image('easy_dag.png'))

# Запуск вычислений и получение результата
result = z.compute()
```

<img width="416" height="705" alt="image" src="https://github.com/user-attachments/assets/b39dfdb4-9024-451b-b65a-ea576df6368b" />

## Сложный граф

Теперь создадим сложный граф, который отражает процесс проведения расчетов. Считаем данные о каждом доме по его ID, отбираем только дома дороже 500 тысяч долларов, считаем цену за кв. м. для отфильтрованных домов

```
# Cписок zpid домов
houses = [101, 102, 103, 104, 105]

# Слой 1: Загружаем каждый дом
def load_house(h):
    return f"дом_{h}"

layer1 = [delayed(load_house)(h) for h in houses]

# Слой 2: Проверяем цену
def filter_expensive(house):
    if house['price'] > 500000:
        return house
    return None

layer2 = [delayed(filter_expensive)(h) for h in layer1]

# Слой 3: Считаем площадь
def price_per_sqft(house):
    if house is None:
        return None
    house['price_per_sqft'] = round(house['price'] / house['sqft'], 2)
    return house

layer3 = [delayed(price_per_sqft)(h) for h in layer2]

total = delayed(list)(layer3)

# Визуализация
total.visualize(filename='difficult_dask.png')  
display(Image('difficult_dask.png'))  
```

<img width="1060" height="1112" alt="image" src="https://github.com/user-attachments/assets/8a29e42e-bce3-428f-844d-74748a03373a" />

# Анализ датасета

Создадим дашборд на основе датасета:

<img width="1745" height="1433" alt="image" src="/images/Без%20названия.png" />

1. График "Количество домов по десятилетиям постройки" показывает, что Остин начал застраиваться в 1900-х годах, но основной период застройки пришелся на 1980-2000-е годы. Примерно в 2010-х количество строящихся домов пошло на спад.
2. График "Количество продаж по месяцам" отражает, что большая часть продаж приходится на летние месяцы, далее наблюдается спад. Самые низкие продажи в январе и феврале.
3. График "Средняя цена по типам домов" показывает, что наибольшая цена на  vacant land - незастроенный земельный участок и single family - классический частный дом, рассчитанный на одну семью. Наименьшая цена на townhouse - таунхаус и condo - квартиры в многоквартирном доме.
4. График "Распределение домов по наличию красивого вида"  показывает процент домов с живописным видом и без, так же в легенде для каждого типа домов указана средняя цена.  Видим, что домов с красивым видом только около 22%, а их средняя цена значительно выше, практически на 150 тысяч долларов
