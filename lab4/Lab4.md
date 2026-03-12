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


# 
