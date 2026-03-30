# Лабораторная работа работа 5.2 Разработка алгоритмов для трансформации данных. Airflow DAG

# Цель работы

1. Закрепить навыки развертывания Apache Airflow в контейнеризированной среде (Docker).
2. Изучить работу с JSON-данными и бинарным контентом (изображениями) внутри ETL-процесса.
3. Научиться проектировать архитектуру ETL-решений и визуализировать её.
4. Автоматизировать выгрузку результатов работы DAG из контейнера в хост-систему.

# Архитектура решения

# Архитектура аналитического решения

## 1. Верхнеуровневая архитектура

```mermaid
---
config:
  layout: elk
---
flowchart LR
    API((🚀 Launch Library 2 API))

    subgraph Docker[🐳 Docker]
        Airflow["⚙️ Airflow"]
        Jupyter["🧠 Jupyter"]
        Streamlit["📊 Streamlit"]
    end

    subgraph Host[💻 Ubuntu]
        Data[(📁 ./data)]
        Logs[(📁 ./logs)]
        Dags[(📁 ./dags)]
    end

    API -->|1. Запрос| Airflow
    Airflow -->|2. Сохраняет| Data
    Airflow -.->|Логи| Logs
    Airflow -.->|Читает| Dags
    Data -->|3. Фото| Jupyter
    Jupyter -->|4. Результаты| Data
    Data -->|5. Отчеты| Streamlit

    style Data fill:#fff3e0,stroke:#f57c00
    style Airflow fill:#ffe0b2,stroke:#fb8c00
    style Jupyter fill:#c8e6c9,stroke:#43a047
    style Streamlit fill:#b3e5fc,stroke:#03a9f4
```

```mermaid
---
config:
  layout: elk
---
flowchart LR
    API((🚀 Launch Library 2 API))

    subgraph Docker["🐳 Docker"]
        direction TB
        Airflow["⚙️ Airflow<br/>DAG: listing_sabitova_rocket"]
        Jupyter["🧠 Jupyter<br/>CLIP ML"]
        Streamlit["📊 Streamlit<br/>Dashboard"]
    end

    subgraph Host["💻 Ubuntu"]
        direction TB
        Data[(📁 ./data<br/>launches.json<br/>images/<br/>reports)]
        Dags[(📁 ./dags<br/>DAG файл)]
        Logs[(📁 ./logs)]
    end

    API -->|1. GET launches| Airflow
    Airflow -->|2. JSON + images| Data
    Airflow -.->|3. logs| Logs
    Dags -.->|4. read| Airflow
    Data -->|5. images| Jupyter
    Jupyter -->|6. ml_predictions.csv| Data
    Data -->|7. all reports| Streamlit

    style API fill:#ce93d8,stroke:#6a1b9a
    style Airflow fill:#ffcc80,stroke:#e65100
```
    style Jupyter fill:#a5d6a7,stroke:#2e7d32
    style Streamlit fill:#81d4fa,stroke:#01579b
    style Data fill:#ffe0b2,stroke:#f57c00
    style Dags fill:#ffcdd2,stroke:#c62828
    style Logs fill:#ffcdd2,stroke:#c62828
