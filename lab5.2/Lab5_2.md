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
