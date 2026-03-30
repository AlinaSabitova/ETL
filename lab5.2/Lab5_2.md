# Лабораторная работа работа 5.2 Разработка алгоритмов для трансформации данных. Airflow DAG

# Цель работы

1. Закрепить навыки развертывания Apache Airflow в контейнеризированной среде (Docker).
2. Изучить работу с JSON-данными и бинарным контентом (изображениями) внутри ETL-процесса.
3. Научиться проектировать архитектуру ETL-решений и визуализировать её.
4. Автоматизировать выгрузку результатов работы DAG из контейнера в хост-систему.

# Архитектура решения

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
        Data[(📁 ./data)]
        Dags[(📁 ./dags<br/>DAG файл)]
        Logs[(📁 ./logs)]
    end

    API -->|1. Получение данных о запусках| Airflow
    Airflow -->|2. Сохранение JSON и фото| Data
    Airflow -.->|3. Запись логов| Logs
    Dags -.->|4. Чтение DAG файла| Airflow
    Data -->|5. Передача изображений| Jupyter
    Jupyter -->|6. Сохранение предсказаний| Data
    Data -->|7. Чтение всех отчетов| Streamlit

    style API fill:#ce93d8,stroke:#6a1b9a
    style Airflow fill:#ffcc80,stroke:#e65100
    style Jupyter fill:#a5d6a7,stroke:#2e7d32
    style Streamlit fill:#81d4fa,stroke:#01579b
    style Data fill:#ffe0b2,stroke:#f57c00
    style Dags fill:#ffcdd2,stroke:#c62828
    style Logs fill:#ffcdd2,stroke:#c62828
```

# Логика работы DAG

```mermaid
flowchart TD
    START([Start]) --> CLEAN[clean_data_directory<br/>Очистка папки data]

    CLEAN --> DOWNLOAD_JSON[download_launches<br/>Загрузка JSON из API]

    DOWNLOAD_JSON --> DOWNLOAD_IMAGES[download_pictures<br/>Скачивание фотографий<br/>⚠️ Уязвимость: нет timeout]

    DOWNLOAD_IMAGES --> REPORT[report_failed_images<br/>📸 Задание 1<br/>Отчет по незагруженным фото]

    REPORT --> ANALYZE[analyze_vulnerabilities<br/>🔒 Задание 3<br/>Анализ уязвимостей DAG]

    ANALYZE --> NOTIFY[notify<br/>Уведомление о завершении]

    NOTIFY --> END([End])

    SUCCESS[on_success_callback<br/>📈 Задание 2<br/>Мониторинг успешных запусков] -.->|При успехе DAG| DOWNLOAD_JSON
    
    FAILURE[on_failure_callback<br/>📈 Задание 2<br/>Логирование ошибок DAG] -.->|При ошибке DAG| DOWNLOAD_JSON

    style REPORT fill:#ffe6cc,stroke:#d79b00
    style ANALYZE fill:#f8cecc,stroke:#b85450
    style SUCCESS fill:#e1d5e7,stroke:#9673a6
    style FAILURE fill:#e1d5e7,stroke:#9673a6
    style DOWNLOAD_IMAGES fill:#fff3e0,stroke:#f57c00
```
