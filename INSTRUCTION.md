## Структура проекта

- `docker-compose.yml`: Конфигурация для запуска всех сервисов (PostgreSQL, Spark, Jupyter, ClickHouse).
- `data/`: Каталог для исходных CSV-файлов (`MOCK_DATA_*.csv`).
- `pg_init_scripts/`: SQL-скрипты для инициализации PostgreSQL (создание `mock_data_raw` и загрузка данных).
- `spark_apps/`: PySpark-скрипты для ETL.
  - `etl_to_star_schema.py`: Трансформация из `mock_data_raw` в модель "звезда" в PostgreSQL.
  - `etl_to_reports.py`: Генерация отчетов и загрузка в ClickHouse.
  - `requirements.txt`: Python зависимости (опционально).
- `drivers/`: Каталог для JDBC-драйверов (PostgreSQL, ClickHouse).

## Запуск проекта

1.  **Сборка и запуск контейнеров:**
    ```bash
    docker-compose up -d
    ```
2.  **Доступ к сервисам:**
    *   **PostgreSQL:** `localhost:5433` (пользователь: `user`, пароль: `password`, база: `bigdata_store`)
    *   **Spark Master Web UI:** `http://localhost:8080`
    *   **JupyterLab:** `http://localhost:8889` (токен: `sparklab`)
    *   **ClickHouse (HTTP):** `http://localhost:8123` (пользователь: `default`, без пароля)
    *   **ClickHouse (CLI в контейнере):** `docker-compose exec clickhouse clickhouse-client`

## Выполнение Spark ETL заданий

Spark-скрипты можно запускать из JupyterLab.

1.  **Откройте JupyterLab:** Перейдите по адресу `http://localhost:8889` в браузере и введите токен `sparklab`.

2.  **Запустите ETL для создания "звезды" в PostgreSQL:**
    *   В JupyterLab перейдите в папку `work` (которая смонтирована из `spark_apps`).
    *   Откройте терминал в JupyterLab (File -> New -> Terminal).
    *   Выполните команду:
        ```bash
        spark-submit \
          --master spark://spark-master:7077 \
          --jars /home/jovyan/drivers/postgresql.jar \
          /home/jovyan/work/etl_to_star_schema.py
        ```
    *   Дождитесь завершения. После этого в PostgreSQL должны появиться таблицы измерений (`Dim*`) и фактов (`FactSales`).

3.  **Запустите ETL для создания отчетов в ClickHouse:**
    *   В том же терминале JupyterLab выполните команду:
        ```bash
        spark-submit \
          --master spark://spark-master:7077 \
          --jars /home/jovyan/drivers/postgresql.jar,/home/jovyan/drivers/clickhouse.jar \
          /home/jovyan/work/etl_to_reports.py
        ```
    *   Дождитесь завершения.

## Проверка результатов

1.  **PostgreSQL:**
    Через `psql` проверьте наличие и содержимое таблиц:
    `DimCustomers`, `DimSellers`, `DimSuppliers`, `DimStores`, `DimProductCategories`, `DimProductBrands`, `DimProductPetCategories`, `DimProducts`, `DimDate`, `FactSales`.

2.  **ClickHouse:**
    *   Через CLI: `docker-compose exec clickhouse clickhouse-client`


## Остановка проекта
```bash
docker-compose down -v # -v удаляет также тома
