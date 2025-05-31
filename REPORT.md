# Лабораторная работа 2 / Apache Spark

### Запуск контейнеров

```bash
docker-compose up -d
```

Это запустит следующие контейнеры:
- PostgreSQL (порт 5432)
- Clickhouse (порты 8123, 9000)
- Spark Master (порт 7077)
- Spark Worker

### Инициализация PostgreSQL

```bash
# Создание схемы "звезда" в PostgreSQL
docker exec -i bigdata_database psql -U postgres -d bigdata < database_init/postgres_init.sql

```

### Загрузка "сырых" данных в PostgreSQL

Я делал через интерфейс DBeaver

### Создание схемы "Звезда" в PostgreSQL

```bash
docker exec spark-worker spark-submit --master spark://spark-master:7077 --jars /opt/spark-apps/jars/postgresql-42.6.0.jar /opt/spark-apps/etl_scripts/create_star_scheme.py
```

### Инициализация Clickhouse

```bash
docker exec -i clickhouse clickhouse-client --user custom_user --password pswd < database_init/clickhouse_init.sql
```

### Создание витрин в Clickhouse

ETL-процесс выполняется с помощью Apache Spark. Скрипт `create_marts.py` читает данные из PostgreSQL, выполняет необходимые преобразования и записывает результаты в Clickhouse.

```bash
docker exec spark-worker spark-submit --master spark://spark-master:7077 --jars /opt/spark-apps/jars/postgresql-42.6.0.jar,/opt/spark-apps/jars/clickhouse-jdbc-0.4.6.jar /opt/spark-apps/etl_scripts/create_marts.py
```

### Выполнение аналитических запросов (получение отчетов из Clickhouse)

Для выполнения запросов:
```bash
docker exec -i clickhouse clickhouse-client --user custom_user --password pswd < reports.sql
```
