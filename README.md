# BigDataSpark — лабораторная работа №2 (Spark ETL)

Анализ больших данных: ETL-пайплайн на Apache Spark — загрузка сырых продаж в PostgreSQL, построение модели **«звезда»** в PostgreSQL и публикация **6 аналитических витрин** в **ClickHouse**.

## Что реализовано

| Компонент | Описание |
|-----------|----------|
| Исходные данные | 10 CSV в каталоге `исходные данные/` (по ~1000 строк, всего ~10 000) |
| `docker-compose.yml` | PostgreSQL, ClickHouse, Spark, автозагрузка CSV |
| ETL «звезда» | `spark/jobs/etl_star_schema.py` |
| Витрины ClickHouse | `spark/jobs/reports_clickhouse.py` (6 таблиц в БД `lab`) |

### Модель «звезда» (PostgreSQL, схема `dwh`)

- **Измерения:** `dim_customer`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier`, `dim_date`
- **Факт:** `fact_sales` (количество, сумма продажи, цена товара)

### Витрины в ClickHouse (схема `lab`)

1. `mart_product_sales` — продукты (топ-10, выручка по категориям, рейтинги)
2. `mart_customer_sales` — клиенты (топ-10, страны, средний чек)
3. `mart_time_sales` — время (месячные/годовые тренды, сравнение периодов, средний заказ)
4. `mart_store_sales` — магазины (топ-5, география, средний чек)
5. `mart_supplier_sales` — поставщики (топ-5, средняя цена, страны)
6. `mart_product_quality` — качество (рейтинги, корреляция, отзывы)

В каждой таблице поле `section` разделяет срезы одного отчёта (удобно проверять в SQL).

## Требования

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (или Docker Engine + Compose v2)
- ~4 ГБ свободной RAM для контейнеров
- (Опционально) [DBeaver](https://dbeaver.io/) для просмотра PostgreSQL и ClickHouse

## Быстрый запуск (для проверяющего)

Из корня репозитория:

**Windows (PowerShell):**

```powershell
.\scripts\run-pipeline.ps1
```

**Linux / macOS / Git Bash:**

```bash
chmod +x scripts/run-pipeline.sh
./scripts/run-pipeline.sh
```

Скрипт:

1. Поднимает PostgreSQL и ClickHouse
2. Создаёт таблицы ClickHouse (`clickhouse-init`)
3. Загружает 10 CSV в `staging.mock_data` (`data-loader`)
4. Собирает образ Spark и запускает ETL + витрины

### Ручной запуск по шагам

```bash
# 1. Инфраструктура и загрузка сырых данных
docker compose up -d postgres clickhouse
docker compose up clickhouse-init data-loader

# 2. Spark
docker compose build spark
docker compose up -d spark

# 3. ETL: mock_data -> звезда в PostgreSQL
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/postgresql-42.7.3.jar,/opt/spark/jars-extra/clickhouse-jdbc-0.6.5-all.jar \
  /opt/spark/work-dir/jobs/etl_star_schema.py

# 4. Отчёты: звезда -> ClickHouse
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/postgresql-42.7.3.jar,/opt/spark/jars-extra/clickhouse-jdbc-0.6.5-all.jar \
  /opt/spark/work-dir/jobs/reports_clickhouse.py
```

## Подключение к БД

| Сервис | Host | Port | User | Password | База |
|--------|------|------|------|----------|------|
| PostgreSQL | `localhost` | 5432 | `lab` | `lab` | `bigdata` |
| ClickHouse HTTP | `localhost` | 8123 | `default` | *(пусто)* | `lab` |

Полезные схемы PostgreSQL: `staging` (сырые данные), `dwh` (звезда).

## Проверка результата

**PostgreSQL** — файл `scripts/verify.sql` или:

```sql
SELECT COUNT(*) FROM staging.mock_data;   -- ожидается ~10000
SELECT COUNT(*) FROM dwh.fact_sales;      -- ожидается ~10000
```

**ClickHouse** — файл `scripts/verify-clickhouse.sql` или:

```sql
SELECT section, count() FROM lab.mart_product_sales GROUP BY section;
```

Через CLI:

```bash
docker compose exec postgres psql -U lab -d bigdata -c "SELECT COUNT(*) FROM dwh.fact_sales;"
docker compose exec clickhouse clickhouse-client -q "SELECT section, count() FROM lab.mart_product_sales GROUP BY section"
```

## Структура репозитория

```
├── docker-compose.yml          # PostgreSQL, ClickHouse, Spark, init-сервисы
├── исходные данные/            # MOCK_DATA*.csv (10 файлов)
├── scripts/
│   ├── init-postgres.sql       # staging + dwh DDL
│   ├── init-clickhouse.sql     # DDL витрин
│   ├── load_mock_data.py       # загрузка CSV в PostgreSQL
│   ├── run-pipeline.ps1        # полный прогон (Windows)
│   ├── run-pipeline.sh         # полный прогон (Linux/macOS)
│   ├── verify.sql
│   └── verify-clickhouse.sql
└── spark/
    ├── Dockerfile
    ├── config.py               # JDBC-настройки
    └── jobs/
        ├── etl_star_schema.py
        └── reports_clickhouse.py
```

## Остановка окружения

```bash
docker compose down
# с удалением томов:
docker compose down -v
```

## Задание (из методички)

<details>
<summary>Оригинальное описание лабораторной</summary>

Необходимо реализовать ETL-пайплайн с помощью Spark, который трансформирует данные из источника (файлы mock_data.csv) в модель данных **звезда** в PostgreSQL, затем создать 6 отчётов в ClickHouse (обязательно). Опционально — Cassandra, Neo4j, MongoDB, Valkey (бонус).

Подробный список метрик по каждой витрине — в исходном README курса и на схеме задания.

</details>

## Автор

Форк репозитория лабораторной работы. Для проверки достаточно ссылки на GitHub и команды из раздела «Быстрый запуск».
