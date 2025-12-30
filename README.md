# Быстрый старт (dbt)

Для запуска проекта требуется только:
- установленный dbt
- корректно настроенный файл `~/.dbt/profiles.yml`

---

## 1. Создание и активация виртуального окружения

Рекомендуется работать через виртуальное окружение Python.

```bash
python -m venv venv
source venv/bin/activate  # macOS / Linux
# venv\Scripts\activate   # Windows
```

## 2. Установка зависимостей

```bash
pip install -r requirements.txt
```

---

## 3. Настройка подключения к базе данных (profiles.yml)

dbt **не хранит настройки подключения в репозитории**.  
Подключение к БД задаётся через файл `profiles.yml`.

### Где должен лежать файл

- macOS / Linux: `~/.dbt/profiles.yml`
- Windows: `%USERPROFILE%\.dbt\profiles.yml`

Создайте директорию, если её нет:

```bash
mkdir -p ~/.dbt
```

### Содержимое profiles.yml (пример для PostgreSQL)

```yaml
agrotest:
  target: dev
  outputs:
    dev:
      type: postgres
      host: <DB_HOST>
      user: <DB_USER>
      password: <DB_PASSWORD>
      port: 5432
      dbname: <DB_NAME>
      schema: public
      threads: 4
```

Важно:
- имя профиля **agrotest** должно совпадать со значением `profile:` в `dbt_project.yml`

---

## 4. Проверка, что dbt видит проект и базу

Находясь **в корне репозитория** (где лежит папка `agrotest/`), выполните:

```bash
dbt debug
```

Команда должна успешно подтвердить:
- найден `dbt_project.yml`
- найден профиль `agrotest`
- подключение к базе данных работает

Если `dbt debug` не проходит — дальнейшие команды запускать нельзя.

---

## 5. Подготовка входных данных (CSV)

Перед запуском `dbt seed` необходимо разместить CSV-файлы в каталоге:

```
agrotest/seeds/smb_data
```

Все входные CSV-файлы должны быть скопированы **строго в эту директорию**.

---

## 6. Инициализация объектов проекта в БД и запуск моделей

После успешного выполнения `dbt debug` выполните команды **строго в указанном порядке**:

```bash
dbt run-operation init_db
dbt run-operation create_snps_parents

dbt seed --full-refresh

dbt run -s ods --vars '{"input_data_date":"<любая дата>"}'
```

Далее - два варианта исполнения. 
Либо последовательно по каждому слою - от ods до dm:

```bash

dbt run -s ods --vars '{"input_data_date":"<дата забора csv из источника>"}'
dbt run -s dds
dbt run -s cdm
dbt run -s dm
```

Либо запускаем cdm.org_year_profile модель вместе с ее upstream и downstream.

```bash

dbt run -s +cdm.org_year_profile+ --vars '{"input_data_date":"<дата забора csv из источника>"}'
```


---

## Примечания

- Проект уже инициализирован — достаточно склонировать репозиторий и настроить `profiles.yml`
- `init_db` и `create_snps_parents` — идемпотентные операции, безопасные для повторного запуска
- `input_data_date` задаёт логическую дату обработки данных для слоя ODS
- Порядок выполнения слоёв фиксирован: **ODS → DDS -> CDM -> DM**
