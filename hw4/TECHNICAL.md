# HW4. Контракт `hw4-serving-v1`

HW4 продолжает проект HW3. Сохраняются `postgres`, `airflow`, `spark`,
`mlflow`, `seaweedfs`, данные и артефакты HW3. Добавляются онлайн-хранилище
признаков на Redis, загрузчик признаков и три сервиса применения модели.

## 1. Что сдаётся

Один репозиторий, ветка `main`, последняя строка сообщения коммита —
ровно `MLSD-HW: HW4`.

```text
hw4/
  compose.yaml
  service/
    gateway/
    feature-service/
    model-service/
  offline/
    feature-build/
    feature-loader/
  config/models.json
  requirements.lock
  README.md
```

## 2. Сервисы

Обязательны и поднимаются одним `docker compose up`:

| Сервис | Назначение | Порт |
|---|---|---|
| `postgres` | состояние активной версии | внутри |
| `airflow-init` | инициализация Airflow (одноразово) | — |
| `airflow-webserver` | Airflow UI/API | внутри |
| `airflow-scheduler` | планировщик | внутри |
| `spark-master` | Spark | внутри |
| `spark-worker` | Spark | внутри |
| `seaweedfs` | Parquet и артефакты | внутри |
| `mlflow` | модели | внутри |
| `redis` | онлайн-хранилище признаков | внутри |
| `gateway` | публичный вход | `8080` |
| `feature-service` | выдача признаков | `8081` |
| `model-service` | применение модели | `8082` |
| `feature-build` | сборка признаков (одноразово) | — |
| `feature-loader` | загрузка признаков в Redis (одноразово) | — |

Наружу публикуется только `gateway:8080`. Остальные сервисы доступны
внутри сети проекта. `feature-build` и `feature-loader` завершаются с
кодом 0; `gateway` стартует после успешной сборки и загрузки.

Офлайн-сборка признаков выполняется в Airflow+Spark, как в HW3; результат —
Parquet в `seaweedfs`.

## 3. Вход

Курс кладёт в каталог `HW4_INPUT_DIR`:

- `history.csv.gz` — история в схеме HW3: `invoice_id`, `stock_code`,
  `description`, `quantity`, `invoice_at`, `unit_price`, `customer_id`,
  `country`;
- `request.json` — снимки `as_of`, период наблюдения и SHA-256 входа.

Проект читает оба файла и не изменяет их.

## 4. Признаки

Восемь признаков на пару `client_id` + `as_of`. Окно 90 дней —
`[as_of − 90 дней, as_of)`, короткое окно 30 дней — `[as_of − 30 дней, as_of)`.
Покупка — строка с `quantity > 0` и `unit_price > 0`; отмена — `invoice_id`,
начинающийся с `C`.

| Признак | Тип | Правило |
|---|---|---|
| `order_count_90d` | int | число покупок за 90 дней |
| `total_amount_90d` | decimal(20,6) | сумма `quantity × unit_price` за 90 дней |
| `average_order_amount_90d` | decimal(20,2) | `total_amount_90d / order_count_90d`, half-even; `0.00` при нуле |
| `days_since_last_order` | int | календарных дней от последней покупки; `−1`, если покупок не было |
| `distinct_stock_codes_90d` | int | различных `stock_code` за 90 дней |
| `return_rate_90d` | decimal(10,6) | отмены / (покупки + отмены) за 90 дней; `0` при нулевом знаменателе |
| `order_count_30d` | int | число покупок за 30 дней |
| `total_amount_30d` | decimal(20,6) | сумма за 30 дней |

Популяция снимка — все клиенты, у которых есть хотя бы одно событие
(покупка или отмена) строго до `as_of`.

## 5. Офлайн-путь

`feature-build` собирает Parquet в `seaweedfs`:
`offline/features/features.parquet` — колонки `client_id`, `as_of` и восемь
признаков. `feature-loader` читает Parquet и пишет в Redis ключи
`hw4:features:<client_id>:<as_of>` со значением из восьми признаков.

## 6. Модели

В MLflow — ровно две модели: `hw4-logistic` и `hw4-boosting`. У каждой
версии артефакты `pipeline.joblib`, `schema.json`, `manifest.json`;
`schema.json` перечисляет восемь признаков в порядке применения,
`manifest.json` — файлы и их SHA-256.

`model-service` загружает модель по паре `model_name` + `model_version`.
Повторное обучение на запросе запрещено. Версии зависимостей для инференса —
из `hw4/versions.json`.

## 7. Активная версия

Состояние — в `postgres`, таблица `hw4.active_model`: `model_name`,
`model_version`, `activation_id`, `state_id`, `activated_at`.

`POST /admin/activate` идемпотентен по `activation_id`: повтор с тем же
`activation_id` возвращает тот же `state_id` и не создаёт второго изменения.
Состояние переживает перезапуск контейнеров с сохранённым томом.

## 8. Эндпоинты

### gateway, 8080

- `GET /health` → `200 {"status": "ok", "model_name": "...", "model_version": "..."}`;
  `503 {"status": "not_ready"}`, пока активная версия недоступна.
- `POST /predict` `{"client_id": "...", "as_of": "YYYY-MM-DD"}` →
  `200 {"client_id", "as_of", "probability", "model_name", "model_version", "feature_key"}`.
- `POST /predict/batch` `{"items": [...]}` → `200 {"results": [...]}`;
  порядок и длина совпадают с запросом, один `model_version` на пакет.
- `GET /model` → `200 {"model_name", "model_version", "activation_id", "state_id"}`.
- `POST /admin/activate` `{"model_name", "model_version", "activation_id"}` →
  `200 {"model_name", "model_version", "activation_id", "state_id"}`.

### feature-service, 8081

`GET /features?client_id=...&as_of=YYYY-MM-DD` →
`200 {"client_id", "as_of", "features": {...}}`; `404`, если ключа нет.

### model-service, 8082

`POST /predict` `{"model_name", "model_version", "features": {...}}` →
`200 {"probability"}`; `409`, если модель несовместима со схемой.

## 9. Ошибки

Единый формат: `{"error": {"code": "...", "message": "..."}}`.

| Код | HTTP | Когда |
|---|---:|---|
| `invalid_request` | 400 | нет поля, неверный тип или дата |
| `client_not_found` | 404 | клиента нет в истории |
| `feature_not_found` | 404 | нет ключа признаков на дату |
| `model_not_found` | 404 | нет модели или версии в MLflow |
| `incompatible_model` | 409 | схема модели не совпадает с признаками |
| `features_unavailable` | 503 | Redis или feature-service недоступны |
| `model_unavailable` | 503 | MLflow или model-service недоступны |
| `not_ready` | 503 | активная версия ещё не готова |

## 10. Ресурсы и время

4 vCPU, 16 GiB, 90 минут на сборку и полный путь. Полный путь — от
`docker compose up` до ответа `gateway`.

## 11. Сдача

Ветка `main`, последняя строка коммита `MLSD-HW: HW4`. Даты: выдача
9 ноября 2026, обычная оценка 16 ноября 18:00 МСК, мягкий дедлайн
23 ноября, жёсткий — 30 ноября.
