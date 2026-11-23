# HW5. Открытые сценарии и локальные прогоны

Этот файл — рабочая тетрадь по потоковой части `hw5-stream-v1`. Каждый класс
поведения из TECHNICAL.md §15 покрыт малым сценарием в `data/scenarios/<имя>.json`
с уже рассчитанными ожиданиями; независимые значения собраны в `data/expected.json`.
Читайте сценарий, предсказывайте результат **до** просмотра ожидания, затем
запускайте и сверяйтесь.

Локальный PASS (в том числе по `sh check.sh HW5`) не заменяет серверный зачёт.
Закрытая проверка меняет идентификаторы, суммы, даты и порядок доставки внутри
объявленных правил.

## Сценарии

| Файл | Класс (§15) | Что проверяется |
|---|---|---|
| `identity.json` | дубликаты и конфликты | повторная доставка, duplicate, conflict, новая строка с новым `event_id` |
| `aging.json` | агрегаты и границы окон; старение | выход строки/заказа из окна 90d, короткое окно 30d, `no_history` от пустого окна |
| `late.json` | опоздания | покупка с днём в прошлом, но `>= closed_until`; неизменность сохранённого прогноза |
| `closure.json` | закрытие | `closed_past`, необратимость, монотонность `/admin/time` |
| `future.json` | события из будущего | приём и хранение, вход в окна при достижении `now`; `unattributed`; непокупочная строка |
| `predict.json` | задания на прогноз | ok / no_history / повтор / конфликт / `invalid_request` / 404 |
| `model_call.json` | настоящий вызов модели | два различных набора признаков на одной версии, независимые вероятности, незнакомый клиент |
| `recovery.json` | восстановление | четыре точки паузы + SIGKILL + перезапуск без потерь и дублей |
| `model_unavailable.json` | недоступность модели | ok ждёт, no_history завершается, события обрабатываются, один результат после восстановления |
| `switch.json` | смена версии посреди потока | фиксация пары до/после активации, неизменность сохранённых прогнозов |
| `order.json` | независимость от порядка | один набор событий в прямом и обратном порядке даёт один результат |
| `drift.json` | мониторинг: сдвиг | предварительный эталон, заморозка, statistic=1.0, алерт critical |
| `labels.json` | зрелые метки и качество | незрелость при `closed_until < t+30`, границы `t` и `t+30`, log loss и ROC-AUC |
| `alerts.json` | алерты | `input_quality`, `event_conflict`, `stream_lag`, ack, 404 |
| `drift-events.json` | — | синтетика для `drift.json` (220 клиентов) |

Формат файла: `name`, `description`, `question` (вопрос самопроверки), `input`
(границы из `request.json`), `steps` — последовательность операций:

- `event` / `request` — `XADD` в `hw5:events` / `hw5:requests` с полем `payload`;
- `time` — `POST /admin/time` (ожидается 200); `time_reject` — ожидается 400;
- `activate` — `POST /admin/activate` на gateway;
- `pause` (`point`, для `after_receive` — `kind`), `resume`, `expect_pause`;
- `kill_restart` — SIGKILL `stream-worker` и перезапуск на тех же томах;
- `model_block` / `model_unblock` — остановка/запуск `model-service`;
- `reset` — чистая среда (`docker compose down -v && docker compose up -d --build --wait`);
- `wait_boundary` — ждать непрерывную границу обработки (§5); `wait_events` —
  границу только по событиям (полезно при заблокированной модели, когда
  ok-задание правомерно остаётся неподтверждённым);
- `expect_status` / `expect_prediction` / `expect_report` / `expect_alert` /
  `expect_rejection` / `expect_http` — проверяемые наблюдения;
- `note` — пояснение, действия не требует.

## Как подавать и наблюдать

Все команды выполняются из каталога `hw5/`. Наружу опубликован только
`gateway:8080`; внутренние ручки — через `docker compose exec`.

```sh
# чистая среда (обязательно перед сценарием с reset или перед первым прогоном)
docker compose down -v --remove-orphans
docker compose up -d --build --wait

# готовность групп: обе должны показать hw5-workers
docker compose exec redis redis-cli XINFO GROUPS hw5:events
docker compose exec redis redis-cli XINFO GROUPS hw5:requests

# подача события (одним полем payload)
docker compose exec redis redis-cli XADD hw5:events * payload \
  '{"event_id":"e-100","invoice_id":"I900","stock_code":"S1","description":"item","quantity":2,"invoice_at":"2011-01-10T10:00:00","unit_price":"10.000000","customer_id":"A","country":"GB"}'

# подача задания
docker compose exec redis redis-cli XADD hw5:requests * payload \
  '{"request_id":"r-id-1","client_id":"A","as_of":"2011-01-12"}'

# учебное время и статус (внутренняя ручка stream-worker:8083)
docker compose exec stream-worker python -c \
  "import json,urllib.request as u; print(u.urlopen(u.Request('http://127.0.0.1:8083/admin/time', data=json.dumps({'now':'2011-01-12','closed_until':'2011-01-01'}).encode(), headers={'Content-Type':'application/json'})).read().decode())"
docker compose exec stream-worker python -c \
  "import urllib.request as u; print(u.urlopen('http://127.0.0.1:8083/stream/status').read().decode())"

# журнал отказов и паузы
docker compose exec stream-worker python -c \
  "import urllib.request as u; print(u.urlopen('http://127.0.0.1:8083/stream/rejections?limit=10').read().decode())"
docker compose exec stream-worker python -c \
  "import json,urllib.request as u; print(u.urlopen(u.Request('http://127.0.0.1:8083/admin/pause', data=json.dumps({'point':'after_receive','kind':'event'}).encode(), headers={'Content-Type':'application/json'})).read().decode())"
docker compose exec stream-worker python -c \
  "import urllib.request as u; print(u.urlopen('http://127.0.0.1:8083/admin/pause').read().decode())"
docker compose exec stream-worker python -c \
  "import urllib.request as u; print(u.urlopen(u.Request('http://127.0.0.1:8083/admin/resume', method='POST', data=b'')).read().decode())"

# публичный вход: активация и чтение прогнозов
curl -s -X POST http://127.0.0.1:8080/admin/activate -H 'Content-Type: application/json' \
  -d '{"model_name":"hw4-logistic","model_version":"1","activation_id":"scen-identity-1"}'
curl -s http://127.0.0.1:8080/predictions/r-id-1

# монитор (внутренняя ручка monitor:8084)
docker compose exec monitor python -c \
  "import urllib.request as u; print(u.urlopen('http://127.0.0.1:8084/monitor/report').read().decode())"
docker compose exec monitor python -c \
  "import urllib.request as u; print(u.urlopen('http://127.0.0.1:8084/monitor/alerts').read().decode())"

# неподтверждённые записи и содержимое записи по ID
docker compose exec redis redis-cli XPENDING hw5:events hw5-workers
docker compose exec redis redis-cli XRANGE hw5:events <id> <id>

# отказ и восстановление worker'а
docker compose kill stream-worker && docker compose start stream-worker

# недоступность модели
docker compose stop model-service   # блокировка
docker compose start model-service  # восстановление
```

Граница обработки (§5) достигнута, когда `last_event_id` и `last_request_id`
не меньше ID последней поданной записи каждого потока **и** `events_pending = 0`
и `requests_pending = 0`. Один нулевой `XPENDING` границу не доказывает.
Задания на прогноз подавайте только после границы по событиям.

## Исходное состояние и ожидания по классам

Общее исходное состояние чистой среды: `now = closed_until = 2011-01-01`
(из `request.json`), потоки и группы создаёт `stream-worker`, активной версии
нет — задания `ok` ждут до активации. Ожидаемые значения ниже продублированы
в JSON сценариев; вероятности — в `data/expected.json` (допуск 1e-9).

### identity — дубликаты и конфликты

1. `time` → `now=2011-01-12`, `closed_until=2011-01-01`; активируйте модель.
2. Событие `e-100` (покупка A, 2×10.000000, день 2011-01-10) — после границы:
   `received_total=1`, `attributed_new=1`, `duplicates=0`, `conflicts=0`.
3. Новый `XADD` с тем же payload — `duplicate`: `received_total=2`,
   `duplicates=1`, `attributed_new=1` (состояние не меняется, сумма не растёт).
4. `e-100` с `quantity: 3` — `conflict`: `received_total=3`, `conflicts=1`;
   в `/stream/rejections` сверху запись `kind=event` с причиной конфликтного
   отказа (в журнале может называться `conflict` или детальнее, например
   `content_mismatch` — словарь причин не фиксирован контрактом).
5. `e-101` с тем же бизнес-содержимым — принимается: `received_total=4`,
   `attributed_new=2`. Итог: две строки одного заказа — сумма `40.000000`,
   а `order_count_90d = 1` (различные `invoice_id`, §3).
6. `r-id-1` (A, 2011-01-12) → `ok`, вероятность из `expected.json`.

Действие при расхождении: сравните счётчики с ожиданиями шага; удвоение суммы
на шаге 3 означает, что повтор не распознан по `event_id`; ноль `duplicates`
при росте `received_total` — та же причина. Смотрите
`docker compose logs stream-worker` и `XRANGE` обеих записей.

### aging — границы окон и старение

События: `I500` строка 1 (день 2011-01-05), `I501` (2011-01-06), `I500` строка 2
(2011-02-15, на момент подачи — будущее). Прогнозы A:

| as_of | исход | order_count_90d | total_amount_90d | окно 30d |
|---|---|---|---|---|
| 2011-02-01 | ok | 2 (I500, I501) | 40.000000 | 2 / 40.000000 |
| 2011-02-20 | ok | 2 | 80.000000 | 1 / 40.000000 |
| 2011-04-07 | ok | **1** (I500 ещё в окне последней строкой) | 40.000000 | 0 / 0.000000 |
| 2011-05-17 | **no_history** | — | — | — |

Ключевое различие: в 2011-04-07 обе строки `I501` (одна строка, день 01-06)
и первая строка `I500` (01-05) вышли из окна `[01-08, 04-07)`, но заказ `I500`
держится строкой от 02-15. В 2011-05-17 окно `[02-16, 05-17)` пусто — покупок
нет, исход `no_history`, признаки `null`.

### late — опоздания

`closed_until=2011-01-05`, `now=2011-02-01`. Прогноз `r-late-1` (A) по одной
покупке 2011-01-10: сумма 20.000000, 22 дня. Затем опоздавшая покупка `e-301`
(день 2011-01-20 < now, но `>= closed_until`) принимается: `attributed_new=2`,
`closed_past_rejected=0`. Новый `r-late-2` с тем же `as_of` видит обе покупки
(сумма 70.000000, 2 заказа, 12 дней). Повторное чтение `r-late-1` возвращает
сохранённый результат без изменений — поздние события не меняют сохранённые
прогнозы (§7).

### closure — закрытие истории

`closed_until=2011-01-10`: `e-350` (день 01-15) принят; `e-351` (день 01-08) —
`closed_past` (`received_total=2`, `closed_past_rejected=1`); `e-352` с новым
`event_id` и тем же днём — снова `closed_past` (`closed_past_rejected=2`):
закрытие необратимо, «протащить» событие новым идентификатором нельзя.
Монотонность: `now` назад, `closed_until > now` и `closed_until > observed_until`
(2011-04-01) → 400 `invalid_request`, время не меняется. Прогноз после
корректного продвижения (`now=2011-03-01`, `closed_until=2011-02-01`) видит
только принятые события.

### future — события из будущего

`e-400` (B, день 2011-02-15) при `now=2011-01-12` принят (`attributed_new=1`),
но `r-fut-1` (B, 2011-01-12) → `no_history`: событий строго до `as_of` нет.
Пустой `customer_id` (`e-401`) → `unattributed=1`; строка с `quantity: 0`
(`e-402`) принята, но в агрегаты не входит (`non_purchase_rows=1`). После
`now=2011-02-20` прогноз `r-fut-2` видит покупку: сумма 20.000000, 5 дней.

### predict — задания на прогноз

`r-p1` (A) → `ok`; `r-p2` (D, нет событий) → `no_history` со всеми `null`;
`predictions_written=2`. Повтор `r-p1` тем же payload — идемпотентно, второй
результат не создаётся. `r-p1` с другим `as_of` → `requests_conflict=1`
(проверка содержимого раньше правила `as_of == now`). Новый `r-p3` с
`as_of != now` → `requests_invalid`; `r-p4` без `client_id` → `requests_invalid`.
`GET /predictions/r-unknown` → 404 `prediction_not_found`. Сохранённый `r-p1`
после конфликтных подач не меняется.

### model_call — настоящий вызов модели

Одна фиксированная версия `hw4-logistic v1`, два различных набора признаков:
A (2 заказа, сумма 45.000000, среднее 22.50, 12 дней) и незнакомый клиент `N1`
(отсутствует в офлайн-снимке HW4; нет ключа `hw4:features:N1:*`). Оба задания
`ok`; вероятности различаются и совпадают с `expected.json` с допуском 1e-9.
Константная подмена вместо инференса (одна константа на версию) отсекается
именно этим сравнением: у A и N1 разные признаки — одинаковый ответ невозможен.

### recovery — четыре точки остановки

Каждый блок начинается с `reset` и активации. Общая схема: `pause` → подача
записи → `expect_pause` (в `GET /admin/pause` появился `paused_at`) →
наблюдение → `kill_restart` → граница → контроль отсутствия потерь и дублей.

| Точка | Что видно до kill | Что после перезапуска |
|---|---|---|
| `after_receive` (event) | запись в `XPENDING`, `received_total=0` | `received_total=1`, `duplicates=0`; признаки из одного применения |
| `after_state_write` (event) | запись в `XPENDING`, счётчики уже обновлены (`received_total=1`) | то же без удвоения; сумма ровно 20.000000 |
| `after_choice_write` (request) | выбор сохранён: `GET /predictions/r-rec3` → `{"status":"pending"}`, `requests_pending=1` | `ok` с зафиксированным выбором, `predictions_written=1` |
| `after_result_write_before_ack` | прогноз уже сохранён (полный JSON), задание не подтверждено | результат не дублируется, `predictions_written=1`, `requests_pending=0` |

Состояние `armed` паузы не переживает перезапуск — после рестарта worker
обрабатывает поток обычно.

### model_unavailable — недоступность модели

Активация до блокировки. `docker compose stop model-service`, затем подаются
`r-u1` (ok, нужен вызов) и `r-u2` (no_history). Ожидается: `r-u2` завершён
(результат сохранён), `r-u1` — `{"status":"pending"}`, `requests_pending=1`,
`predictions_written=1`; событие `e-u2` в это же время обрабатывается
(`attributed_new=2`, `events_pending=0`; граница по событиям, не по заданиям) —
worker не останавливается.
После `docker compose start model-service`: ровно один правильный результат
`r-u1`, `predictions_written=2`, `requests_pending=0`.

### switch — смена версии посреди потока

`r-s1` завершён на `hw4-logistic`. Затем: пауза `after_choice_write` → подача
`r-s2` — worker фиксирует признаки и пару (logistic) и останавливается до
вызова модели (`GET /predictions/r-s2` → `{"status":"pending"}`) →
`POST /admin/activate` `hw4-boosting` → `resume`. `r-s2` завершается
**logistic** с той же вероятностью, что `r-s1`: пара зафиксирована до
активации (§10). `r-s3` после активации — **boosting**. Сохранённый `r-s1`
не меняется.

Замечание о среде: серверная проверка блокирует только соединение
`stream-worker` → `model-service`; локальная остановка `model-service`
(`docker compose stop`) мешает и `gateway` → `model-service`, поэтому
`/admin/activate` в этот момент может не пройти — для сценария смены версии
достаточно штатной точки паузы `after_choice_write`.

### order — независимость от порядка

Пять событий (три покупки разных заказов, отмена, вторая строка первого
заказа) подаются в прямом и обратном порядке из чистой среды. После границы
обработки всего набора оба прогона дают одинаковые признаки
(3 заказа, 52.000000, среднее 17.33, 11 дней, 4 кода, `return_rate 0.250000`),
одинаковую вероятность и одинаковые счётчики. Перестановка внутри набора,
целиком обработанного до снимка, результата не меняет (§5).

### drift — сдвиг входов и заморозка эталона

Синтетика: 220 клиентов по одной покупке в `[2011-01-05, 2011-02-25]` плюс
клиенты M1/M2 (для зрелых меток контрпримера). Границы популяций: ≥200 клиентов
в каждом периоде для алерта.

1. После первых 120 клиентов: `drift.status=insufficient_data`,
   `reference.frozen=false`, `reference.n=122` (120 + M1/M2), алерта нет.
2. После всех 220: `reference.n=222`, `statistic=0.0`, статус `ok`, алерта нет —
   эталон ещё предварительный.
3. `time now=2011-06-01 closed_until=2011-03-01` — заморозка (`frozen=true`)
   и сдвиг старением: у всех 222 клиентов окно `[03-03, 06-01)` пусто,
   `order_count_90d=0` → интервалы `[1,0,0,0,0]` против эталонных
   `[0, 220/222, 2/222, 0, 0]`; `statistic=1.0 ≥ 0.20`, статус `drift`,
   алерт `drift` (critical, open).
4. Контрпример «сдвиг без зрелых меток»: `labels.matured=2` (только m-r1/m-r2,
   обе метки 1 — один класс), `quality.by_version.hw4-logistic.1`:
   `status=not_assessed`, `roc_auc=null`.

### labels — зрелость меток и качество

`t = 2011-01-30`, горизонт `[t, t+30) = [2011-01-30, 2011-03-01)`. Клиенты
L1 (покупка ровно в `t`), L2 (покупка ровно в `t+30`), L3 (без покупок);
базовые покупки 2011-01-15 дают всем трём `ok` с разными вероятностями.

1. `time now=2011-03-01 closed_until=2011-02-15`: `now ≥ t+30`, но
   `closed_until < t+30` → `labels.matured=0`, `labels.immature=3`,
   `quality.by_version={}` — прогнозы исключены из оценки, не обнулены.
2. `time now=2011-03-01 closed_until=2011-03-01`: горизонт закрыт →
   `matured=3`; метки: L1=1 (покупка в `t` входит), L2=0 (ровно в `t+30`
   не входит), L3=0. `quality`: `n=3`, `log_loss` и `roc_auc` из
   `expected.json` (клиппинг 1e-12, ничьи 0.5), статус `ok`.
3. Контрпример «качество без сдвига»: `drift.statistic=0.0`, алерта drift нет
   (текущее распределение совпадает с эталоном; статус `insufficient_data`
   из-за n=3 < 200 — алерт не открывается).

### alerts — алерты и подтверждения

1. Окно: 22 валидных + 2 `invalid_event` (нет поля `country`; `quantity` строкой)
   = 24 записи, из них 2 invalid (8.3% ≥ 5%). До закрытия окна отчёт показывает
   `since_last_advance.received=24`, `invalid_events=2`, но алерта нет —
   `input_quality` оценивается только успешным `POST /admin/time`, изменившим
   `now`/`closed_until`. После `time now=2011-02-02` — алерт `input_quality`
   (warning, open), окно обнулилось (`received=0`).
2. `event_conflict`: конфликт по `event_id` (счётчик `conflicts=1`) → алерт
   warning при очередной оценке (отчёт или продвижение).
3. `ack`: подтверждение возвращает объект со `status=acknowledged` и
   `acked_at`; повтор — тот же `acked_at`; неизвестный id → 404
   `alert_not_found`.
4. `stream_lag`: пауза `after_receive` + событие + ожидание ≥125 c —
   `oldest_pending_idle_seconds ≥ 120`, алерт warning (пауза не делает его
   ложным). После `resume` pending обнуляется.
5. Повторное открытие после ack: новый конфликт после подтверждения
   `event_conflict` создаёт ровно один новый алерт.

## Запуск из чистой среды и повторный запуск

- Чистая среда: `docker compose down -v --remove-orphans &&
  docker compose up -d --build --wait`, затем активация и сценарий.
  Начальные `now`/`closed_until` применяются только к чистой среде (§2).
- Повторный запуск **без** `-v` сохраняет тома: проверьте, что уцелели
  `now`/`closed_until`, идентичность событий, сохранённые прогнозы, алерты и
  их подтверждения, записи и группы Redis Streams. После `docker compose stop &&
  docker compose start` счётчики не меняются, повторная подача тех же payload
  даёт `duplicate`, а не второе применение.
- `sh check.sh HW5` всегда начинает с чистых томов и в конце оставляет тома
  текущего прогона для осмотра.

## Виды отказов и что делать

| Отказ | Наблюдение | Действие |
|---|---|---|
| `invalid_event` | запись в `/stream/rejections`, счётчик `invalid_events` | найдите запись по ID из журнала: `XRANGE hw5:events <id> <id>`; подайте корректное событие (новым `XADD`; тем же `event_id`, если он не был принят) |
| `conflict` | `conflicts`/`requests_conflict` растёт | новое содержимое требует новый `event_id`/новую подачу с корректировкой; старое состояние не менять |
| `closed_past` | `closed_past_rejected` растёт | необратимо в текущей среде: закрытие не откатывается; данные этого дня не войдут в состояние |
| потерянная запись | `XPENDING` не пуст после перезапуска | worker обязан забрать запись прежнего потребителя (`XAUTOCLAIM`/`XCLAIM`); проверьте журналы и повторите прогон |

Поиск исходной записи: ID записи находится в журнале отказов (`id`), содержимое
— `XRANGE` по этому ID. Терминальный исход фиксируется на запись потока:
повторная доставка той же записи не меняет счётчики; `duplicate` — только для
новой записи с уже известным `event_id`.

## Вопросы самопроверки и контрпримеры

К каждому сценарию вопрос записан в самом файле (`question`). Ориентиры
самопроверки для двух контрпримеров независимости сигналов (наблюдаемый факт /
недопустимый вывод / разумное действие):

**Сдвиг без зрелых меток** (прогон `drift.json`, шаг 4).

- Факт: алерт `drift` critical открыт, `statistic=1.0`, при этом
  `quality...status=not_assessed`, `roc_auc=null` — зрелых двухклассовых пар
  «вероятность — исход» нет.
- Недопустимый вывод: «качество модели упало, доказано алертом». Доли интервалов
  `order_count_90d` не содержат пар «сохранённая вероятность — фактический
  исход»; алерт не является ни доказательством падения качества, ни
  статистически значимым различием.
- Разумное действие: зафиксировать расхождение входов, дождаться зрелых меток
  (горизонт `[t, t+30)` закрывается продвижением `closed_until`) и только потом
  оценивать качество; параллельно искать причину сдвига в источнике данных.

**Качество на зрелом наборе без сдвига** (прогон `labels.json`, шаг 3).

- Факт: `quality` посчитана по трём зрелым наблюдениям (log loss, ROC-AUC),
  `drift.statistic=0.0`, drift-алерта нет.
- Недопустимый вывод: причинное сравнение версий или «модель подтверждена».
  Зрелый набор мал и одноконтекстен; разрезы по версиям описательные — версии
  могли обслуживать разные даты и клиентов.
- Разумное действие: доложить метрики с объёмом выборки и границами; при
  необходимости — накопить зрелые наблюдения; не отключать мониторинг сдвига.

Проверяйте понимание изменением условия, а не повтором команды: переставьте
порядок доставки, измените payload, сдвиньте дату, закройте историю раньше —
и предскажите результат до прогона.
