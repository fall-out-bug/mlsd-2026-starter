# Как добавить HW4 в репозиторий HW1–HW3

HW4 продолжает ваш проект HW3: `postgres`, `mlflow`, `seaweedfs` и история
остаются основой. Скопируйте каталог `hw4/` и обновлённый `check.sh` из
starter. Сохраните свои `hw1/`, `hw2/`, `hw3/`, корневой README и зависимости.

Что делаете сами, по контракту из [TECHNICAL.md](TECHNICAL.md):

1. Соберите `hw4/compose.yaml` с обязательными сервисами: `gateway`,
   `feature-service`, `model-service`, `feature-build`, `feature-loader`,
   `redis`, `postgres`, `mlflow`, `seaweedfs`.
2. Реализуйте расчёт восьми признаков и раскладку в Redis.
3. Реализуйте три сервиса применения и таблицу активной версии в Postgres.
4. Обучите и зарегистрируйте в MLflow две модели: `hw4-logistic` и
   `hw4-boosting`.
5. Зафиксируйте `requirements.lock` и опишите запуск в `hw4/README.md`.

Добавьте в корневой README ссылку на HW4. Проверьте прежние работы командами
`sh check.sh HW1`, `sh check.sh HW2`, `sh check.sh HW3`, затем запустите
`sh check.sh HW4`. Стартер не содержит проекта HW4: несобранный проект
получает учебный отказ `compose_missing`.

Сроки HW4: обычная оценка 16 ноября 2026 года в 18:00 МСК, мягкий дедлайн
23 ноября, жёсткий — 30 ноября.
