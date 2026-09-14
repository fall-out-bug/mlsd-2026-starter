# HW1 private repository template source

Этот README — точный минимальный исходник отдельного public GitHub template.
В нём нет решения, Dockerfile, lockfile, checker package, credentials или
course-wide files. Текущий статус template указан в
[`template-delivery.json`](https://github.com/fall-out-bug/mlsd-2026/blob/main/assignments/HW1/v1/template-delivery.json): пока там
`draft_unresolved`, actual link и неавторский pilot остаются `NOT_ASSESSED`.

После появления `verified` metadata создайте **новый private repository** только
через GitHub `Use this template`; не используйте fork, folder copy или course
checkout. Работайте в его ветке `main`, добавьте собственные `Dockerfile`,
deterministic dependency lock, `README.md` с командами `docker build`/
`docker run` и реализацию `GET /health`.

Затем следуйте [HW1 instructions](https://github.com/fall-out-bug/mlsd-2026/blob/main/assignments/HW1/v1/README.md):
marker commit, local mini-checker и `git push origin main` выполняются именно из
вашего private repository. Local `PASS` не является server receipt, grade,
human review или course release.
