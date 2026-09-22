#!/bin/sh
# Локальная проверка HW1. Запуск из корня студенческого репозитория:
#   sh ./check.sh
set -eu

label="mlsd-hw1-shell.run=hw1-$$-$(date +%s)"
image="mlsd-hw1-local-${label#*=}"
cleanup_owned() {
    cleanup_failed=0
    if ! command -v docker >/dev/null 2>&1 || ! command -v timeout >/dev/null 2>&1; then
        return 0
    fi

    container_ids=$(timeout 30 docker ps --all --quiet --filter "label=$label") || cleanup_failed=1
    if [ -n "${container_ids:-}" ]; then
        timeout 30 docker rm --force $container_ids >/dev/null 2>&1 || cleanup_failed=1
    fi

    image_ids=$(timeout 30 docker images --quiet --filter "label=$label") || cleanup_failed=1
    if [ -n "${image_ids:-}" ]; then
        timeout 30 docker image rm --force $image_ids >/dev/null 2>&1 || cleanup_failed=1
    fi

    if [ "$cleanup_failed" -eq 1 ]; then
        return 1
    fi
    return 0
}

finish() {
    status=$?
    trap - 0 HUP INT TERM
    if ! cleanup_owned; then
        printf '%s\n' "FAIL: не удалось очистить Docker-ресурсы, созданные проверкой HW1" >&2
        exit 1
    fi
    exit "$status"
}

fail() {
    printf '%s\n' "FAIL: $*" >&2
    exit 1
}

json_status_ok() {
    awk '
function ws(    c) {
    while (position <= length(source)) {
        c = substr(source, position, 1)
        if (c !~ /[ \t\r\n]/) return
        position++
    }
}
function hex_value(hex,    index_value, character, value) {
    value = 0
    for (index_value = 1; index_value <= 4; index_value++) {
        character = index("0123456789abcdef", tolower(substr(hex, index_value, 1))) - 1
        value = value * 16 + character
    }
    return value
}
function string(    c, hex, code) {
    parsed_string = ""
    if (substr(source, position, 1) != "\"") return 0
    position++
    while (position <= length(source)) {
        c = substr(source, position, 1)
        if (c == "\"") { position++; return 1 }
        if (c ~ /[[:cntrl:]]/) return 0
        if (c == "\\") {
            position++
            c = substr(source, position, 1)
            if (c == "\"" || c == "\\" || c == "/") { parsed_string = parsed_string c; position++; continue }
            if (c == "b") { parsed_string = parsed_string sprintf("%c", 8); position++; continue }
            if (c == "f") { parsed_string = parsed_string sprintf("%c", 12); position++; continue }
            if (c == "n") { parsed_string = parsed_string sprintf("%c", 10); position++; continue }
            if (c == "r") { parsed_string = parsed_string sprintf("%c", 13); position++; continue }
            if (c == "t") { parsed_string = parsed_string sprintf("%c", 9); position++; continue }
            if (c != "u" || substr(source, position + 1, 4) !~ /^[0-9A-Fa-f]{4}$/) return 0
            hex = substr(source, position + 1, 4)
            code = hex_value(hex)
            if (code <= 127) parsed_string = parsed_string sprintf("%c", code)
            else parsed_string = parsed_string "\\u" hex
            position += 5
            continue
        }
        parsed_string = parsed_string c
        position++
    }
    return 0
}
function number(    rest) {
    rest = substr(source, position)
    if (!match(rest, /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?/)) return 0
    position += RLENGTH
    return 1
}
function literal(    rest) {
    rest = substr(source, position)
    if (substr(rest, 1, 4) == "true" || substr(rest, 1, 4) == "null") { position += 4; return 1 }
    if (substr(rest, 1, 5) == "false") { position += 5; return 1 }
    return 0
}
function array(    c) {
    position++; ws()
    if (substr(source, position, 1) == "]") { position++; return 1 }
    while (1) {
        if (!value()) return 0
        ws(); c = substr(source, position, 1)
        if (c == "]") { position++; return 1 }
        if (c != ",") return 0
        position++; ws()
    }
}
function object(    c) {
    position++; ws()
    if (substr(source, position, 1) == "}") { position++; return 1 }
    while (1) {
        if (!string()) return 0
        ws()
        if (substr(source, position, 1) != ":") return 0
        position++; ws()
        if (!value()) return 0
        ws(); c = substr(source, position, 1)
        if (c == "}") { position++; return 1 }
        if (c != ",") return 0
        position++; ws()
    }
}
function root_object(    c, key, status_seen, status_ok) {
    if (substr(source, position, 1) != "{") return 0
    position++; ws()
    if (substr(source, position, 1) == "}") { position++; return 0 }
    while (1) {
        if (!string()) return 0
        key = parsed_string
        ws()
        if (substr(source, position, 1) != ":") return 0
        position++; ws()
        if (key == "status") {
            if (!string() || parsed_string != "ok" || status_seen) return 0
            status_seen = 1
            status_ok = 1
        } else if (!value()) return 0
        ws(); c = substr(source, position, 1)
        if (c == "}") { position++; return status_seen && status_ok }
        if (c != ",") return 0
        position++; ws()
    }
}
function value(    c) {
    ws(); c = substr(source, position, 1)
    if (c == "{") return object()
    if (c == "[") return array()
    if (c == "\"") return string()
    if (c == "-" || c ~ /[0-9]/) return number()
    return literal()
}
{
    source = source $0 "\n"
}
END {
    position = 1
    ws()
    valid = root_object()
    ws()
    exit !(valid && position > length(source))
}'
}

trap finish 0
trap 'exit 1' HUP INT TERM

[ -f Dockerfile ] || fail "отсутствует файл Dockerfile: создайте его по разделу требований задания"

lockfile=""
for candidate in uv.lock poetry.lock Pipfile.lock requirements.lock package-lock.json npm-shrinkwrap.json pnpm-lock.yaml yarn.lock Cargo.lock go.sum; do
    if [ -f "$candidate" ]; then
        lockfile=$candidate
        break
    fi
done
[ -n "$lockfile" ] || fail "отсутствует файл закреплённых зависимостей: создайте один из файлов, перечисленных в требованиях задания"

[ -f README.md ] || fail "отсутствует файл README.md: создайте его с командами сборки и запуска"
if head -n 20 Dockerfile | grep -Eq '^[[:space:]]*#[[:space:]]*syntax[[:space:]]*=' ; then
    fail "уберите строку '# syntax=...': серверная проверка собирает со встроенным frontend и не загружает внешние frontend-образы"
fi
grep -Eiq '^[[:space:]]*docker[[:space:]]+build.*[[:space:]]\.[[:space:]]*$' README.md || fail "README.md должен содержать команду docker build с контекстом ."
grep -Eiq '^[[:space:]]*docker[[:space:]]+run.*8080' README.md || fail "README.md должен содержать команду docker run с портом 8080"

from_lines=$(grep -Ei '^[[:space:]]*from[[:space:]]+' Dockerfile || true)
[ -n "$from_lines" ] || fail "в Dockerfile нет инструкции FROM"
while IFS= read -r from_line; do
    set -- $from_line
    shift
    case "$1" in
        --platform=*) shift ;;
    esac
    base_image=$1
    case "$base_image" in
        scratch) ;;
        *@sha256:*)
            digest=${base_image##*@sha256:}
            printf '%s' "$digest" | grep -Eq '^[0-9A-Fa-f]{64}$' || fail "в Dockerfile каждый FROM должен указывать образ с sha256 digest"
            ;;
        *) fail "в Dockerfile каждый FROM должен указывать образ с sha256 digest" ;;
    esac
done <<EOF
$from_lines
EOF

command -v docker >/dev/null 2>&1 || fail "не установлена утилита docker: установите Docker и повторите проверку"
command -v curl >/dev/null 2>&1 || fail "не установлена утилита curl: установите curl и повторите проверку"
command -v timeout >/dev/null 2>&1 || fail "не установлена утилита timeout: установите GNU timeout и повторите проверку"
timeout 30 docker info >/dev/null 2>&1 || fail "демон Docker недоступен: запустите Docker и проверьте командой docker version"

printf '%s\n' "Сборка локального образа HW1..."
timeout 600 docker build --label "$label" --tag "$image" . || fail "ошибка docker build"
container=$(timeout 30 docker run --detach --label "$label" --publish 127.0.0.1::8080 "$image") || fail "ошибка docker run"
address=$(timeout 30 docker port "$container" 8080/tcp) || fail "Docker не опубликовал порт 8080: проверьте, что сервис слушает порт 8080 внутри контейнера"
host_port=${address##*:}
[ -n "$host_port" ] || fail "Docker не сообщил порт на хосте: повторите проверку"

response=$(timeout 2 curl --fail --silent --show-error --max-time 2 --retry 10 --retry-all-errors --retry-delay 0 --retry-max-time 2 --max-filesize 65536 "http://127.0.0.1:${host_port}/health") || fail "GET /health не вернул HTTP 200 за две секунды: проверьте, что контейнер запущен и отвечает по адресу /health"
printf '%s' "$response" | json_status_ok || fail "GET /health должен вернуть JSON-объект верхнего уровня с полем status ok: проверьте формат ответа"

trap - 0 HUP INT TERM
cleanup_owned || fail "не удалось очистить Docker-ресурсы, созданные проверкой HW1: удалите контейнеры и образы с меткой mlsd-hw1 вручную"
printf '%s\n' "PASS: локальная проверка контейнера HW1 прошла"
