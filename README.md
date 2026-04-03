# cpu_monitor

Мониторинг загрузки CPU всех процессов хоста с классификацией по группам и генерацией JSON-отчёта.

## Запуск

```bash
# 24 часа, интервал 10 сек
./cpu_monitor

# 1 час, интервал 5 сек
./cpu_monitor --duration 1 --interval 5

# С отправкой отчёта в чат
./cpu_monitor --room-id 12345 --webhook-url http://10.10.0.1:9099/send-file-image
```

## Запуск в фоне

```bash
nohup ./cpu_monitor --duration 24 > monitor.log 2>&1 &
```

Проверить процесс:
```bash
jobs -l
# или
ps aux | grep cpu_monitor
```

Остановить:
```bash
kill %1
# или по PID
kill <PID>
```

При прерывании (`Ctrl+C` или `kill`) сохраняется частичный отчёт.

## Опции

| Опция | По умолчанию | Описание |
|-------|--------------|----------|
| `-r, --room-id` | `""` | ID комнаты для отправки отчёта |
| `-i, --interval` | `10` | Интервал опроса (сек) |
| `--duration` | `24` | Длительность мониторинга (часы) |
| `--webhook-url` | `http://10.10.0.1:9099/send-file-image` | URL вебхука |

## Отчёт

Результат сохраняется в `cpu_report_<timestamp>.json`:

```json
{
  "duration_hours": 24,
  "total_samples": 8640,
  "started_at": "2024-01-01T00:00:00+00:00",
  "finished_at": "2024-01-02T00:00:00+00:00",
  "groups": [
    ["💻 System", 3600.5],
    ["🐋 Docker:containerd", 1200.3],
    ["🦊 Gitlab", 800.0]
  ]
}
```

## Сборка

```bash
cargo build --release
```
