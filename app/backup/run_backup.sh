#!/bin/bash

echo "Backup scheduler started"
echo "Next backup at: $(date -d '03:00 tomorrow' '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -v +1d -v 3H -v 0M -v 0S '+%Y-%m-%d %H:%M:%S')"

while true; do
    current_time=$(date +%H:%M)

    # Запуск в 03:00
    if [ "$current_time" = "03:00" ]; then
        echo "================================"
        echo "Starting backup at $(date)"
        echo "================================"
        python /backup/run_backup.py
        echo "================================"
        echo "Backup completed at $(date)"
        echo "================================"
        echo "Next backup at: $(date -d '03:00 tomorrow' '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -v +1d -v 3H -v 0M -v 0S '+%Y-%m-%d %H:%M:%S')"
        # Спим минуту чтобы не запускать дважды
        sleep 60
    fi

    # Проверяем каждую минуту
    sleep 60
done
