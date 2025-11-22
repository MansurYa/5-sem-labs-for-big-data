#!/bin/bash

INPUT_FILE=${1:-"git_commits.txt"}
OUTPUT_DIR=${2:-"output"}

echo "Анализ вклада компаний в Git репозиторий:"
echo "Входной файл: $INPUT_FILE"
echo "Директория вывода: $OUTPUT_DIR"
echo ""

# Создаём директорию в HDFS для входных данных
echo "1. Создание директории input в HDFS..."
hdfs dfs -mkdir -p /user/input

# Копируем входной файл в HDFS
echo "2. Копирование $INPUT_FILE в HDFS..."
hdfs dfs -put -f $INPUT_FILE /user/input/

# Удаляем директорию вывода если существует
echo "3. Очистка предыдущих результатов..."
hdfs dfs -rm -r -f /user/$OUTPUT_DIR

# Запускаем MapReduce задачу
echo "4. Запуск MapReduce задачи..."
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files src/mapper.py,src/reducer.py \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -input /user/input/$INPUT_FILE \
    -output /user/$OUTPUT_DIR

# Проверяем результат
if [ $? -eq 0 ]; then
    echo ""
    echo "MapReduce задача завершена успешно:"
    echo ""
    echo "Результаты:"
    hdfs dfs -cat /user/$OUTPUT_DIR/part-* | sort -t$'\t' -k2 -nr

    echo ""
    echo "Полные результаты сохранены в HDFS: /user/$OUTPUT_DIR"
else
    echo ""
    echo "Ошибка при выполнении MapReduce задачи:"
    exit 1
fi
