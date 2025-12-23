# Лабораторные работы по курсу "Большие данные"

Репозиторий содержит лабораторные работы по курсу "Большие данные" и "Наука о данных" (5 семестр, ПМ-ПУ, СПбГУ).

## 📋 Содержание

- [MapReduce](#mapreduce) - Сравнение производительности подходов к обработке данных
- [Hadoop](#hadoop) - Кластер Hadoop и анализ Git-репозиториев
- [Spark](#spark) - Лабораторные работы с Apache Spark
- [Кластеризация заказов Ozon](#кластеризация-заказов-ozon) - Группировка заказов по зонам доставки
- [ML проекты](#ml-проекты) - Задачи машинного обучения

## 🚀 Быстрый старт

```bash
# Создать и активировать виртуальное окружение
python3.11 -m venv local_env
source local_env/bin/activate

# Установить зависимости
pip install -r requirements.txt

# Запустить Jupyter
jupyter notebook
```

## 📂 Проекты

### MapReduce

Реализация MapReduce для вычисления средней цены по категориям товаров с сравнением производительности трёх подходов.

**Запуск:**
```bash
cd map_reduce
python map_reduce.py
```

**Результаты (датасет 500M строк):**
- Pandas groupby: 99.86 сек
- Sequential MapReduce: 38.31 сек (**2.6x быстрее**)
- Parallel MapReduce: 56.86 сек (**1.76x быстрее**)

### Hadoop

Docker-кластер Hadoop с MapReduce-задачей для анализа вклада компаний в Git-репозитории.

**Запуск:**
```bash
cd hadoop/hadoop-cluster-docker
docker-compose up -d
```

**Web-интерфейсы:**
- HDFS NameNode: http://localhost:9870
- YARN ResourceManager: http://localhost:8088

**Анализ Git-репозитория:**
```bash
cd hadoop/git_analysis
python prepare_data.py /path/to/repo git_commits.txt
docker cp git_analysis hadoop-cluster-docker-namenode-1:/opt/
docker cp git_commits.txt hadoop-cluster-docker-namenode-1:/opt/git_analysis/

docker exec -it hadoop-cluster-docker-namenode-1 bash
cd /opt/git_analysis
bash run_hadoop.sh git_commits.txt output
```

### Spark

Лабораторные работы с Apache Spark для анализа данных с IoT-датчиков стиральных машин.

**Notebooks:**
- `lab 01 - assignment2_spark2.3_python3.6.ipynb` - DataFrame API
- `lab 01 - assignment3_spark2.3_python3.5_cos.ipynb` - Статистические функции

**Данные:** `data/washing.parquet` (2058 строк, 11 столбцов)

### Кластеризация заказов Ozon

Группировка заказов доставки по полигонам с использованием иерархической кластеризации и метрики Хаусдорфа.

**Файлы:**
- `ozone_orders/ozone_orders.ipynb` - Основной notebook (~187MB)
- `ozone_orders/clusters.py` - Класс ClusterSet
- `ozone_orders/voronoi_algorithm.py` - Алгоритм Вороного

### ML проекты

- `iris/` - Классификация набора данных Iris
- `cnn-mushrooms/` - CNN для классификации грибов
- `cnn-butterflies/` - CNN для классификации бабочек
- `lego-vision/` - Computer Vision с CNN
- `spaceship-titanic/` - Решение Kaggle-соревнования

## 🛠️ Технологии

Python 3.11 • Apache Hadoop 3.3.6 • Apache Spark 2.3 • Docker • Pandas • NumPy • Matplotlib • PySpark • Jupyter

## ⚠️ Решение проблем

**Hadoop: DataNode не запускается**
```bash
cd hadoop/hadoop-cluster-docker
docker-compose down && docker volume prune -f && docker-compose up -d
```

**Hadoop: MapReduce зависает**
```bash
docker exec hadoop-cluster-docker-resourcemanager-1 yarn node -list
```
Должна быть 1 активная нода. Если нет - перезапустить кластер.

## 📝 Примечания

- Данные хранятся в `data/` (не включены в репозиторий)
- Большие notebook'и могут долго загружаться
