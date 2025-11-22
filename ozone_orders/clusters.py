import pandas as pd
import numpy as np
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform
from tqdm import tqdm

class ClusterSet:
    """Объект для представления кластеров - множеств ID с валидацией"""
    
    def __init__(self, df=None, method='mpid'):
        if df is not None and method == 'mpid' and isinstance(df, pd.DataFrame):
            # Используем существующую группировку по MpId
            self.clusters = df.groupby('MpId')['ID'].apply(set).to_dict()
        else:
            self.clusters = {}
        self._validate()
    
    def _validate(self):
        """Проверка: объединение = все ID, пересечения = пусто"""
        all_ids = set()
        for cluster in self.clusters.values():
            all_ids.update(cluster)
        
        # Проверка пересечений
        cluster_list = list(self.clusters.values())
        for i in range(len(cluster_list)):
            for j in range(i + 1, len(cluster_list)):
                if cluster_list[i] & cluster_list[j]:
                    raise ValueError("Кластеры пересекаются!")
            
    def get_cluster(self, cluster_id):
        """Получить кластер по ID"""
        return self.clusters.get(cluster_id, set())
    
    def get_point_cluster(self, point_id):
        """Найти кластер для точки"""
        for cluster_id, cluster in self.clusters.items():
            if point_id in cluster:
                return cluster_id
        return None
    
    def add_point(self, cluster_id, point_id):
        """Добавить точку в кластер"""
        if cluster_id not in self.clusters:
            self.clusters[cluster_id] = set()
        self.clusters[cluster_id].add(point_id)
    
    def remove_point(self, point_id):
        """Удалить точку из всех кластеров"""
        for cluster in self.clusters.values():
            cluster.discard(point_id)
    
    def to_dict(self):
        """Экспорт в словарь для совместимости"""
        return {k: list(v) for k, v in self.clusters.items()}
    
    def __len__(self):
        return len(self.clusters)
    
    def __iter__(self):
        return iter(self.clusters.items())
    
    def cluster_hierarchical(self, n_clusters, distance_matrix, id_mapping, min_cluster_size=3):
        """
        Иерархическая кластеризация с метрикой Хаусдорфа для асимметричных расстояний
        
        Args:
            n_clusters: Количество кластеров для получения
            distance_matrix: Матрица расстояний между точками
            id_mapping: Маппинг ID -> индекс в матрице
            min_cluster_size: Минимальный размер кластера для фильтрации
        """
        
        # Фильтруем малые кластеры
        filtered_clusters = {k: v for k, v in self.clusters.items() if len(v) >= min_cluster_size}
        print(f"   Фильтрация: {len(self.clusters)} -> {len(filtered_clusters)} кластеров")
        
        if len(filtered_clusters) < 2:
            return self
        
        # Вычисляем матрицу расстояний между кластерами
        cluster_list = list(filtered_clusters.items())
        n = len(cluster_list)
        distance_matrix_clusters = np.zeros((n, n))
        
        print(f"   Вычисляем расстояния между {n} кластерами...")
        
        # Используем tqdm для отслеживания прогресса
        total_pairs = n * (n - 1) // 2
        with tqdm(total=total_pairs, desc="Вычисление метрики Хаусдорфа", unit="пар") as pbar:
            for i in range(n):
                for j in range(i + 1, n):
                    cluster1_id, cluster1_points = cluster_list[i]
                    cluster2_id, cluster2_points = cluster_list[j]
                    
                    # Вычисляем асимметричную метрику Хаусдорфа
                    hausdorff_dist = self._compute_hausdorff_distance(
                        cluster1_points, cluster2_points, distance_matrix, id_mapping
                    )
                    
                    distance_matrix_clusters[i, j] = hausdorff_dist
                    distance_matrix_clusters[j, i] = hausdorff_dist
                    pbar.update(1)
        
        # Иерархическая кластеризация
        condensed_distances = squareform(distance_matrix_clusters)
        
        linkage_matrix = linkage(condensed_distances, method='complete')
        
        # Получаем кластеры
        cluster_labels = fcluster(linkage_matrix, n_clusters, criterion='maxclust')
        
        # Создаем новые кластеры
        new_clusters = {}
        for i, (original_id, points) in enumerate(cluster_list):
            new_cluster_id = cluster_labels[i]
            if new_cluster_id not in new_clusters:
                new_clusters[new_cluster_id] = set()
            new_clusters[new_cluster_id].update(points)
        
        # УПРОЩЕНИЕ: Убираем избыточную логику с малыми кластерами
        # Просто добавляем их в ближайший большой кластер
        if min_cluster_size > 1:
            small_clusters = {k: v for k, v in self.clusters.items() if len(v) < min_cluster_size}
            if small_clusters:
                print(f"   Добавляем {len(small_clusters)} малых кластеров к ближайшим большим...")
                for small_id, small_points in tqdm(small_clusters.items(), desc="Обработка малых кластеров"):
                    # Находим ближайший большой кластер
                    min_distance = np.inf
                    closest_cluster = None
                    
                    for big_cluster_id, big_points in new_clusters.items():
                        if big_points:  # Проверяем, что кластер не пустой
                            dist = self._compute_hausdorff_distance(
                                small_points, big_points, distance_matrix, id_mapping
                            )
                            if dist < min_distance:
                                min_distance = dist
                                closest_cluster = big_cluster_id
                    
                    if closest_cluster is not None:
                        new_clusters[closest_cluster].update(small_points)
        
        self.clusters = new_clusters
        self._validate()
        
        return self
    
    def _compute_hausdorff_distance(self, cluster1, cluster2, distance_matrix, id_mapping):
        """Вычисляет асимметричную метрику Хаусдорфа между двумя кластерами"""
        # Получаем индексы в матрице расстояний
        indices1 = [id_mapping[point_id] for point_id in cluster1 if point_id in id_mapping]
        indices2 = [id_mapping[point_id] for point_id in cluster2 if point_id in id_mapping]
        
        if not indices1 or not indices2:
            return np.inf
        
        # Получаем подматрицу расстояний
        submatrix = distance_matrix[np.ix_(indices1, indices2)]
        
        # Вычисляем асимметричную метрику Хаусдорфа
        # max(max(min(dist(x,y) for y in Y) for x in X), max(min(dist(y,x) for x in X) for y in Y))
        
        # X -> Y: для каждого x в X найти минимальное расстояние до Y
        min_dist_x_to_y = np.min(submatrix, axis=1)  # min по столбцам (Y)
        max_min_x_to_y = np.max(min_dist_x_to_y)
        
        # Y -> X: для каждого y в Y найти минимальное расстояние до X  
        min_dist_y_to_x = np.min(submatrix, axis=0)  # min по строкам (X)
        max_min_y_to_x = np.max(min_dist_y_to_x)
        
        # Хаусдорфово расстояние = максимум из двух направлений
        hausdorff_dist = max(max_min_x_to_y, max_min_y_to_x)
        
        return hausdorff_dist
