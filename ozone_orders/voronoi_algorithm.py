#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from scipy.spatial import Voronoi
from shapely.geometry import Polygon
from shapely.ops import unary_union
from typing import Dict, List, Tuple
from clusters import ClusterSet


def build_voronoi_clusters(df: pd.DataFrame) -> Dict[int, List[List[Tuple[float, float]]]]:    
    # Извлекаем координаты и создаем маппинг
    points = df[['Lat', 'Long']].values
    cluster_mapping = df['MpId'].values
        
    # Строим диаграмму Вороного
    vor = Voronoi(points)
    
    # Словарь для хранения многоугольников по кластерам
    cluster_polygons = {}
    
    # Обрабатываем каждую точку
    for point_idx, mp_id in enumerate(cluster_mapping):
        region_index = vor.point_region[point_idx]
        region = vor.regions[region_index]
        
        # Пропускаем бесконечные ячейки
        if -1 in region or len(region) < 3:
            continue
            
        # Получаем координаты вершин
        vertices = vor.vertices[region]
        
        # Создаем многоугольник
        try:
            polygon = Polygon(vertices)
            if polygon.is_valid:
                if mp_id not in cluster_polygons:
                    cluster_polygons[mp_id] = []
                cluster_polygons[mp_id].append(polygon)
        except:
            continue
    
    # Объединяем многоугольники для каждого кластера
    
    result = {}
    for mp_id, polygons in cluster_polygons.items():
        if len(polygons) == 1:
            # Один многоугольник
            coords = list(polygons[0].exterior.coords)
            # Конвертируем в формат (lat, lon) для Folium
            coords = [(lat, lon) for lat, lon in coords]
            result[mp_id] = [coords]
        else:
            # Несколько многоугольников - объединяем
            try:
                union_polygon = unary_union(polygons)
                if hasattr(union_polygon, 'exterior'):
                    # Один многоугольник
                    coords = list(union_polygon.exterior.coords)
                    coords = [(lat, lon) for lat, lon in coords]
                    result[mp_id] = [coords]
                else:
                    # Несколько многоугольников
                    coords_list = []
                    for geom in union_polygon.geoms:
                        coords = list(geom.exterior.coords)
                        coords = [(lat, lon) for lat, lon in coords]
                        coords_list.append(coords)
                    result[mp_id] = coords_list
            except:
                # Если не удалось объединить, берем первый
                coords = list(polygons[0].exterior.coords)
                coords = [(lat, lon) for lat, lon in coords]
                result[mp_id] = [coords]
    
    
    return result


def get_cluster_statistics(df: pd.DataFrame, voronoi_clusters: Dict) -> Dict:
    stats = {
        'total_clusters': len(voronoi_clusters),
        'total_points': len(df),
        'clusters_with_polygons': len(voronoi_clusters),
        'average_points_per_cluster': len(df) / len(df['MpId'].unique())
    }
    
    return stats
