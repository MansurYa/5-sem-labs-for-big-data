# Отчёт по Fine-Tuning Vision Transformer (ViT) на датасете грибов

**Дата:** 2025-12-05
**Автор:** Мансур Зайнуллин
**Модель:** Vision Transformer Base/16 (ViT-Base/16)
**Датасет:** Mushrooms (9 классов грибов)

---

## 1. Цель работы

Выполнить fine-tuning предобученной модели Vision Transformer (ViT-Base/16) на датасете грибов, следуя протоколу из статьи **"An Image is Worth 16x16 Words: Transformers for Image Recognition at Scale"** (Dosovitskiy et al., ICLR 2021).

---

## 2. Исходные данные

### 2.1 Датасет
- **Источник:** Preprocessed датасет `Mushrooms_preprocessed.pt`
- **Размер:** 6,714 изображений
- **Количество классов:** 9 (Agaricus, Amanita, Boletus, Cortinarius, Entoloma, Hygrocybe, Lactarius, Russula, Suillus)
- **Разрешение:** 128×128 → upsampled до 224×224 (требование ViT)
- **Split:** 80% train (5,371), 20% test (1,343)

### 2.2 Нормализация
Данные уже предобработаны с нормализацией:
```python
mean = [0.5, 0.5, 0.5]
std = [0.5, 0.5, 0.5]
```

---

## 3. Модель

### 3.1 Архитектура
- **Название:** Vision Transformer Base/16 (ViT-Base/16)
- **Источник:** `google/vit-base-patch16-224` (Hugging Face)
- **Параметры:** 86M (86 миллионов)
- **Конфигурация (Table 1 из статьи):**
  - Layers: 12
  - Hidden size (D): 768
  - MLP size: 3072
  - Heads: 12
  - Patch size: 16×16

### 3.2 Предобучение
Модель предобучена Google Research на датасете **ImageNet-21k** (14M изображений, 21k классов).

### 3.3 Модификации для fine-tuning
Согласно разделу **3.2** статьи:
1. **Удалён предобученный classification head** (MLP с одним скрытым слоем)
2. **Добавлен новый linear layer** (768 → 9 классов) с **zero-initialization**
3. **Позиционные эмбеддинги:** Сохранены от 224×224 (интерполяция не требуется)

---

## 4. Протокол обучения

### 4.1 Параметры из статьи (Table 4, Section B.1.1)

| Параметр | Значение | Источник |
|----------|----------|----------|
| **Optimizer** | SGD | Section B.1.1 |
| **Momentum** | 0.9 | Table 4 |
| **Learning Rate** | 0.003 | Table 4 (диапазон: 0.001–0.03) |
| **Weight Decay** | 0.0 | Table 4 (no weight decay) |
| **LR Scheduler** | Cosine Annealing | Table 4 |
| **Gradient Clipping** | 1.0 (global norm) | Section B.1.1 |
| **Batch Size** | 512 (статья) → **4 (адаптация для M1)** | Table 4 |
| **Resolution** | 384 (статья) → **224 (адаптация для M1)** | Section 3.2 |

### 4.2 Адаптация для MacBook Air M1 (8GB RAM)

**Ограничения железа:**
- Unified Memory: 8 ГБ (делится между CPU и GPU)
- Нет активного охлаждения
- MPS (Metal Performance Shaders) вместо CUDA

**Изменения:**
1. **Batch Size:** 512 → **4**
   - Причина: Ограничение памяти
   - Компенсация: Увеличение числа эпох для достижения того же числа gradient updates

2. **Resolution:** 384×384 → **224×224**
   - Причина: Sequence length растёт квадратично ($N = HW/P^2$)
   - 384×384: $N = 576$ patches
   - 224×224: $N = 196$ patches (в 2.9 раза меньше)

3. **Сохранение каждой эпохи:**
   - Checkpoint: `vit_epoch_{N}.pth`
   - Предотвращает потерю прогресса при сбое

### 4.3 Data Augmentation (On-the-fly)

Вместо offline аугментации (×7 копий на диске) используется **on-the-fly** подход:

```python
transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.RandomHorizontalFlip(p=0.5),
    transforms.RandomVerticalFlip(p=0.5),
    transforms.RandomRotation(15),
    transforms.ColorJitter(brightness=0.2, contrast=0.2,
                           saturation=0.2, hue=0.1),
    transforms.Normalize(mean=[0.5, 0.5, 0.5],
                        std=[0.5, 0.5, 0.5])
])
```

**Преимущества:**
- Не занимает место на диске
- Каждая эпоха видит новые варианты искажений
- Эффект «×7» достигается увеличением числа эпох

### 4.4 Early Stopping

```python
patience = 10
min_delta = 0.001
```

Обучение останавливается, если test loss не улучшается 10 эпох подряд. Загружается best model (по min test loss).

---

## 5. Вычислительные затраты

### 5.1 Теоретическая оценка (для M1)

**Время на 1 эпоху:**
- Batches в эпохе: $5,371 / 4 \approx 1,343$ шага
- Время на батч (ViT-Base на MPS): ~0.3–0.5 сек
- **Итого:** $1,343 \times 0.4 \approx 537$ сек $\approx$ **9 минут**

**Для 10 эпох:** ~90 минут (~1.5 часа)

### 5.2 Сравнение с оригиналом (Table 4)

| Параметр | Оригинал (статья) | Наша реализация |
|----------|-------------------|-----------------|
| Hardware | TPUv3 (128GB HBM) | MacBook Air M1 (8GB) |
| Batch Size | 512 | 4 |
| Resolution | 384×384 | 224×224 |
| Epochs | ~500 steps | ~10-100 epochs |
| Время/эпоха | ~1 мин (TPU) | ~9 мин (M1) |

---

## 6. Результаты (ожидаемые)

### 6.1 Baseline (Table 5 из статьи)

**ViT-Base/16, pretrained на ImageNet-21k:**
- CIFAR-10: **98.95%**
- CIFAR-100: **91.67%**
- Oxford Flowers-102: **99.38%**
- Oxford-IIIT Pets: **94.43%**

### 6.2 Наш датасет (прогноз)

Учитывая:
- Датасет меньше (~6.7k против 10k+ в CIFAR)
- 9 классов (средняя сложность)
- High-quality изображения грибов

**Ожидаемая точность:**
- Train Accuracy: **85-95%**
- Test Accuracy: **75-85%**

**Факторы, влияющие на результат:**
- ✅ Предобучение на ImageNet-21k (общие визуальные признаки)
- ✅ Data augmentation (×7 эффект через эпохи)
- ⚠️ Малый batch size (может ухудшить сходимость)
- ⚠️ Датасет меньше, чем у BiT/ViT benchmarks

### 6.3 Метрики для оценки

1. **Test Accuracy** (основная метрика)
2. **Train/Test Gap** (индикатор переобучения)
3. **Loss curves** (стабильность сходимости)
4. **Per-class accuracy** (для выявления проблемных классов)

---

## 7. Сравнение с предыдущей работой

### 7.1 Предыдущая модель (из `final_ensemble.ipynb`)

| Параметр | CNN (Ensemble) | ViT-Base/16 |
|----------|----------------|-------------|
| Архитектура | Custom CNN (5 conv + 3 FC) | Transformer (12 layers) |
| Параметры | ~6M | **86M** |
| Предобучение | ❌ С нуля | ✅ ImageNet-21k |
| Датасет | Augmented ×7 (47k) | Preprocessed (6.7k) + on-the-fly |
| Test Accuracy | **83.86%** (ensemble) | **? (TBD)** |
| Train Accuracy | 81.09% (avg single) | **? (TBD)** |

### 7.2 Ожидаемое улучшение

**Преимущества ViT:**
- Предобучен на 14M изображений
- Global attention (видит всю картинку с 1-го слоя)
- State-of-the-art архитектура

**Потенциальные проблемы:**
- Малый batch size на M1
- ViT требует больше данных для обучения с нуля (но мы делаем fine-tuning)

**Прогноз:** ViT должен **превзойти** CNN на **3-7%** (итоговая точность ~87-90%).

---

## 8. Файлы и артефакты

### 8.1 Входные данные
```
data/Mushrooms_preprocessed.pt   # Исходный датасет (6714 img)
```

### 8.2 Код
```
cnn/vit_finetuning.ipynb          # Основной ноутбук
```

### 8.3 Выходные данные
```
data/vit_epoch_{1..N}.pth         # Checkpoint каждой эпохи
data/vit_finetuned_final.pth      # Финальная модель (best)
data/vit_training_curves.png      # Графики Loss/Accuracy
```

### 8.4 Структура checkpoint файла
```python
{
    'epoch': int,
    'model_state_dict': OrderedDict,
    'optimizer_state_dict': dict,
    'scheduler_state_dict': dict,
    'train_loss': float,
    'train_acc': float,
    'test_loss': float,
    'test_acc': float,
}
```

---

## 9. Воспроизводимость

### 9.1 Environment
```bash
Python: 3.11
PyTorch: 2.x (with MPS support)
transformers: 4.x
Device: Apple M1 (Metal Performance Shaders)
```

### 9.2 Seed
```python
SEED = 42  # Фиксирован во всех операциях
```

### 9.3 Команды для запуска
```bash
cd cnn
jupyter notebook vit_finetuning.ipynb
# Run all cells
```

---

## 10. Выводы

### 10.1 Достижения

1. ✅ **Успешно адаптирован протокол из статьи** для ограниченного железа (M1 8GB)
2. ✅ **Реализован корректный fine-tuning** с заменой classification head
3. ✅ **Применена on-the-fly аугментация** (экономия места + разнообразие)
4. ✅ **Early stopping + сохранение каждой эпохи** (защита от сбоев)

### 10.2 Уроки

**Компромиссы для M1:**
- Batch size 4 vs 512 (статья) → может ухудшить сходимость SGD
- Resolution 224 vs 384 (статья) → может потерять мелкие детали

**Решения:**
- Увеличение числа эпох компенсирует малый batch
- Cosine scheduler адаптируется под длительное обучение

### 10.3 Следующие шаги

**Если результат < 85%:**
1. Попробовать AdamW вместо SGD (лучше для малых batch)
2. Увеличить LR warmup
3. Добавить Label Smoothing (0.1)
4. Попробовать Stochastic Depth

**Если результат > 85%:**
1. Сделать Ensemble из 3-5 моделей (seeds 42, 100, 200...)
2. Попробовать Test-Time Augmentation (TTA)
3. Посчитать Confusion Matrix для анализа ошибок

---

## 11. Ссылки

### 11.1 Статья
- **Title:** An Image is Worth 16x16 Words: Transformers for Image Recognition at Scale
- **Authors:** Alexey Dosovitskiy et al.
- **Conference:** ICLR 2021
- **Paper:** https://arxiv.org/abs/2010.11929
- **Code:** https://github.com/google-research/vision_transformer

### 11.2 Предобученные веса
- **Hugging Face:** https://huggingface.co/google/vit-base-patch16-224
- **Model Card:** ViT-Base/16, pretrained on ImageNet-21k

### 11.3 Датасет
- **Источник:** Собственная коллекция изображений грибов
- **Предобработка:** `cnn/preprocess_dataset.ipynb`

---

## Приложение A: Архитектура ViT-Base/16

```
Input: [B, 3, 224, 224]
  ↓
Patch Embedding (conv 16×16, stride 16)
  → [B, 196, 768]  # 14×14 patches, dim 768
  ↓
+ Position Embedding [196+1, 768]
+ [CLS] Token
  ↓
Transformer Encoder (12 layers)
  ├─ Multi-Head Attention (12 heads)
  ├─ LayerNorm
  ├─ MLP (768 → 3072 → 768)
  └─ LayerNorm
  ↓
[CLS] Token Output [B, 768]
  ↓
Classification Head (Linear)
  → [B, 9]  # logits для 9 классов
```

---

## Приложение B: Сравнение с BiT (ResNet)

**Из Table 2 статьи:**

| Model | ImageNet | CIFAR-100 | Flowers | Pets | Params | TPUv3-days |
|-------|----------|-----------|---------|------|--------|------------|
| **BiT-L (R152×4)** | 87.54% | 93.51% | 99.63% | 96.62% | ~900M | 9.9k |
| **ViT-L/16 (I21k)** | 85.30% | 93.25% | 99.61% | 94.67% | 307M | 0.23k |
| **ViT-B/16 (I21k)** | ~84% | ~92% | ~99% | ~94% | **86M** | **~0.1k** |

**Вывод:** ViT-Base на порядок дешевле ResNet при сопоставимом качестве.

---

**Конец отчёта**
