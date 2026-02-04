# -*- coding: utf-8 -*-
"""Анализ timing.csv: потоки (worker_slot), скорость, визуализация."""

import csv
from collections import defaultdict

# Попытка импорта для графиков
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


def load_csv(path):
    """Загрузка без pandas: возврат list of dict. Пропускает повторные заголовки."""
    rows = []
    with open(path, 'r', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            # Пропуск строки-заголовка (дубликат заголовка в данных)
            if row.get('duration_sec') == 'duration_sec':
                continue
            try:
                row['duration_sec'] = float(row['duration_sec'])
            except (ValueError, TypeError):
                continue
            row['success'] = row['success'] == 'True'
            row['worker_slot'] = int(row['worker_slot'])
            rows.append(row)
    return rows


def analyze(rows):
    """Статистика по worker_slot и по порядку запросов."""
    by_slot = defaultdict(lambda: {'durations': [], 'success': [], 'count': 0})
    for i, r in enumerate(rows):
        s = r['worker_slot']
        by_slot[s]['durations'].append(r['duration_sec'])
        by_slot[s]['success'].append(r['success'])
        by_slot[s]['count'] += 1

    slot_stats = {}
    for s in sorted(by_slot.keys()):
        d = by_slot[s]['durations']
        succ = by_slot[s]['success']
        n = len(d)
        slot_stats[s] = {
            'count': n,
            'mean': sum(d) / n,
            'min': min(d),
            'max': max(d),
            'success_rate': sum(succ) / n * 100,
            'median': sorted(d)[n // 2],
        }
        if n >= 2:
            sorted_d = sorted(d)
            slot_stats[s]['p95'] = sorted_d[int(0.95 * n)] if n >= 20 else max(d)
        else:
            slot_stats[s]['p95'] = max(d)

    # Анализ по порядку: разбиваем на блоки по 50 запросов
    block_size = 50
    by_block = []
    for start in range(0, len(rows), block_size):
        block = rows[start:start + block_size]
        if not block:
            continue
        durations = [r['duration_sec'] for r in block]
        by_block.append({
            'block': start // block_size,
            'start_row': start,
            'mean': sum(durations) / len(durations),
            'max': max(durations),
            'count': len(durations),
        })

    return slot_stats, by_block, rows


def print_report(slot_stats, by_block):
    """Текстовый отчёт."""
    print("=" * 60)
    print("СТАТИСТИКА ПО ПОТОКАМ (worker_slot)")
    print("=" * 60)
    print("worker_slot 0 = direct, 1-4 = proxy")
    print()

    slowest_slot = max(slot_stats.items(), key=lambda x: x[1]['mean'])
    fastest_slot = min(slot_stats.items(), key=lambda x: x[1]['mean'])

    for s in sorted(slot_stats.keys()):
        st = slot_stats[s]
        conn = "direct" if s == 0 else "proxy"
        print(f"  Поток {s} ({conn}):")
        print(f"    запросов: {st['count']}, успех: {st['success_rate']:.1f}%")
        print(f"    длительность: средняя {st['mean']:.2f} с, мин {st['min']:.2f}, макс {st['max']:.2f}, медиана {st['median']:.2f}, p95 {st['p95']:.2f} с")
        print()

    print("ВЫВОДЫ:")
    print(f"  Самый медленный поток: {slowest_slot[0]} (средняя {slowest_slot[1]['mean']:.2f} с)")
    print(f"  Самый быстрый поток:  {fastest_slot[0]} (средняя {fastest_slot[1]['mean']:.2f} с)")
    print()

    if by_block:
        block_means = [b['mean'] for b in by_block]
        worst_block = max(enumerate(block_means), key=lambda x: x[1])
        best_block = min(enumerate(block_means), key=lambda x: x[1])
        print("ПО ВРЕМЕНИ (блоки по 50 запросов):")
        print(f"  Худший блок: №{worst_block[0]} (строки ~{worst_block[0]*50}-{worst_block[0]*50+50}), средняя {worst_block[1]:.2f} с")
        print(f"  Лучший блок: №{best_block[0]}, средняя {best_block[1]:.2f} с")
        if len(block_means) >= 2 and block_means[-1] > block_means[0] * 1.1:
            print("  Тренд: к концу прогона средняя длительность выросла — возможна деградация скорости.")
        elif len(block_means) >= 2:
            print("  Тренд: значительного роста времени к концу не видно.")


def plot_charts(rows, slot_stats, by_block, out_prefix="D:\\BS\\AGNumbers\\booking\\timing"):
    """Построение графиков."""
    if not HAS_MATPLOTLIB:
        print("Для графиков установите: pip install matplotlib")
        return

    fig, axes = plt.subplots(2, 2, figsize=(12, 10))

    # 1) Средняя длительность по потокам
    ax = axes[0, 0]
    slots = sorted(slot_stats.keys())
    means = [slot_stats[s]['mean'] for s in slots]
    colors = ['#2ecc71' if s == 0 else '#3498db' for s in slots]
    bars = ax.bar([str(s) for s in slots], means, color=colors)
    ax.set_xlabel('Поток (worker_slot)')
    ax.set_ylabel('Средняя длительность (с)')
    ax.set_title('Средняя длительность по потокам (0=direct, 1-4=proxy)')
    for b, v in zip(bars, means):
        ax.text(b.get_x() + b.get_width()/2, b.get_height() + 0.3, f'{v:.1f}', ha='center', fontsize=9)

    # 2) Box plot по потокам (распределение)
    ax = axes[0, 1]
    by_slot = defaultdict(list)
    for r in rows:
        by_slot[r['worker_slot']].append(r['duration_sec'])
    data = [by_slot[s] for s in slots]
    bp = ax.boxplot(data, tick_labels=[str(s) for s in slots], patch_artist=True)
    for i, patch in enumerate(bp['boxes']):
        patch.set_facecolor('#2ecc71' if slots[i] == 0 else '#3498db')
    ax.set_xlabel('Поток (worker_slot)')
    ax.set_ylabel('Длительность (с)')
    ax.set_title('Распределение длительности по потокам')

    # 3) Динамика по блокам (когда начинаются проблемы)
    ax = axes[1, 0]
    blocks = [b['block'] for b in by_block]
    block_means = [b['mean'] for b in by_block]
    ax.plot(blocks, block_means, 'o-', color='#e74c3c', markersize=6)
    ax.set_xlabel('Блок (каждый = 50 запросов)')
    ax.set_ylabel('Средняя длительность в блоке (с)')
    ax.set_title('Динамика скорости по ходу прогона')
    ax.grid(True, alpha=0.3)

    # 4) Все запросы: длительность от номера строки, по потокам
    ax = axes[1, 1]
    for s in sorted(set(r['worker_slot'] for r in rows)):
        indices = [i for i, r in enumerate(rows) if r['worker_slot'] == s]
        durs = [rows[i]['duration_sec'] for i in indices]
        label = f'Поток {s}' + (' (direct)' if s == 0 else '')
        ax.scatter(indices, durs, alpha=0.4, s=8, label=label)
    ax.set_xlabel('Номер запроса (порядок)')
    ax.set_ylabel('Длительность (с)')
    ax.set_title('Длительность каждого запроса по потокам')
    ax.legend(loc='upper right', fontsize=7)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(out_prefix + '_charts.png', dpi=120, bbox_inches='tight')
    print(f"Графики сохранены: {out_prefix}_charts.png")
    plt.close()


def main():
    path = r"D:\BS\AGNumbers\booking\timing.csv"
    rows = load_csv(path)
    if not rows:
        print("Нет данных в файле")
        return

    slot_stats, by_block, _ = analyze(rows)
    print_report(slot_stats, by_block)
    plot_charts(rows, slot_stats, by_block)


if __name__ == "__main__":
    main()
