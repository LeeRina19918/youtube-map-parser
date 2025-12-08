import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Фільтрація каналів за кластером і кількістю підписників"
    )
    parser.add_argument(
        "--cluster",
        required=True,
        help="Назва кластера (значення стовпчика clusterName)",
    )
    parser.add_argument(
        "--min-subs",
        type=int,
        default=0,
        help="Мінімальна кількість підписників",
    )
    parser.add_argument(
        "--input",
        type=str,
        default="channels_all.csv",
        help="Вхідний CSV-файл з усіма каналами",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Ім'я вихідного CSV (якщо не вказати — згенерується автоматично)",
    )
    return parser.parse_args()


def slugify(text: str) -> str:
    """Перетворює назву кластера на безпечне ім'я файла."""
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789_"
    text = text.lower().replace(" ", "_")
    return "".join(ch for ch in text if ch in allowed)


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Не знайдено файл {input_path}")

    # Читаємо всі канали
    df = pd.read_csv(input_path)

    # subscribersCount у нас текстом — перетворюємо на число
    if "subscribersCount" in df.columns:
        df["subscribersCount"] = (
            pd.to_numeric(df["subscribersCount"], errors="coerce").fillna(0).astype(int)
        )
    else:
        raise SystemExit("У файлі немає стовпчика 'subscribersCount'")

    if "clusterName" not in df.columns:
        raise SystemExit("У файлі немає стовпчика 'clusterName'")

    # Фільтр по кластеру та кількості підписників
    mask = (df["clusterName"] == args.cluster) & (
        df["subscribersCount"] >= args.min_subs
    )
    filtered = df.loc[mask].copy()

    if filtered.empty:
        print(
            f"Немає каналів у кластері '{args.cluster}' "
            f"з підписниками ≥ {args.min_subs}"
        )
    else:
        print(
            f"Знайдено {len(filtered)} канал(ів) "
            f"у кластері '{args.cluster}' з підписниками ≥ {args.min_subs}"
        )

    # Визначаємо назву файлу
    if args.output:
        output_path = Path(args.output)
    else:
        slug = slugify(args.cluster)
        output_path = Path(f"filtered_{slug}_min{args.min_subs}.csv")

    # Записуємо CSV (навіть якщо порожній — щоб було видно, що фільтр відпрацював)
    filtered.to_csv(output_path, index=False)
    print(f"Результат збережено у файл: {output_path}")


if __name__ == "__main__":
    main()
