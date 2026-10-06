"""Реализуйте только преобразование строк в агрегаты по клиенту."""
from pyspark.sql import DataFrame


def customer_features(rows: DataFrame) -> DataFrame:
    """Верните customer_id, order_count, total_amount, average_order_amount.

    Вход: восемь строковых колонок нормализованного CSV.
    Выходные типы: string, long, decimal(20,6), decimal(20,2).
    Подробные правила отбора и округления — в README.md.
    """
    raise NotImplementedError("HW2: реализуйте отбор покупок и агрегирование")
