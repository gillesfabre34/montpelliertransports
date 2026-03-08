"""
E — Rolling window (sum or avg over last N rows or last N time units)

STATEMENT
--------
For each row, compute a metric over a "rolling" window: e.g. sum of amount over the last 3 rows
(in order), or sum over all rows in the last N seconds. Used for: moving average, "last 7 days" metrics.

Input:  list of records ordered by time (or with timestamp), and window size (e.g. 3 rows or 86400 seconds).
Output: same records with an extra field e.g. rolling_sum or rolling_avg.
"""
from rich import print

# Daily sales. For each day, compute the sum of amount over the last 3 days (including current).
SALES = [
    {"date": "2024-01-01", "amount": 100},
    {"date": "2024-01-02", "amount": 120},
    {"date": "2024-01-03", "amount": 80},
    {"date": "2024-01-04", "amount": 150},
    {"date": "2024-01-05", "amount": 90},
]
# Expected (window=3): day1: 100; day2: 100+120=220; day3: 100+120+80=300; day4: 120+80+150=350; day5: 80+150+90=320.


def rolling_sum(
    rows: list[dict],
    order_key: str,
    value_key: str,
    window_size: int,
) -> list[dict]:
    """
    Add rolling_sum: sum of value_key over the last window_size rows (including current), ordered by order_key.

    Args:
        rows: list of dicts with order_key and value_key.
        order_key: e.g. "date" (rows assumed sorted by this).
        value_key: e.g. "amount".
        window_size: number of rows in the window (e.g. 3).

    Returns:
        list of dicts with same fields plus "rolling_sum".
    """
    raise NotImplementedError


if __name__ == "__main__":
    print(rolling_sum(SALES, "date", "amount", 3))
