#!/usr/bin/env python3
import sys
from datetime import date, timedelta

def month_last_day(y, m):
    if m == 12:
        return date(y, 12, 31)
    return date(y, m + 1, 1) - timedelta(days=1)

def months_between(start, end):
    y1, m1 = map(int, start.split("-"))
    y2, m2 = map(int, end.split("-"))
    y, m = y1, m1
    while (y < y2) or (y == y2 and m <= m2):
        yield y, m
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: month_spans.py START_YYYY-MM END_YYYY-MM")
        sys.exit(1)
    s, e = sys.argv[1], sys.argv[2]
    for y, m in months_between(s, e):
        start = date(y, m, 1)
        endd = month_last_day(y, m)
        print(f"{start.day}/{start.month}/{start.year},{endd.day}/{endd.month}/{endd.year},{y}-{m:02d}")
