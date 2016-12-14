#!/usr/bin/env python2

from __future__ import division

import sys
import json
import datetime
import math
import subprocess

from wowah import Auction, db_connect

DB = db_connect(sys.argv[1])
GOLD_QUOTIENT = 10000

LOOK_BACK_DAYS  = int(sys.argv[2])
PCT_LARGE       = 0.75
PCT_SMALL       = 0.25
PCT_HIGH        = 0.90
PCT_LOW         = 0.10

STMT_SAMPLE_COUNT = """
SELECT DISTINCT
        started_at
    FROM auction
    WHERE
        started_at >= ?
;
"""
STMT_QUANTITY_STATS = """
SELECT DISTINCT
        item_id
        , max(quantity)
        , count(*)
        , sum(quantity)
        , sum(buyout)
    FROM auction
    WHERE
        started_at >= ?
        AND buyout IS NOT NULL
    GROUP BY item_id
;
"""
STMT_PERCENTILE_STATS = """
SELECT
        (CAST(buyout AS float) / quantity) / {gold_q:d} AS buyout_ppq
    FROM auction
    WHERE
        started_at >= ?
        AND item_id = ?
        AND buyout IS NOT NULL
        AND quantity {q_cmp} ?
    ORDER BY buyout_ppq
    LIMIT 1
    OFFSET CAST(((
        SELECT
                count(*)
            FROM auction
            WHERE
                started_at >= ?
                AND item_id = ?
                AND buyout IS NOT NULL
                AND quantity {q_cmp} ?
        ) * ?) AS int)
;
"""

def get_item_name(item_id):
    return subprocess.check_output(['./contrib/get-item-name.sh', str(item_id)]).strip()

START_TIME = datetime.datetime.utcnow() - datetime.timedelta(days=LOOK_BACK_DAYS)
SAMPLE_COUNT = len(Auction.select(Auction.started_at).distinct().where(Auction.started_at >= START_TIME))
for item_id, mss, avol, qvol, tbo in DB.execute_sql(STMT_QUANTITY_STATS, params=(START_TIME,)):
    if      mss == 1 \
            or mss > 50 \
            or mss % 5 != 0 \
            or avol < SAMPLE_COUNT * 2:
        # this is not an item we care about
        continue

    try:
        large_high = DB.execute_sql(STMT_PERCENTILE_STATS.format(q_cmp='>', gold_q=GOLD_QUOTIENT),
            (START_TIME, item_id, int(mss * PCT_LARGE),
            START_TIME, item_id, int(mss * PCT_LARGE), PCT_HIGH)).fetchall()[0][0]
        large_low = DB.execute_sql(STMT_PERCENTILE_STATS.format(q_cmp='>', gold_q=GOLD_QUOTIENT),
            (START_TIME, item_id, int(mss * PCT_LARGE),
            START_TIME, item_id, int(mss * PCT_LARGE), PCT_LOW)).fetchall()[0][0]
        large_diff = large_high - large_low
        large_avg = large_high + large_low / 2

        small_high = DB.execute_sql(STMT_PERCENTILE_STATS.format(q_cmp='<', gold_q=GOLD_QUOTIENT),
            (START_TIME, item_id, int(mss * PCT_SMALL),
            START_TIME, item_id, int(mss * PCT_SMALL), PCT_HIGH)).fetchall()[0][0]
        small_low = DB.execute_sql(STMT_PERCENTILE_STATS.format(q_cmp='<', gold_q=GOLD_QUOTIENT),
            (START_TIME, item_id, int(mss * PCT_SMALL),
            START_TIME, item_id, int(mss * PCT_SMALL), PCT_LOW)).fetchall()[0][0]
        small_diff = small_high - small_low
        small_avg = small_high + small_low / 2

        avg_bo_ppq = (tbo / GOLD_QUOTIENT) / qvol
        bucket_diff = abs(large_avg - small_avg)
        bucket_avg = (small_avg + large_avg) / 2
    except IndexError as err:
        continue

    qscore = 1.0 - (qvol / avol) / mss
    vscore = math.log(avol / SAMPLE_COUNT)
    pscore = math.log(bucket_diff * (bucket_avg / avg_bo_ppq))

    sys.stdout.write(','.join(str(e) for e in [
        item_id, get_item_name(item_id),
        qscore, vscore, pscore, \
        (qscore * vscore * pscore), (qscore * (pscore / vscore))
    ]) + '\n')

    sys.stderr.write(' '.join(str(e) for e in [
        item_id, \
        '|{:3.1f} < {:3.1f}+/-{:3.1f} < {:3.1f}|'.format(small_low, small_avg, small_diff, small_high), \
        '<< {:3.1f} ({:3.1f}) <<'.format(avg_bo_ppq, bucket_avg), \
        '|{:3.1f} < {:3.1f}+/-{:3.1f} < {:3.1f}|'.format(large_low, large_avg, large_diff, large_high)
    ]) + '\n')
