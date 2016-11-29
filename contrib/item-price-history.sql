select
        (CAST(buyout as float) / quantity) / 10000 as bo_ppq,
        datetime(
            strftime('%s', ended_at)
                + (random() % (60 * 30 - 1)),
            'unixepoch') as ts
    from auction
    where item_id = ITEM_ID
        and buyout is not NULL
        and ended_at is not NULL
    order by ts asc
;
