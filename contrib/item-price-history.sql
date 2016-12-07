select
        (CAST(buyout as float) / quantity) / 10000 as bo_ppq,
        datetime(
            strftime('%s', ended_at)
                + (random() % (60 * 30 - 1)),
            'unixepoch') as ts,
        CAST(quantity as float) / (select max(quantity) from auction where item_id = ITEM_ID) as qs,
        (strftime('%s', ended_at) - strftime('%s', started_at)) / 3600 as runtime
    from auction
    where item_id = ITEM_ID
        and buyout is not NULL
        and ended_at is not NULL
        and bo_ppq < 2 * (select avg(CAST(buyout as float) / quantity / 10000) as bo_ppq_avg
            from auction
            where item_id = ITEM_ID
                and buyout is not NULL
                and ended_at is not NULL
            )
    order by ts asc
;
