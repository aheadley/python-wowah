#!/usr/bin/gnuplot -p

set datafile separator '|'
set timefmt '%Y-%m-%d %H:%M:%S'
set xdata time
set xlabel 'Time'
set ylabel 'Price (Gold)'
set title 'Price History'
set autoscale x
set grid

#set palette rgb -21,-22,-23
#set palette negative

set palette maxcolors 100
set palette defined (0 "red", 99 "blue")


plot "< sed \"s/ITEM_ID/$ITEM_ID/\" contrib/item-price-history.sql | sqlite3 $DB_FILE" \
    using 2:1:($3 / 10):4 \
    with points pt 7 ps variable lc palette
