#!/usr/bin/gnuplot -p

set datafile separator '|'
set timefmt '%Y-%m-%d %H:%M:%S'
set xdata time
set xlabel 'Time'
set ylabel 'Price (Gold)'
set title 'Price History'
set autoscale x
set grid

plot "< sed \"s/ITEM_ID/$ITEM_ID/\" contrib/item-price-history.sql | sqlite3 $DB_FILE" using 2:1
