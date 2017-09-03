#!/bin/bash
# -exu

# first argument is:  "var1|var2|var3"
# example     ./proc_report_freq.sh     "_top3_n|_top20percent_n|_chekstatus"

[ $# -lt 1 ] && echo "Need grp_variables as first argument!" && exit 1

vars=$1
number_of_colum_sep=$(grep -o "|" <<< "$vars" | wc -l) 
nsep=$(expr $number_of_colum_sep + 2 )
[ $nsep -lt 1 ] && echo "internal parameter error" && exit 1
sql_vars="$( echo "$vars" | sed  's/|/,/g'  )"
s0="---"

s1=$( seq -s"|---" $nsep  |tr -d '[:digit:]' )
header_separator="$s0 $s1"

header_vars="$vars|N"

# echo "number_of_colum_sep = $number_of_colum_sep"
# echo "header_separator    = $header_separator"
# echo "sql_vars            = $sql_vars"

sqlite3 -batch wikidata_naturalearth_qa.db <<EOF
.headers off
.nullvalue NULL
.print
.print FREQ REPORT: $1
.print
.print $header_vars
.print $header_separator
SELECT $sql_vars , count(*) AS N
FROM wd_match
GROUP BY $sql_vars
ORDER BY $sql_vars
;
EOF


# sqlite3 -header -csv wikidata_naturalearth_qa.db "select * from wd_match;" > wd_match.csv
