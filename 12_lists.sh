#!/bin/bash

#./proc_report_list.sh  "ne_xid,  ne_wikidataid, wd_id, wd_label, wd_description"  "where _score > 100 and  ne_wikidataid=''"

./proc_report_list.sh  "ne_xid,  ne_wikidataid, wd_id, wd_label, wd_description" \
                            "where _score > 120 and  ne_wikidataid!=''  and ne_wikidataid!=wd_id"    > _wikidataid_diffs.md



#  New wikidata ids !
./proc_report_list.sh \
    "ne_xid, ne_wikidataid, ne_name, ne_adm0name, ne_adm1name, wd_id, wd_label, wd_description, wd_type, wd_countrylabel, wd_sitelink_en" \
    "where _score > 120 and  ne_wikidataid=='' and substr(_chekstatus,1,2)='C1'  and  wd_distance < 15 ;"    > _new_wikidataid.md
