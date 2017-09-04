#!/bin/bash

./proc_report_freq.sh     "_ne_already_has_wikidataid | _status  | _chekstatus | _wikidata_status | _geonames_status"

./proc_report_freq.sh     "_status "

./proc_report_freq.sh     "_ne_already_has_wikidataid | _status  "

./proc_report_freq.sh     "_ne_already_has_wikidataid | _status |_chekstatus  "

./proc_report_freq.sh     " _status |_chekstatus  "

./proc_report_freq.sh     "_top3_n|_top20percent_n|_chekstatus"

./proc_report_freq.sh     "_distancestatus"

./proc_report_freq.sh     "_status | _distancestatus"

./proc_report_freq.sh     "ne_scalerank | _status "

./proc_report_freq.sh     "ne_scalerank | _ne_already_has_wikidataid  "

./proc_report_freq.sh     "wd_type "

./proc_report_freq.sh     "_step | _status"

