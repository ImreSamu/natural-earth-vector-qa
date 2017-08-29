.headers off
.nullvalue NULL


.print ''
.print '# Debug report - no match '
.print ''
.print ' _score | _status  | ne_xid |  _wikidata_status | _geonames_status  '
.print ' --------- | -------- | ------ | ----------------- | ----------------- '
SELECT _score, _status , ne_xid, _wikidata_status, _geonames_status
FROM   wd_match
WHERE  _score < 40
ORDER BY _score

;

