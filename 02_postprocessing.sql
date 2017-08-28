.headers off
.nullvalue NULL

CREATE INDEX IF NOT EXISTS  wd_index  ON wd ( ne_xid );

DROP TABLE IF EXISTS wd_maxscore;
CREATE TABLE wd_maxscore AS
	SELECT ne_xid, MAX(_score) as max_score
	FROM wd
	GROUP BY ne_xid
	ORDER BY ne_xid
;
CREATE UNIQUE INDEX IF NOT EXISTS  ne_xid_index  ON wd_maxscore ( ne_xid );


DROP TABLE IF EXISTS wd_match;
CREATE TABLE wd_match AS
SELECT wd_maxscore.ne_xid      as ne_xid
      ,wd.ne_wikidataid        as ne_wikidataid
      ,wd_maxscore.max_score   as max_score
	  ,CASE
	     WHEN wd_maxscore.max_score >= 120 THEN "S1-Very good match       ( _score > 120) "
	     WHEN wd_maxscore.max_score >=  90 THEN "S2-Good match            ( 90    -  120) "
	     WHEN wd_maxscore.max_score >=  40 THEN "S3-Maybe                 ( 40    -   90) "
		 ELSE                                   "S4-Not found in wikidata ( score <   40) "
		END AS _status
	  ,wd.wd_id    as wd_id
	  ,wd.wd_label as wd_label
	  ,wd._wikidata_status as _wikidata_status
	  ,wd._geonames_status as _geonames_status
	  ,wd.wd_distance      as wd_distance
FROM wd_maxscore
     LEFT JOIN wd  ON  wd.ne_xid=wd_maxscore.ne_xid  AND wd._score=wd_maxscore.max_score
;

.print ''
.print '# Summary report v1'
.print ''
.print '_status  | _wikidata_status | _geonames_status | n  '
.print '-------- | -----------------| -----------------| -- '
SELECT _status , _wikidata_status, _geonames_status,  count(*) as n
FROM   wd_match
GROUP BY _status , _wikidata_status, _geonames_status
ORDER BY _status , _wikidata_status, _geonames_status
;


.print ''
.print '# Summary report v2'
.print ''
.print '_status  | n '
.print '-------- | --'
SELECT _status , count(*) as n
FROM   wd_match
GROUP BY _status
ORDER BY _status
;


.print ''
.print ''
.print ' -- End of postprocessing '
