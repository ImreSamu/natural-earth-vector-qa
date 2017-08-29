.headers off
.nullvalue NULL

CREATE INDEX IF NOT EXISTS  wd_index  ON wd ( ne_xid );

DROP TABLE IF EXISTS wd_maxscore;
CREATE TABLE wd_maxscore AS
select  *
from    wd wd1
where   ne_xid || '@' || wd_id  in
        (
        select ne_xid || '@' || wd_id
        from    wd wd2
        where   ne_xid = wd1.ne_xid
        order by _score desc
        limit   1
        )
order by  ne_xid		
;		

DROP INDEX IF EXISTS wd_maxscore_index;
CREATE UNIQUE INDEX  wd_maxscore_index  ON wd_maxscore ( ne_xid );


DROP TABLE IF EXISTS wd_match;
CREATE TABLE wd_match AS
SELECT 
	   CASE
			WHEN _score >= 120 THEN "S1-Very good match       ( _score > 120) "
			WHEN _score >=  90 THEN "S2-Good match            ( 90    -  120) "
			WHEN _score >=  40 THEN "S3-Maybe                 ( 40    -   90) "
			ELSE                    "S4-Not found in wikidata ( score <   40) "
		END AS _status
	  ,*
FROM wd_maxscore
;

DROP INDEX IF EXISTS wd_match_index;
CREATE UNIQUE INDEX wd_match_index  ON wd_match ( ne_xid );




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
