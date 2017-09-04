.headers off
.nullvalue NULL

PRAGMA cache_size = -2000000; 
-- PRAGMA default_cache_size = -2000000;
PRAGMA count_changes = OFF;
PRAGMA journal_mode = OFF;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA synchronous = OFF;
PRAGMA temp_store = MEMORY;




.print ' importing 1342-candidates-cleaned.md to  candidates  '
.separator ","
DROP TABLE IF EXISTS candidates;
.import 1342-candidates-cleaned.csv candidates
PRAGMA table_info(candidates);
SELECT  * FROM  candidates LIMIT 4;
.print ' candidates records: '
SELECT  count(*) candidates_records FROM  candidates;


.print 'STEP: indexing '
CREATE INDEX IF NOT EXISTS  wd_index  ON wd ( ne_xid );


.print  'STEP: _temp_wd'

DROP TABLE IF EXISTS _temp_wd;
CREATE TABLE _temp_wd AS
SELECT ne_xid,wd_id,_score
FROM  wd 
ORDER BY  ne_xid, _score DESC		
;		


DROP INDEX IF EXISTS _temp_wd_index;
CREATE INDEX  _temp_wd_index  ON _temp_wd ( ne_xid, _score DESC );



--
----------------------------------------------------------------------------------
--
.print  'STEP: wd_maxscore'
DROP TABLE IF EXISTS wd_maxscore;
CREATE TABLE wd_maxscore AS
SELECT *
FROM  wd wd1
WHERE ne_xid || '@' || wd_id  in
    (
        SELECT wd2.ne_xid || '@' || wd2.wd_id
        FROM   _temp_wd wd2
        WHERE  wd2.ne_xid = wd1.ne_xid
        ORDER BY _score DESC
        LIMIT 1
    )
ORDER BY  ne_xid		
;		

DROP INDEX IF EXISTS wd_maxscore_index;
CREATE UNIQUE INDEX  wd_maxscore_index  ON wd_maxscore ( ne_xid );

.print '------------'

--
------------------------------------------------------------------------
--
.print  'STEP: wd_top3'
DROP TABLE IF EXISTS wd_top3;
CREATE TABLE wd_top3 AS

WITH top3 AS
(
SELECT *
FROM wd wd1
WHERE   ne_xid || '@' || wd_id  in
        (
        SELECT wd2.ne_xid || '@' || wd2.wd_id
        FROM _temp_wd  wd2
        WHERE wd2.ne_xid = wd1.ne_xid
        ORDER BY _score DESC
        LIMIT 3
        )
order by ne_xid, _score	DESC
)
SELECT  top3.ne_xid as ne_xid
      ,count(*)                                      AS _top3_n
      ,SUM( top3._score >=  wd_maxscore._score*0.8 ) AS _top20percent_n   
      ,group_concat(round(top3._score,2) ,'#')       AS _top3_score
      ,group_concat(top3.wd_id           ,'#')       AS _top3_wikidataid
FROM top3
LEFT JOIN wd_maxscore ON top3.ne_xid=wd_maxscore.ne_xid
GROUP BY top3.ne_xid
ORDER BY top3.ne_xid
;
DROP INDEX IF EXISTS wd_top3_index;
CREATE UNIQUE INDEX  wd_top3_index  ON wd_top3 ( ne_xid );




.print  'STEP: wd_maxscore_top3'
DROP TABLE IF EXISTS wd_maxscore_top3;
CREATE TABLE  wd_maxscore_top3 AS
SELECT *
FROM wd_maxscore
LEFT JOIN wd_top3 ON wd_maxscore.ne_xid = wd_top3.ne_xid
ORDER BY ne_xid
;
DROP INDEX IF EXISTS  wd_maxscore_top3_index;
CREATE UNIQUE INDEX  wd_maxscore_top3_index  ON  wd_maxscore_top3 ( ne_xid );


.print  'STEP: wd_match'

DROP VIEW IF EXISTS wd_match;
CREATE VIEW wd_match AS
SELECT 
	    CASE
			WHEN _score > 120 and wd_sitelink_en!='' and  _top20percent_n=1  and  wd_distance < 20   THEN  "F1_OK"       
			WHEN _score > 90  and wd_sitelink_en!='' and wd_distance < 50                            THEN  "F2_GOOD"           
			WHEN _score > 60  and wd_sitelink_en!=''                                                 THEN  "F3_MAYBE"                                
            ELSE                                                                                           "F9_BAD"
		END AS _mstatus  
        ,
	    CASE
			WHEN ne_wikidataid !=''  
              -- THEN  "["||ne_wikidataid||"](https://www.wikidata.org/wiki/"||ne_wikidataid||")"
              THEN  "https://www.wikidata.org/wiki/"||ne_wikidataid              
            ELSE  ""
		END AS ne_wikidataid_url  
	   ,CASE
			WHEN wd_id !=''  
              -- THEN  "["||wd_id||"](https://www.wikidata.org/wiki/"||wd_id||")"
              THEN  "https://www.wikidata.org/wiki/"||wd_id
            ELSE  ""
		END AS wd_id_url          
	   ,CASE
			WHEN ne_wikidataid !=''  THEN  "NE0Y-has a wikidataid   "
			                         ELSE  "NE1N-wikidata id missing"
		END AS _ne_already_has_wikidataid
	   ,CASE
       		WHEN _score >= 150 THEN "S0-Exclent match         ( _score > 150) "
			WHEN _score >= 120 THEN "S1-Very good match       ( 120   -  150) "
			WHEN _score >=  90 THEN "S2-Good match            ( 90    -  120) "
			WHEN _score >=  60 THEN "S3-Medium match          ( 60    -   90) "            
			WHEN _score >=  30 THEN "S4-Maybe                 ( 30    -   60) "
			ELSE                    "S5-Not found in wikidata ( score <   30) "
		END AS _status
	   ,CASE
			WHEN _top20percent_n =0 THEN "CN           "       
			WHEN _top20percent_n =1 THEN "C1-clear win "
			WHEN _top20percent_n =2 THEN "C2-check top2"
			WHEN _top20percent_n =3 THEN "C3-check top3"
			ELSE                         "CX-ERROR"
		END AS _chekstatus     
	   ,CASE
			WHEN -1.0 <  wd_distance  and wd_distance  <=   5.0 THEN "D00-05 km "       
			WHEN  5.0 <  wd_distance  and wd_distance  <=  10.0 THEN "D05-10 km "
			WHEN 10.0 <  wd_distance  and wd_distance  <=  20.0 THEN "D10-20 km "
			WHEN 20.0 <  wd_distance  and wd_distance  <=  30.0 THEN "D20-30 km "  
			WHEN 30.0 <  wd_distance  and wd_distance  <=  40.0 THEN "D30-40 km "
			WHEN 40.0 <  wd_distance  and wd_distance  <=  50.0 THEN "D40-50 km "   
			WHEN 50.0 <  wd_distance  and wd_distance  <=  60.0 THEN "D50-60 km "  
			WHEN 60.0 <  wd_distance  and wd_distance  <=  70.0 THEN "D60-70 km "       
			WHEN 70.0 <  wd_distance  and wd_distance  <=  80.0 THEN "D70-80 km "
			WHEN 80.0 <  wd_distance  and wd_distance  <=  90.0 THEN "D80-90 km "    
			WHEN 90.0 <  wd_distance  and wd_distance  <= 100.0 THEN "D90-100km "                                                                        
			WHEN 100.0<  wd_distance                            THEN "D100-  km "                                                      
			ELSE                                                     "DX ERROR  "
		END AS _distancestatus                       
	  ,*
FROM wd_maxscore_top3
;





.print  'STEP: _wd_match_'

DROP   VIEW IF EXISTS _wd_match_f1_ok;
CREATE VIEW           _wd_match_f1_ok AS
    SELECT *
    FROM wd_match
    WHERE _mstatus="F1_OK";

DROP   VIEW IF EXISTS _wd_match_f2_good;
CREATE VIEW           _wd_match_f2_good AS
    SELECT *
    FROM wd_match
    WHERE _mstatus="F2_GOOD";

DROP   VIEW IF EXISTS _wd_match_f3_maybe;
CREATE VIEW           _wd_match_f3_maybe AS
    SELECT *
    FROM wd_match
    WHERE _mstatus="F3_MAYBE";

DROP   VIEW IF EXISTS _wd_match_wikidataid_diffs;
CREATE VIEW           _wd_match_wikidataid_diffs AS
    SELECT *
    FROM wd_match
    WHERE (_mstatus="F1_OK" or _mstatus="F2_GOOD") and _wikidata_status='NE';

DROP   VIEW IF EXISTS _wd_match_wikidataid_new;
CREATE VIEW           _wd_match_wikidataid_new AS
    SELECT *
    FROM wd_match
    WHERE (_mstatus="F1_OK" or _mstatus="F2_GOOD") and _wikidata_status='Na';

DROP   VIEW IF EXISTS _wd_match_wikidataid_validated;
CREATE VIEW           _wd_match_wikidataid_validated AS
    SELECT *
    FROM wd_match
    WHERE (_mstatus="F1_OK" or _mstatus="F2_GOOD") and _wikidata_status='EQ';

DROP   VIEW IF EXISTS _wd_match_geodataname_diffs;
CREATE VIEW           _wd_match_geodataname_diffs AS
    SELECT *
    FROM wd_match
    WHERE (_mstatus="F1_OK" or _mstatus="F2_GOOD") and _geonames_status='NE';


DROP   TABLE IF EXISTS _1342_diffs ;
CREATE TABLE          _1342_diffs AS
SELECT candidates.fid, candidates.name, candidates.wd_wiki_id, 
       wd_match.wd_id, wd_match.wd_label, wd_match.wd_distance,  wd_match._score,   wd_match.wd_sitelink_en, wd_match.wd_description,wd_match.wd_type
FROM candidates
LEFT JOIN wd_match  ON wd_match.ne_fid||wd_match.ne_name = candidates.fid||candidates.name  
WHERE wd_match._score > 60  and  wd_wiki_id != wd_id and wd_match.wd_distance < 50 
		and fid not in ( 1775 )
;

.print '  Problematic records by ADM0NAME'
.print ' --------------------------------------'
select ne_adm0name, count(*) as n from wd_match where _score < 30 group by 1 having n > 2 order by n desc; 
.print 


.print '  _mstatus FREQ '
.print ' --------------------------------------'
select _mstatus, count(*) as n from wd_match  group by 1  order by 1 ; 
.print 

.print ' -- End of postprocessing '