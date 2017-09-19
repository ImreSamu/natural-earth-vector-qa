.headers off
.nullvalue NULL

PRAGMA cache_size = -3000000;
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


.print  'STEP: wd_geonamesstatus__maxscore'
DROP TABLE IF EXISTS wd_geonamesstatus__maxscore;
CREATE TABLE wd_geonamesstatus__maxscore AS
WITH geoeg AS (
    SELECT ne_xid
          ,wd_id   
          ,_score 
    FROM  wd 
    WHERE _geonames_status ='EQ'
)
SELECT ne_xid
      ,wd_id             AS _geonames_wd_id 
      ,_score            AS _geonames_score
FROM  geoeg qwd1
WHERE ne_xid || '@' || wd_id  in
    (
        SELECT qwd2.ne_xid || '@' || qwd2.wd_id
        FROM   geoeg qwd2
        WHERE  qwd2.ne_xid = qwd1.ne_xid 
        ORDER BY _score DESC
        LIMIT 1
    )
ORDER BY  ne_xid
;


DROP INDEX IF EXISTS wd_geonamesstatus__maxscore_index;
CREATE UNIQUE INDEX  wd_geonamesstatus__maxscore_index  ON wd_geonamesstatus__maxscore ( ne_xid );

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
	,CASE
			WHEN _geonames_wd_id !=''  THEN  "https://www.wikidata.org/wiki/"||_geonames_wd_id
            ELSE  ""
	END AS _geonames_wd_id_url

	,CASE 
       WHEN _top3_n=2 THEN  "https://www.wikidata.org/wiki/"||substr(_top3_wikidataid,instr(_top3_wikidataid,'#')+1 )
       WHEN _top3_n=3 THEN  "https://www.wikidata.org/wiki/"||substr( 
		 substr(_top3_wikidataid,instr(_top3_wikidataid,'#')+1 )
		,0
		,instr( substr(_top3_wikidataid,instr(_top3_wikidataid,'#')+1 ),'#')
	   )        
     ELSE '' 
     END AS _top2_second_best_wd_id

	,CASE WHEN _top3_n>=3 THEN "https://www.wikidata.org/wiki/"||substr( 
		 substr(_top3_wikidataid,instr(_top3_wikidataid,'#')+1 )
		,instr( substr(_top3_wikidataid,instr(_top3_wikidataid,'#')+1 ),'#')+1
	 )
     ELSE '' 
     END AS _top3_third_best_wd_id

FROM wd_maxscore
LEFT JOIN wd_top3                       
          ON wd_maxscore.ne_xid  =  wd_top3.ne_xid
LEFT JOIN wd_geonamesstatus__maxscore   
          ON wd_maxscore.ne_xid  =  wd_geonamesstatus__maxscore.ne_xid 
             AND wd_maxscore.wd_id  !=  wd_geonamesstatus__maxscore._geonames_wd_id 
             AND wd_geonamesstatus__maxscore._geonames_score > -500
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
			WHEN _score > 75  and wd_sitelink_en!=''                                                 THEN  "F3_MEDIUM"
			WHEN _score > 75                                                                         THEN  "F4_MAYBE"            
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

DROP   VIEW IF EXISTS _wd_match_f3_medium;
CREATE VIEW           _wd_match_f3_medium AS
    SELECT *
    FROM wd_match
    WHERE _mstatus="F3_MEDIUM";

DROP   VIEW IF EXISTS _wd_match_wikidataid_diffs;
CREATE VIEW           _wd_match_wikidataid_diffs AS
    SELECT 
    --instr(_top3_wikidataid,'#'||ne_wikidataid||'#') as _instr
    --,
    *
    FROM wd_match
    WHERE (_mstatus!="F9_BAD" ) and _wikidata_status='NE'
    and instr('#'||_top3_wikidataid||'#','#'||ne_wikidataid||'#')=0
    ;

DROP   VIEW IF EXISTS _wd_match_wikidataid_strange_diffs;
CREATE VIEW           _wd_match_wikidataid_strange_diffs AS
    SELECT *
    FROM wd_match
    WHERE (_mstatus!="F9_BAD" ) and _wikidata_status='NE' and instr('#'||_top3_wikidataid||'#','#'||ne_wikidataid||'#')!=0;



DROP   VIEW IF EXISTS _wd_match_wikidataid_new;
CREATE VIEW           _wd_match_wikidataid_new AS
    SELECT *
    FROM wd_match
    WHERE  (_mstatus!="F9_BAD" ) and _wikidata_status='Na'  AND _geonames_wd_id IS NULL AND _top20percent_n=1 ;

DROP   VIEW IF EXISTS _wd_match_wikidataid_validated;
CREATE VIEW           _wd_match_wikidataid_validated AS
    SELECT *
    FROM wd_match
    WHERE  (_mstatus!="F9_BAD" ) and _wikidata_status='EQ';

DROP   VIEW IF EXISTS _wd_match_wikidataid_not_validated;
CREATE VIEW           _wd_match_wikidataid_not_validated AS
    SELECT *
    FROM wd_match
    WHERE  NOT (  (_mstatus!="F9_BAD" ) and _wikidata_status='EQ');


DROP   VIEW IF EXISTS _wd_match_validated_unicodename_diff;
CREATE VIEW           _wd_match_validated_unicodename_diff AS
    SELECT ne_fid,ne_adm0name, ne_name, wd_label , wd_id_url, wd_sitelink_en
    FROM _wd_match_wikidataid_validated
	WHERE _name_status="R13-Unidecode_equal";



DROP   VIEW IF EXISTS _wd_match_geodataname_diffs;
CREATE VIEW           _wd_match_geodataname_diffs AS
    SELECT *
    FROM wd_match
    WHERE ( _mstatus!="F9_BAD" ) and _geonames_status='NE';


DROP   TABLE IF EXISTS _1342_diffs ;
CREATE TABLE          _1342_diffs AS
SELECT candidates.fid, candidates.name, candidates.wd_wiki_id,
       wd_match.wd_id, wd_match.wd_label, wd_match.wd_distance,  wd_match._score,   wd_match.wd_sitelink_en, wd_match.wd_description,wd_match.wd_type
FROM candidates
LEFT JOIN wd_match  ON wd_match.ne_fid||wd_match.ne_name = candidates.fid||candidates.name
WHERE wd_match._score > 60  and  wd_wiki_id != wd_id and wd_match.wd_distance < 50
		and fid not in ( 1775 )
;


--  wiki reports ...
DROP   VIEW IF EXISTS _wiki_warn_only_cebuano_no_english_wiki;
CREATE VIEW           _wiki_warn_only_cebuano_no_english_wiki AS
    SELECT *
    FROM wiki
    WHERE wd_sitelink_en='' AND wd_sitelink_es='' AND wd_sitelink_ru='' AND  wd_sitelink_ceb!='' ;


DROP   VIEW IF EXISTS _wiki_err_disambiguation;
CREATE VIEW           _wiki_err_disambiguation AS
    SELECT *
    FROM wiki
    WHERE wd_disambiguation!='' ;


DROP   VIEW IF EXISTS _wiki_err_disambiguation_suggestions;
CREATE VIEW           _wiki_err_disambiguation_suggestions AS
    SELECT * from wd_match
    WHERE ne_xid IN ( SELECT ne_xid FROM wiki  WHERE wd_disambiguation!='' )
    ;



DROP VIEW IF EXISTS wiki_extended;
CREATE VIEW wiki_extended AS
SELECT
	    CASE
			WHEN wd_disambiguation!=''                                   THEN  "DEL-Disambiguation"
			WHEN     wd_sitelink_en='' 
                 and wd_sitelink_de=''
                 and wd_sitelink_es=''
                 and wd_sitelink_fr=''
                 and wd_sitelink_pt=''
                 and wd_sitelink_ru=''
                 and wd_sitelink_zh=''                 
                 and ne_wikidataid!=''                                   THEN  "DEL-No en/de/es/fr/pt/ru/zh wiki page"
			WHEN wd_location='' and ne_wikidataid!=''                    THEN  "DEL-No location (lat,lon) on wikidata"
			WHEN    wd_distance >   50
                AND ne_adm0name != wd_countrylabel
                AND wd_countrylabel not in ("Republic of the Congo","People's Republic of China")
                                                                         THEN  "DEL/WARN Distance>50km and country diff"
			WHEN    wd_distance >   500                                  THEN  "DEL/WARN Extreme distance    >500km "
			WHEN    wd_distance >   100                                  THEN  "DEL/WARN Extreme distance 100-499km "
			WHEN    wd_distance >    50                                  THEN  "WARN Extreme distance      50- 99km "

	--		WHEN    ne_adm0name != wd_countrylabel 
    --               and ne_wikidataid   !=''
    --               and wd_id !=''  
    --               and  ( NOT (ne_adm0name="China"	and wd_countrylabel="People's Republic of China"))
    --               and  ( NOT (ne_adm0name="Antarctica"	and wd_countrylabel=""))
    --               and  ( NOT (ne_adm0name="Congo (Kinshasa)"	    and wd_countrylabel="Democratic Republic of the Congo"))
    --               and  ( NOT (ne_adm0name="Congo (Brazzaville)"	and wd_countrylabel="Republic of the Congo"))        
    --                              THEN  "WARN: ne_adm0name != wd_countrylabel "           


            ELSE                                                         ""
		END AS _quick_status

	    ,CASE
			WHEN ne_wikidataid !=''
              THEN  "https://www.wikidata.org/wiki/"||ne_wikidataid
            ELSE  ""
		END AS ne_wikidataid_url        
        ,*
FROM wiki
ORDER BY CAST( ne_fid as INTEGER )
;



DROP   VIEW IF EXISTS _wd_match_wikidataid_update_01;
CREATE VIEW           _wd_match_wikidataid_update_01 AS
SELECT wd_match.*
FROM wd_match
WHERE  (_mstatus!="F9_BAD" ) 
AND ne_xid in (  SELECT ne_xid FROM wiki_extended WHERE substr(_quick_status,1,4)='DEL-'  ) 
;


DROP VIEW IF EXISTS wiki_extended_countryname_diffs;
CREATE VIEW wiki_extended_countryname_diffs AS
SELECT ne_adm0name , wd_countrylabel , count(*) as n
FROM wiki_extended
WHERE ne_adm0name != wd_countrylabel 
                   and ne_wikidataid   !=''
                   and wd_id !=''  
				   and _quick_status=''	
GROUP BY 1,2
ORDER BY 1,2	
;


.print '  _mstatus FREQ '
.print ' --------------------------------------'
select _quick_status, count(*) as n from wiki_extended  group by 1  order by 1 ;
.print


.print '  Problematic records by ADM0NAME'
.print ' --------------------------------------'
select ne_adm0name, count(*) as n from wd_match where _score < 30 group by 1 having n > 2 order by n desc;
.print


.print '  _mstatus FREQ '
.print ' --------------------------------------'
select _mstatus, count(*) as n from wd_match  group by 1  order by 1 ;
.print

.print ' -- End of postprocessing '