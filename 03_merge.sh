#!/bin/bash -ex

rm -f wikidata_naturalearth_qa.db
sqlite3 -batch wikidata_naturalearth_qa.db <<EOF
PRAGMA cache_size = -1000000;
PRAGMA count_changes = OFF;
PRAGMA journal_mode = OFF;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA synchronous = OFF;

attach '_p0_wiki.db' as p0;
attach '_p1_wiki.db' as p1;
attach '_p2_wiki.db' as p2;
attach '_p3_wiki.db' as p3;
attach '_p4_wiki.db' as p4;
attach '_p5_wiki.db' as p5;
attach '_p6_wiki.db' as p6;
attach '_p7_wiki.db' as p7;
attach '_p8_wiki.db' as p8;
attach '_p9_wiki.db' as p9;

BEGIN;

CREATE TABLE wd AS SELECT * FROM p0.wd LIMIT 0;

INSERT INTO  wd    SELECT * FROM p0.wd ORDER BY ne_xid, wd_id;
INSERT INTO  wd    SELECT * FROM p1.wd ORDER BY ne_xid, wd_id;
INSERT INTO  wd    SELECT * FROM p2.wd ORDER BY ne_xid, wd_id;
INSERT INTO  wd    SELECT * FROM p3.wd ORDER BY ne_xid, wd_id;
INSERT INTO  wd    SELECT * FROM p4.wd ORDER BY ne_xid, wd_id;
INSERT INTO  wd    SELECT * FROM p5.wd ORDER BY ne_xid, wd_id;
INSERT INTO  wd    SELECT * FROM p6.wd ORDER BY ne_xid, wd_id;
INSERT INTO  wd    SELECT * FROM p7.wd ORDER BY ne_xid, wd_id;
INSERT INTO  wd    SELECT * FROM p8.wd ORDER BY ne_xid, wd_id;
INSERT INTO  wd    SELECT * FROM p9.wd ORDER BY ne_xid, wd_id;

COMMIT;

detach p0;
detach p1;
detach p2;
detach p3;
detach p4;
detach p5;
detach p6;
detach p7;
detach p8;
detach p9;

VACUUM;

.print -- Parallel postprocessing OK --
;
EOF

#  -- 
chmod 666  wikidata_naturalearth_qa.db
