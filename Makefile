.PHONY: all

all: build init runparallel export

build:
	docker pull python:3-stretch
	cd docker && docker build -t nearth_qa . && cd ..
	docker images | grep nearth_qa

init:
	rm -rf ./natural-earth-vector
	git clone  --depth 1 https://github.com/nvkelso/natural-earth-vector.git
	ls -la ./natural-earth-vector/10m_cultural/ne_10m_populated_places.dbf
	docker-compose run --rm nearth_qa /osm/candidates.sh

dev:
	docker-compose run --rm  nearth_qa /bin/bash

run:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py

test:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Algeria"

test-niagara-falls:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Niagara Falls"

test-moscow:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Moscow"

test-celeken:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Celeken"

test-chanaral:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Chanaral"


test-tierra-amarilla:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Tierra Amarilla"

test-cardenas:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Cardenas"


test-turgay:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Turgay"



test-qusmuryn:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Qusmuryn"


# no english
test-esperanza:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Esperanza"


# no english - espanole
test-villa-rumipal:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Villa Rumipal"

test-utrecht :
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Utrecht"

test-assen:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Assen"

test-weyburn:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Weyburn"

test-netherlands:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Netherlands"

test-chile:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Chile"

test-turkey:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Turkey"

test-romania:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Romania"

test-mexico:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Mexico"

test-argentina:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Argentina"


postprocessing:
	docker-compose run --rm  nearth_qa sqlite3  -batch wikidata_naturalearth_qa.db < 05_postprocessing.sql

hun-testparallel:
	docker-compose run --rm -T nearth_qa parallel -k -j5 python3 -u /osm/01_query_wikidata.py -filter_adm0name="Hungary"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm -T nearth_qa /osm/03_merge.sh
	docker-compose run --rm -T nearth_qa sqlite3 wikidata_naturalearth_qa.db < 05_postprocessing.sql

runparallel:
	docker-compose run --rm -T nearth_qa parallel -k -j4 python3 -u /osm/01_query_wikidata.py  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm -T nearth_qa /osm/03_merge.sh
	docker-compose run --rm -T nearth_qa sqlite3 wikidata_naturalearth_qa.db < 05_postprocessing.sql

export:
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM wiki_extended;"           		   > _wiki_extended.csv
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM wiki_extended_countryname_diffs;" > _wiki_extended_countryname_diffs.csv
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM wd_match;"           			   > _wd_match.csv
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_f1_ok;"    			   > _wd_match_f1_ok.csv
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_f2_good;"  			   > _wd_match_f2_good.csv
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_f3_maybe;" 			   > _wd_match_f3_maybe.csv
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_wikidataid_diffs;"      > _wd_match_wikidataid_diffs.csv
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_wikidataid_new;"        > _wd_match_wikidataid_new.csv
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_wikidataid_validated;"  > _wd_match_wikidataid_validated.csv
	docker-compose run --rm -T nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_validated_unicodename_diff;" > _wd_match_validated_unicodename_diff.csv

clean:
	rm -f _*.db
	rm -f _*.log
	rm -f _*.job
	rm -f _*.csv
	rm -f wikidata_naturalearth_qa.db

run_all:
	docker-compose run --rm -T nearth_qa parallel --res _log_query_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01_query_wikidata.py   -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm -T nearth_qa parallel --res _log_fetch_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01c_fetch_wikidata.py  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm  nearth_qa /osm/03_merge.sh
	docker-compose run --rm  nearth_qa sqlite3  -batch wikidata_naturalearth_qa.db < 05_postprocessing.sql

run_all_hun:
	docker-compose run --rm -T nearth_qa parallel --res _log_query_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01_query_wikidata.py  -filter_adm0name="Hungary"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm -T nearth_qa parallel --res _log_fetch_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01c_fetch_wikidata.py -filter_adm0name="Hungary"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm  nearth_qa /osm/03_merge.sh
	docker-compose run --rm  nearth_qa sqlite3  -batch wikidata_naturalearth_qa.db < 05_postprocessing.sql

run_all_monaco:
	docker-compose run --rm -T nearth_qa parallel --res _log_query_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01_query_wikidata.py  -filter_adm0name="Monaco"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm -T nearth_qa parallel --res _log_fetch_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01c_fetch_wikidata.py -filter_adm0name="Monaco"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm  nearth_qa /osm/03_merge.sh
	docker-compose run --rm  nearth_qa sqlite3  -batch wikidata_naturalearth_qa.db < 05_postprocessing.sql



test-morocco:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Morocco"

test-canada:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Canada"

test-myanmar:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Myanmar"

test-taiwan:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Taiwan"

test-venezuela:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Venezuela"

test-kazakhstan:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Kazakhstan"

test-congo_kinshasa:
	docker-compose run --rm -T nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Congo (Kinshasa)"




#Malaysia
#Mauritania
#Ivory Coast
#Indonesia
#Panama
#Niger

run_all_canada:
	docker-compose run --rm -T nearth_qa parallel --res _log_query_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01_query_wikidata.py  -filter_adm0name="Canada"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm -T nearth_qa parallel --res _log_fetch_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01c_fetch_wikidata.py -filter_adm0name="Canada"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm  nearth_qa /osm/03_merge.sh
	docker-compose run --rm  nearth_qa sqlite3  -batch wikidata_naturalearth_qa.db < 05_postprocessing.sql

run_all_russia:
	docker-compose run --rm -T nearth_qa parallel --res _log_query_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01_query_wikidata.py  -filter_adm0name="Russia"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm -T nearth_qa parallel --res _log_fetch_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01c_fetch_wikidata.py -filter_adm0name="Russia"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm  nearth_qa /osm/03_merge.sh
	docker-compose run --rm  nearth_qa sqlite3  -batch wikidata_naturalearth_qa.db < 05_postprocessing.sql

run_all_congo_kinshasa:
	docker-compose run --rm -T nearth_qa parallel --res _log_query_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01_query_wikidata.py  -filter_adm0name="Congo (Kinshasa)"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm -T nearth_qa parallel --res _log_fetch_wikidata  --halt now,fail=1 -k -j5 python3 -u /osm/01c_fetch_wikidata.py -filter_adm0name="Congo (Kinshasa)"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm  nearth_qa /osm/03_merge.sh
	docker-compose run --rm  nearth_qa sqlite3  -batch wikidata_naturalearth_qa.db < 05_postprocessing.sql



twikimonaco:
	docker-compose run --rm -T nearth_qa python3 -u /osm/01c_fetch_wikidata.py -filter_name="Monaco"

