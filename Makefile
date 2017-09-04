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

run:
	docker-compose run --rm nearth_qa python3 /osm/01_query_wikidata.py

test:
	docker-compose run --rm nearth_qa python3 /osm/01_query_wikidata.py  -filter_adm0name="Algeria"

test-niagara-falls:
	docker-compose run --rm nearth_qa python3 /osm/01_query_wikidata.py  -filter_name="Niagara Falls"

postprocessing:
	docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 05_postprocessing.sql

hun-testparallel:
	docker-compose run --rm nearth_qa parallel -k -j5 python3 -u /osm/01_query_wikidata.py -filter_adm0name="Hungary"  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm nearth_qa /osm/03_merge.sh
	docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 05_postprocessing.sql

runparallel:
	docker-compose run --rm nearth_qa parallel -k -j5 python3 -u /osm/01_query_wikidata.py  -filter_parallel_id={} ::: 0 1 2 3 4 5 6 7 8 9
	docker-compose run --rm nearth_qa /osm/03_merge.sh
	docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 05_postprocessing.sql

export:	
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM wd_match;"           			    > _wd_match.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_f1_ok;"    			    > _wd_match_f1_ok.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_f2_good;"  			    > _wd_match_f2_good.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_f3_maybe;" 			    > _wd_match_f3_maybe.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_wikidataid_diffs;"      > _wd_match_wikidataid_diffs.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_wikidataid_new;"        > _wd_match_wikidataid_new.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_wikidataid_validated;"  > _wd_match_wikidataid_validated.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_validated_unicodename_diff;" > _wd_match_validated_unicodename_diff.csv

clean:
	rm -f _*.db
	rm -f _*.log
	rm -f _*.job
	rm -f _*.csv
	rm -f wikidata_naturalearth_qa.db
