.PHONY: all

all: build init run export

build:
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

postprocessing:
	docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 02_postprocessing.sql

export:	
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "select * from wd_match;"           			> _wd_match.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "select * from _wd_match_f1_ok;"    			> _wd_match_f1_ok.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "select * from _wd_match_f2_good;"  			> _wd_match_f2_good.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "select * from _wd_match_f3_maybe;" 			> _wd_match_f3_maybe.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_wikidataid_diffs;"  > _wd_match_wikidataid_diffs.csv
	docker-compose run --rm nearth_qa sqlite3 -header -csv wikidata_naturalearth_qa.db "SELECT * FROM _wd_match_wikidataid_new;"    > _wd_match_wikidataid_new.csv
	