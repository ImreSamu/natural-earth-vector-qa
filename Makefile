.PHONY: all

all: build

build:
	cd docker && docker build -t nearth_qa . && cd .. 
	docker images | grep nearth_qa

init:
	rm -rf ./natural-earth-vector
	git clone  --depth 1 https://github.com/nvkelso/natural-earth-vector.git
	ls -la ./natural-earth-vector/10m_cultural/ne_10m_populated_places.dbf

run:
	docker-compose run --rm nearth_qa python3 /osm/01_query_wikidata.py 
	docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 02_postprocessing.sql  

test:
	docker-compose run --rm nearth_qa python3 /osm/01_query_wikidata.py China
	docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 02_postprocessing.sql  
	docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 501_report_matching_debug.sql

postprocessing:
	docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 02_postprocessing.sql
