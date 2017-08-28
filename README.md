# natural-earth-vector-qa
Compare/Merge  [Natural Earth Vector](https://github.com/nvkelso/natural-earth-vector)  and  Wikidata databases  ( work in progress )


### Init

```bash
make build
make init
```


### Genereate for all 

It is about 4-5 hour run time ..

```bash
make run
```

or  

```
docker-compose run --rm nearth_qa python3 /osm/01_query_wikidata.py
docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 02_postprocessing.sql 
```


### Testing a country ( example: Tunisia )


Add an extra parameter:
*  `Tunisia` is a valid "ADM0NAME" value of `ne_10m_populated_places.dbf`

```
docker-compose run --rm nearth_qa python3 /osm/01_query_wikidata.py Tunisia
docker-compose run --rm nearth_qa sqlite3 wikidata_naturalearth_qa.db < 02_postprocessing.sql 
```

### Outputs 

....

