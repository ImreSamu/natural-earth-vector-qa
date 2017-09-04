# natural-earth-vector-qa  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Compare/Merge  [Natural Earth Vector](https://github.com/nvkelso/natural-earth-vector)  and  Wikidata databases  ( work in progress )

### Status:
Work in progress ...


### Genereate ...

* It is about  > 2-3 hour run time ...
* sytem req:   linux,  docker,  docker-compose


```bash
git clone https://github.com/ImreSamu/natural-earth-vector-qa.git
cd natural-earth-vector-qa
make runparallel
make export
```

The [Wikidata SPARQL query service](https://www.mediawiki.org/wiki/Wikidata_query_service/User_Manual) is limited to 5 parallel queries per IP, and this program using all 5! 


### Outputs

filename | description
---- | -----
wikidata_naturalearth_qa.db |  matching file  in sqlite3 database format 
_wd_match.csv | all matching information in csv format  
_wd_match_f1_ok.csv | only the first class matches
_wd_match_f2_good.csv | only the second class matches
_wd_match_f3_maybe.csv | maybe
_wd_match_wikidataid_diffs.csv | data problems?  ne.wikidataid !=  best match 
_wd_match_wikidataid_new.csv   | new wikidataids  
_wd_match_wikidataid_validated.csv | validated wikidataids 

### License:
* Program license:  [License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
* Data license:
    * Input databases:
        * [Natural Earth Vector](https://github.com/nvkelso/natural-earth-vector)  : public domain
        * [Wikidata](https://www.wikidata.org/) 
            * "All structured data from the main and property namespace is available under the Creative Commons CC0 License"
    * Output reports/databases:
        * Creative Commons CC0 License;
