#!/usr/bin/env python

from SPARQLWrapper import SPARQLWrapper, JSON
from sys import argv

import datetime
import editdistance
import fiona
import jellyfish
import Levenshtein
import os
import sqlite3
import sys
import time
import unidecode

script, param_adm0name = argv

os.system("rm -f wikidata_naturalearth_qa.db")

conn = sqlite3.connect('wikidata_naturalearth_qa.db')
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS wd ;")
c.execute('''CREATE TABLE wd
            (
                ne_xid text,
                ne_fid  text,
                ne_name text,
                ne_adm0name text ,
                ne_adm1name text ,
                ne_ls_name text,
                ne_geonameid text,
                ne_wikidataid text,
                wd_id text,
                wd_label text,
                wd_description text,
                wd_countrylabel text,
                wd_location text,
                wd_geonames_id_grp text,
                wd_placetype_grp text,
                wd_has_sistercity text,
                wd_distance real,
                _score real,
                _name_status text,
                _geonames_status text,
                _wikidata_status text,
                _lev_ratio real,
                _lev_distance real,
                _lev_jaro real,
                _lev_jaro_winkler real,
                ts timestamp
            )
          ''')

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

def getwikidatacity(ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid, ne_name ,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid):

    query_template="""
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>

        SELECT 
            ?place
            ?placeLabel
            ?placeDescription
            ?countryLabel
            (SAMPLE(?sistercity) as ?sistercity_sample)
            (MIN(?distance)      as ?distance   )
            (MAX(?population)    as ?population )
            # (group_concat(distinct ?placeAlternative ; separator = "#") as ?placeAlternative_group)
            (group_concat(distinct ?GeoNames_ID        ; separator = "#") as ?GeoNames_ID_grp)
            (group_concat(distinct SUBSTR(STR(?placetype),STRLEN("http://www.wikidata.org/entity/")+1); separator = "#")
                    as ?placetype_grp)

        WHERE {
            VALUES ?placetype  {
                                wd:Q515
                                wd:Q3957
                                wd:Q486972
                                }
            ?place (wdt:P31/wdt:P279*) ?placetype.

            SERVICE wikibase:around {
                ?place wdt:P625 ?location.
                bd:serviceParam wikibase:center "Point(16.373064 48.20833)"^^geo:wktLiteral.
                bd:serviceParam wikibase:radius "50".
                bd:serviceParam wikibase:distance ?distance.
            }
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            OPTIONAL { ?place wdt:P17   ?country.     }
            OPTIONAL { ?place wdt:P1566 ?GeoNames_ID. }
            OPTIONAL { ?place wdt:P190  ?sistercity.  }
            OPTIONAL { ?place wdt:P1082 ?population . }
            # OPTIONAL { ?place skos:altLabel ?placeAlternative . }
        }
        GROUP BY ?place ?placeLabel   ?placeDescription ?countryLabel
        ORDER BY ?placeLabel
    """

    q=query_template.replace('16.373064',ne_lon).replace('48.20833',ne_lat)

    print(q)

    ts = datetime.datetime.now()

    sparql.setQuery(q)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    for result in results['results']['bindings']:

        _score=0;

        wd_id = result['place']['value'].split('/')[4]
        wd_distance = float( result['distance']['value'] )

        if 'placeLabel' in result:
            wd_label = result['placeLabel']['value']
        else:
            wd_label = ''

        if 'placeDescription' in result:
            wd_description = result['placeDescription']['value']
        else:
            wd_description = ''

        if 'countryLabel' in result:
            wd_countrylabel = result['countryLabel']['value']
        else:
            wd_countrylabel =''

        if 'location' in result:
            wd_location = result['location']['value']
        else:
            wd_location=''

        if 'GeoNames_ID_grp' in result:
            wd_geonames_id_grp="#"+result['GeoNames_ID_grp']['value']+"#"
        else:
            wd_geonames_id_grp=''


        if 'placetype_grp' in result:
            wd_placetype_grp="#"+result['placetype_grp']['value']+"#"
        else:
            wd_placetype_grp=''


        wd_has_sistercity=""
        if ('sistercity_sample' in result):
            if result['sistercity_sample']['value'] !=  '':
                wd_has_sistercity="Y"
                _score+=20

        uni_ne_name=unidecode.unidecode(ne_name)
        uni_wd_name=unidecode.unidecode(wd_label)

        _lev_ratio        = Levenshtein.ratio(uni_ne_name, uni_wd_name)
        _lev_distance     = Levenshtein.distance(uni_ne_name, uni_wd_name)
        _lev_jaro         = Levenshtein.jaro(uni_ne_name, uni_wd_name)
        _lev_jaro_winkler = Levenshtein.jaro_winkler(uni_ne_name, uni_wd_name)
        _match_rating_comparison     = jellyfish.match_rating_comparison(uni_ne_name, uni_wd_name)
        _damerau_levenshtein_distance= jellyfish.damerau_levenshtein_distance(uni_ne_name, uni_wd_name)
        _hamming_distance            = jellyfish.hamming_distance(uni_ne_name, uni_wd_name)

        _score+= _lev_jaro_winkler*10;

        if ne_name == wd_label:
            _name_status='R01-Equal'
            _score+=100
        elif ne_name.lower()==wd_label.lower():
            _name_status='R12-Lowcase_equal'
            _score+=99
        elif uni_ne_name==uni_wd_name:
            _name_status='R13-Unidecode_equal'
            _score+=90
        else:
            _name_status=''
            if _lev_jaro_winkler> 0.9 :
                _score+=50
            elif _lev_jaro_winkler> 0.8 :
                _score+=20


        if ne_geonameid != '' and ('#'+ne_geonameid+'#' in wd_geonames_id_grp)  :
            _geonames_status='OK'
            _score+=40
        else:
            _geonames_status=''


        if (ne_wikidataid != '' ) and (wd_id !='' ) and (ne_wikidataid==wd_id):
            _wikidata_status='EQ'
            _score+=20
        elif (ne_wikidataid != '' ) and (wd_id !='' ):
            _wikidata_status='DIFF'

            # smaller wikidataid is sometimes better
            if float(  ne_wikidataid[1:]) > float(wd_id[1:]):
                _score+=3
            else:
                _score-=3

        else:
            _wikidata_status=''


        c.execute("INSERT INTO wd VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                        ne_xid,
                        ne_fid,
                        ne_name,
                        ne_adm0name,
                        ne_adm1name,
                        ne_ls_name,
                        ne_geonameid,
                        ne_wikidataid,
                        wd_id,
                        wd_label,
                        wd_description,
                        wd_countrylabel,
                        wd_location,
                        wd_geonames_id_grp,
                        wd_placetype_grp,
                        wd_has_sistercity,
                        wd_distance,
                        _score,
                        _name_status,
                        _geonames_status,
                        _wikidata_status,
                        _lev_ratio,
                        _lev_distance,
                        _lev_jaro,
                        _lev_jaro_winkler,
                        ts
            ))

    conn.commit()

    return


print('- Start Natural-Earth wikidata check - ')

with fiona.open('./natural-earth-vector/10m_cultural/ne_10m_populated_places.shp', 'r') as input:
        i=0
        for pt in input:
            i=i+1

            ne_fid= pt['id']
            ne_lat= str( pt['properties']['LATITUDE']  )
            ne_lon= str( pt['properties']['LONGITUDE'] )
            ne_name= pt['properties']['NAME']
            ne_wikidataid=pt['properties']['wikidataid']
            ne_adm0name=pt['properties']['ADM0NAME']
            ne_adm1name=pt['properties']['ADM1NAME']
            ne_ls_name=pt['properties']['LS_NAME']
            ne_geonameid=str(pt['properties']['GEONAMEID']).split('.')[0]

            ne_xid=ne_fid + '|' + ne_name + '|' + ne_adm0name + '|' + ne_adm1name + '|' + ne_ls_name

            if param_adm0name!='' and param_adm0name!=ne_adm0name:
                continue

            print(i, ne_xid  )
            w=getwikidatacity(ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid)

print (' - End -')

conn.close()

os.system("chmod 666 wikidata_naturalearth_qa.db")

print (' - Example start -')
os.system(""" sqlite3 wikidata_naturalearth_qa.db  "select _score, * from wd  order by _score desc limit 3   "    """ )
print (' - Example end -')

