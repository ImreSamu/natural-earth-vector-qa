#!/usr/bin/env python

#  Created by Imre Samu
#  https://github.com/ImreSamu/natural-earth-vector-qa
#


from SPARQLWrapper import SPARQLWrapper,  SPARQLExceptions, JSON
from urllib.error import URLError, HTTPError
from sys import argv


import argparse
import datetime
import editdistance
import fiona
import jellyfish
import Levenshtein
import os
import re
import sqlite3
import sys
import time
import unidecode

param_adm0name=''

print('parameter:', param_adm0name )

parser = argparse.ArgumentParser(description='Search Natural-Earth places in Wikidata.')

parser.add_argument('-filter_parallel_id',   default='',  help='filter by parallelisation ID (0-4) ')

parser.add_argument('-filter_fid',   default='',  help='filter by internal FID value')
parser.add_argument('-filter_name',   default='',  help='filter by NAME value')
parser.add_argument('-filter_nameascii',   default='',  help='filter by NAMEASCII value')
parser.add_argument('-filter_adm0name',   default='',  help='filter by ADM0NAME value')
parser.add_argument('-filter_wikidataid', default='',  help='filter by wikidataid value')
parser.add_argument('-filter_adm0_a3', default='',  help='filter by ADM0_A3 value')
parser.add_argument('-filter_wof_id', default='',  help='filter by wof_id value')
parser.add_argument('-filter_iso_a2', default='',  help='filter by ISO_A2 value')

parser.add_argument('--wikidataid_empty',    action="store_true",  help='processing empty wikidataid records ')
parser.add_argument('--wikidataid_nonempty', action="store_true",  help='processing non-empty wikidataid records ')

parser.add_argument('-database_name', default='wikidata_naturalearth_wiki.db',  help='output sqlite3 database name')

args = parser.parse_args()


print( 'Start wikidata processing ... ')
print( 'parmeter: ', args )



max_parallel_queries=10
if args.filter_parallel_id!='' and  int(args.filter_parallel_id) in (0,1,2,3,4,5,6,7,8,9) :
    args.database_name = "_p"+args.filter_parallel_id + '_' + "fetch_wiki.db"
    # automatically create log file .... 
    sys.stdout = open(args.database_name+'.log', 'w')
#else:
    #print("Error:   Parallelisaton parameter should be 0,1,2,3,4")
    #sys.exit(1) 


os.system("rm -f "+args.database_name)

conn = sqlite3.connect(args.database_name)
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS wd ;")
c.execute('''CREATE TABLE wiki
            (

                ne_fid  text,
                ne_wikidataid text,
                wd_id text,                
                ne_name text,
                wd_label text,       
                ne_adm0name text ,
                wd_countrylabel text,


                ne_namealt text,
                ne_nameascii text,
                
                ne_adm1name text ,
                ne_ls_name text,

      


                wd_description text,
                wd_type text,

                ne_geonameid text,
                wd_geonames_id_grp text,


                wd_place_alternative_grp text,
                wd_place_name_en text,
                wd_place_name_de text,
                wd_place_name_es text,
                wd_place_name_fr text,
                wd_place_name_pt text,  
                wd_place_name_ru text,  
                wd_place_name_zh text,                                  
                wd_sitelink_en text,
                wd_sitelink_de text,
                wd_sitelink_es text,
                wd_sitelink_fr text,
                wd_sitelink_pt text,            
                wd_sitelink_ru text,  
                wd_sitelink_zh text,                                                                        
                wd_sitelink_ceb text,
                wd_disambiguation text,
                wd_distance real,
                ne_latitude text,
                ne_longitude text,
                wd_location text, 
                wd_max_population text,
                ne_xid text,
                ne_scalerank text,
                ne_labelrank text,
                ne_natscale text,                
                ts timestamp,
                _runtime float
            )
          ''')

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")



def get_sparql_value(result,id):
    val=''
    if id in result:
        val = result[id]['value']
    return val

def get_sparql_numvalue(result,id):
    val=-1
    if id in result:
        val = float(result[id]['value'])
    return val


def fetchwikidata( _step, list_wikidataid, ne_fid, ne_xid, ne_longitude, ne_latitude, ne_wikidataid, ne_name ,ne_namealt,
                  ne_nameascii, ne_adm0name,ne_adm1name,ne_ls_name,
                  ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale):

    query_template="""
    
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        SELECT
            ?place
            ?placeLabel
            ?placeDescription

            
            (AVG(?distance)                                     as ?distance )
            (group_concat(distinct ?location ; separator = "#") as ?location )

    
            (group_concat(distinct  ?pLabel       ; separator = "#")        as ?type_grp)

            (group_concat(distinct  ?place_name_en ; separator = "#")       as ?place_name_en)
            (group_concat(distinct  ?place_name_de ; separator = "#")       as ?place_name_de)
            (group_concat(distinct  ?place_name_es ; separator = "#")       as ?place_name_es)
            (group_concat(distinct  ?place_name_fr ; separator = "#")       as ?place_name_fr)
            (group_concat(distinct  ?place_name_pt ; separator = "#")       as ?place_name_pt)
            (group_concat(distinct  ?place_name_ru ; separator = "#")       as ?place_name_ru)
            (group_concat(distinct  ?place_name_zh ; separator = "#")       as ?place_name_zh)

            (group_concat(distinct  ?sitelink_en  ; separator = "#")        as ?sitelink_en)
            (group_concat(distinct  ?sitelink_de  ; separator = "#")        as ?sitelink_de)
            (group_concat(distinct  ?sitelink_es  ; separator = "#")        as ?sitelink_es)
            (group_concat(distinct  ?sitelink_fr  ; separator = "#")        as ?sitelink_fr)
            (group_concat(distinct  ?sitelink_pt  ; separator = "#")        as ?sitelink_pt)
            (group_concat(distinct  ?sitelink_ru  ; separator = "#")        as ?sitelink_ru)
            (group_concat(distinct  ?sitelink_zh  ; separator = "#")        as ?sitelink_zh)

            (group_concat(distinct  ?sitelink_ceb ; separator = "#")        as ?sitelink_ceb)

            (group_concat(distinct  ?countryLabelx; separator = "#")        as ?countryLabel)
            (group_concat(distinct  ?disambiguation; separator = "#")       as ?disambiguation)
            
            (MAX(?population)                                               as ?max_population )
            (group_concat(distinct ?place_alternative_en ; separator = "#") as ?place_alternative_en)
            (group_concat(distinct ?GeoNames_ID          ; separator = "#") as ?GeoNames_ID_grp)
        WHERE {
            VALUES ?place { wd:Q1741 }
          
            # "#ne_name#" , "#ne_adm0name#"
          
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            OPTIONAL { ?place wdt:P31 ?property.  ?property rdfs:label ?pLabel         FILTER (lang(?pLabel) = "en").         }
            OPTIONAL { ?place wdt:P17 ?country.   ?country  rdfs:label ?countryLabelx  FILTER (lang(?countryLabelx) = "en").  }
          
            OPTIONAL { ?place wdt:P17       ?country.     }
            OPTIONAL { ?place wdt:P1566     ?GeoNames_ID. }
            OPTIONAL { ?place wdt:P1082     ?population . }
   
            OPTIONAL { ?place wdt:P625 ?location.   BIND(geof:distance(?location,  "Point(16.373064 48.20833)") as ?distance  )  }

            OPTIONAL { ?place  wdt:P31/wdt:P279*  wd:Q4167410 . BIND( "disambiguation" AS ?disambiguation). }

            OPTIONAL { ?place rdfs:label ?place_name_en   FILTER((LANG(?place_name_en)) = "en") . }
            OPTIONAL { ?place rdfs:label ?place_name_de   FILTER((LANG(?place_name_de)) = "de") . }
            OPTIONAL { ?place rdfs:label ?place_name_es   FILTER((LANG(?place_name_es)) = "es") . }
            OPTIONAL { ?place rdfs:label ?place_name_fr   FILTER((LANG(?place_name_fr)) = "fr") . }
            OPTIONAL { ?place rdfs:label ?place_name_pt   FILTER((LANG(?place_name_pt)) = "pt") . }
            OPTIONAL { ?place rdfs:label ?place_name_ru   FILTER((LANG(?place_name_ru)) = "ru") . }
            OPTIONAL { ?place rdfs:label ?place_name_zh   FILTER((LANG(?place_name_zh)) = "zh") . }

            OPTIONAL { ?sitelink_en  schema:about ?place . ?sitelink_en schema:isPartOf  <https://en.wikipedia.org/>. }
            OPTIONAL { ?sitelink_de  schema:about ?place . ?sitelink_de schema:isPartOf  <https://de.wikipedia.org/>. }
            OPTIONAL { ?sitelink_es  schema:about ?place . ?sitelink_es schema:isPartOf  <https://es.wikipedia.org/>. }
            OPTIONAL { ?sitelink_fr  schema:about ?place . ?sitelink_fr schema:isPartOf  <https://fr.wikipedia.org/>. }
            OPTIONAL { ?sitelink_pt  schema:about ?place . ?sitelink_pt schema:isPartOf  <https://pt.wikipedia.org/>. }
            OPTIONAL { ?sitelink_ru  schema:about ?place . ?sitelink_ru schema:isPartOf  <https://ru.wikipedia.org/>. } 
            OPTIONAL { ?sitelink_zh  schema:about ?place . ?sitelink_zh schema:isPartOf  <https://zh.wikipedia.org/>. }

            OPTIONAL { ?sitelink_ceb schema:about ?place . ?sitelink_ceb schema:isPartOf <https://ceb.wikipedia.org/>.}
            OPTIONAL { ?place skos:altLabel ?place_alternative_en   FILTER((LANG(?place_alternative_en)) = "en") . }
        }
        GROUP BY ?place ?placeLabel   ?placeDescription

    """
   
    q=query_template.replace('16.373064',ne_longitude).replace('48.20833',ne_latitude)
    q=q.replace('#ne_name#',ne_name).replace('#ne_adm0name#',ne_adm0name).replace('Q1741',ne_wikidataid)

    
    print("_step:",_step)

    while '  ' in q:
        q = q.replace('  ', ' ')
    
    if args.filter_name!='':
        print(q)


    ts = datetime.datetime.now()



    results = None
    retries = 0
    while results == None and retries < 8:
        try:
            results = None
            sparql.setQuery(q)
            sparql.setTimeout(1000)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

        except SPARQLExceptions.EndPointNotFound as e:
            print("ERRwikidata-SPARQLExceptions-EndPointNotFound:  Retrying in 30 seconds.", flush=True )
            time.sleep(30)
            retries += 1
            continue

        except SPARQLExceptions.EndPointInternalError as e:
            print("ERRwikidata-SPARQLExceptions-EndPointInternalError: Retrying in 30 seconds.", flush=True )
            time.sleep(30)
            retries += 1
            continue

        except TimeoutError:
            print("ERRwikidata-SPARQLExceptions  TimeOut : Retrying in 1 seconds.", flush=True )
            time.sleep(1)
            retries += 1            
            continue

        except SPARQLExceptions.QueryBadFormed as e:
            print("ERRwikidata-SPARQLExceptions-QueryBadFormed : Check!  "  ,  flush=True )
            return "error"

        except HTTPError as e:
            print("ERRwikidata: Got an HTTPError while querying. Retrying in 12 seconds.", flush=True )
            time.sleep(12)
            retries += 1
            continue

        except:
            print("ERRwikidata: other error. Retrying in 3 seconds.", flush=True )
            time.sleep(3)
            retries += 1
            continue




    if results == None and retries >= 8:
        print("Wikidata request failed ; system stopped! ")
        sys.exit(1)

    _runtime=   (datetime.datetime.now() - ts).total_seconds()



    rc_list_wikidataid=[]
#TODO empty answer ..

    for result in results['results']['bindings']:


        wd_id=get_sparql_value(result,'place').split('/')[4]
        wd_label=get_sparql_value(result,'placeLabel')
        wd_description = get_sparql_value(result,'placeDescription')
        wd_type = "#"+get_sparql_value(result,'type_grp')+"#"
        wd_countrylabel=get_sparql_value(result,'countryLabel')


        wd_place_name_en=get_sparql_value(result,'place_name_en')
        wd_place_name_de=get_sparql_value(result,'place_name_de')
        wd_place_name_es=get_sparql_value(result,'place_name_es')
        wd_place_name_fr=get_sparql_value(result,'place_name_fr')            
        wd_place_name_pt=get_sparql_value(result,'place_name_pt')
        wd_place_name_ru=get_sparql_value(result,'place_name_ru')
        wd_place_name_zh=get_sparql_value(result,'place_name_zh')

        wd_sitelink_en=get_sparql_value(result,'sitelink_en')
        wd_sitelink_de=get_sparql_value(result,'sitelink_de')
        wd_sitelink_es=get_sparql_value(result,'sitelink_es')
        wd_sitelink_fr=get_sparql_value(result,'sitelink_fr')              
        wd_sitelink_pt=get_sparql_value(result,'sitelink_pt')
        wd_sitelink_ru=get_sparql_value(result,'sitelink_ru')
        wd_sitelink_zh=get_sparql_value(result,'sitelink_zh')
        wd_sitelink_ceb=get_sparql_value(result,'sitelink_ceb')    

        wd_disambiguation=get_sparql_value(result,'disambiguation')
        wd_geonames_id_grp="#"+get_sparql_value(result,'GeoNames_ID_grp')+"#"

        wd_max_population = get_sparql_value(result,'max_population')
        wd_place_alternative_grp="#"+get_sparql_value(result,'place_alternative_grp')+"#"

        wd_distance = float( get_sparql_numvalue(result,'distance') )      
        wd_location = get_sparql_value(result,'location')
  


        c.execute("INSERT INTO wiki VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (

                        ne_fid,
                        ne_wikidataid,
                        wd_id,                        
                        ne_name,
                        wd_label,  
                        ne_adm0name,
                        wd_countrylabel,

                        ne_namealt,
                        ne_nameascii,
                        ne_adm1name,
                        ne_ls_name,

                        wd_description,
                        wd_type,

                        ne_geonameid,
                        wd_geonames_id_grp,

                        wd_place_alternative_grp,
                        wd_place_name_en,
                        wd_place_name_de,
                        wd_place_name_es,
                        wd_place_name_fr,
                        wd_place_name_pt, 
                        wd_place_name_ru,  
                        wd_place_name_zh,                                                                                                                                                
                        wd_sitelink_en,
                        wd_sitelink_de,
                        wd_sitelink_es,
                        wd_sitelink_fr,
                        wd_sitelink_pt, 
                        wd_sitelink_ru,   
                        wd_sitelink_zh,                                                                                                                                           
                        wd_sitelink_ceb,
                        wd_disambiguation,
                        wd_distance,
                        ne_latitude,
                        ne_longitude,
                        wd_location,
                        wd_max_population,
                        ne_xid,
                        ne_scalerank,
                        ne_labelrank,
                        ne_natscale,
                        ts,
                        _runtime
            ))

    conn.commit()
    sys.stdout.flush()

    return 








print('- Start Natural-Earth wikidata check - ')

with fiona.open('./natural-earth-vector/10m_cultural/ne_10m_populated_places.shp', 'r') as input:
        i=0
        for pt in input:
            i=i+1

            ne_fid= pt['id']
            ne_latitude= str( pt['properties']['LATITUDE']  )
            ne_longitude= str( pt['properties']['LONGITUDE'] )
            ne_name= pt['properties']['NAME']
            ne_namealt= pt['properties']['NAMEALT']
            ne_nameascii= pt['properties']['NAMEASCII']
            ne_wikidataid=pt['properties']['wikidataid']
            ne_adm0name=pt['properties']['ADM0NAME']
            ne_adm1name=pt['properties']['ADM1NAME']
            ne_ls_name=pt['properties']['LS_NAME']
            ne_geonameid=str(pt['properties']['GEONAMEID']).split('.')[0]

            ne_adm0_a3=pt['properties']['ADM0_A3']
            ne_iso_a2=pt['properties']['ISO_A2']
            ne_wof_id=pt['properties']['wof_id']

            ne_scalerank=pt['properties']['SCALERANK']
            ne_labelrank=pt['properties']['LABELRANK']
            ne_natscale=pt['properties']['NATSCALE']

            ne_xid=ne_fid + '#' + ne_name + '#' + ne_adm0name + '#' + ne_adm1name + '#' + ne_ls_name

            if args.filter_fid!='' and args.filter_fid!=ne_fid:
                continue
            if args.filter_name!='' and args.filter_name!=ne_name:
                continue
            if args.filter_nameascii!='' and args.filter_nameascii!=ne_nameascii:
                continue
            if args.filter_adm0name!='' and args.filter_adm0name!=ne_adm0name:
                continue
            if args.filter_wikidataid!='' and args.filter_wikidataid!=ne_wikidataid:
                continue
            if args.filter_adm0_a3!='' and args.filter_adm0_a3!=ne_adm0_a3:
                continue
            if args.filter_wof_id!='' and args.filter_wof_id!=str(ne_wof_id):
                continue
            if args.filter_iso_a2!='' and args.filter_iso_a2!=ne_iso_a2:
                continue

            if args.wikidataid_empty and ne_wikidataid!='':
                continue

            if args.wikidataid_nonempty and ne_wikidataid=='':
                continue

            if args.filter_parallel_id!='' and    int(args.filter_parallel_id)  != ( int(ne_fid) % max_parallel_queries ) :
                continue


            list_wikidataid=[]
            print(i, ne_xid , ne_scalerank , ne_wikidataid )

            fetchwikidata(1, list_wikidataid, ne_fid, ne_xid, ne_longitude, ne_latitude, ne_wikidataid,ne_name,
                     ne_namealt,ne_nameascii,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)

print (' - End -')

conn.close()

os.system("chmod 666 "+args.database_name)


if args.filter_parallel_id=='':
    #print (' - Postprocessing -')
    #os.system(" sqlite3 "+args.database_name+" < 05_postprocessing.sql " )

    #if args.filter_name!=''  or args.filter_fid!='':
    #    os.system("""  sqlite3 -line """ +args.database_name+ """    " select * from wd_match;  "     """ )
    if args.filter_name!=''  or args.filter_fid!='':
        os.system("""  sqlite3 -line """ +args.database_name+ """    " select * from wiki;  "     """ )

    print (' - Status -')
    #os.system("""  ./proc_report_freq.sh     "_status "     """ )
else:
    os.system("""  sqlite3 -line """ +args.database_name+ """    " select count(*) as N from wiki ;  "     """ )    

print (' - JOB end -')

