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

parser.add_argument('-database_name', default='wikidata_naturalearth_qa.db',  help='output sqlite3 database name')

args = parser.parse_args()


print( 'Start wikidata processing ... ')
print( 'parmeter: ', args )



max_parallel_queries=10
if args.filter_parallel_id!='' and  int(args.filter_parallel_id) in (0,1,2,3,4,5,6,7,8,9) :
    args.database_name = "_p"+args.filter_parallel_id + '_' + "wiki.db"
    # automatically create log file .... 
    sys.stdout = open(args.database_name+'.log', 'w')
#else:
    #print("Error:   Parallelisaton parameter should be 0,1,2,3,4")
    #sys.exit(1) 


os.system("rm -f "+args.database_name)

conn = sqlite3.connect(args.database_name)
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS wd ;")
c.execute('''CREATE TABLE wd
            (
                ne_xid text,
                ne_fid  text,
                ne_name text,
                ne_namealt text,
                ne_adm0name text ,
                ne_adm1name text ,
                ne_ls_name text,
                ne_scalerank text,
                ne_labelrank text,
                ne_natscale text,
                ne_geonameid text,
                ne_wikidataid text,
                wd_id text,
                wd_label text,
                wd_description text,
                wd_type text,
                wd_countrylabel text,
                wd_geonames_id_grp text,
                wd_place_alternative_grp text,
                wd_sitelink_en text,
                wd_sitelink_ceb text,
                wd_has_sistercity text,
                wd_max_population text,
                wd_distance real,
                _step text,
                _score real,
                _name_status text,
                _geonames_status text,
                _wikidata_status text,
                _in_altnames text,
                _lev_ratio real,
                _lev_distance real,
                _lev_jaro real,
                _lev_jaro_winkler real,
                ts timestamp,
                _runtime float
            )
          ''')

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")


def getwikidatacity(_step, list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid, ne_name ,ne_namealt ,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale):

    query_template="""
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        SELECT
            ?place
            ?placeLabel
            ?placeDescription
            (group_concat(distinct  ?pLabel       ; separator = "#")        as ?type_grp)
            (group_concat(distinct  ?sitelink_en  ; separator = "#")        as ?sitelink_en)
            (group_concat(distinct  ?sitelink_ceb ; separator = "#")        as ?sitelink_ceb)
            (group_concat(distinct  ?countryLabelx; separator = "#")        as ?countryLabel)
            (SAMPLE(?sistercity)                                            as ?sistercity_sample)
            (AVG(?distance)                                                 as ?distance   )
            (MAX(?population)                                               as ?max_population )
            (group_concat(distinct ?place_alternative ; separator = "#")    as ?place_alternative_grp)
            (group_concat(distinct ?GeoNames_ID       ; separator = "#")    as ?GeoNames_ID_grp)
        WHERE {

            #S1#     ?place p:P31/ps:P31  wd:Q515.

            #S2#     ?place p:P31/ps:P31  wd:Q3957.

            #S3#           {?place (p:P31/wdt:P31/wdt:P279*)  wd:Q532.     }
            #S3#     UNION {?place  p:P31/ps:P31              wd:Q532.     }
            #S3#     UNION {?place (p:P31/wdt:P31/wdt:P279*)  wd:Q15078955.}
            #S3#     UNION {?place  p:P31/ps:P31              wd:Q15078955.}
            #S3#     UNION {
            #S3#      ?place (p:P31/wdt:P31/wdt:P279*) wd:Q486972 .
            #S3#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q131596.    }.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q5084.      }.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q2514025    }.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P36 ?capitalplace  }.
            #S3#      ?place rdfs:label ?placeLabel_en FILTER (lang(?placeLabel_en) = "en").
            #S3#     }
            #S3#     UNION {
            #S3#      ?place p:P31/ps:P31  wd:Q486972.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q131596.    }.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q5084.      }.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q2514025    }.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P36 ?capitalplace  }.
            #S3#      ?place rdfs:label ?placeLabel_en FILTER (lang(?placeLabel_en) = "en").
            #S3#     }
            #S3#     UNION {
            #S3#      ?place p:P31/ps:P31/wdt:P279*  wd:Q486972.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q131596.    }.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q5084.      }.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q2514025    }.
            #S3#      FILTER NOT EXISTS  { ?place wdt:P36 ?capitalplace  }.
            #S3#      ?place rdfs:label ?placeLabel_en FILTER (lang(?placeLabel_en) = "en").
            #S3#     }

            #S4#            {?place (p:P31/wdt:P31/wdt:P279*)  wd:Q2039348. }
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q2039348. }
            #S4#     UNION  {?place (p:P31/wdt:P31/wdt:P279*)  wd:Q1867183. }
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q1867183. }
            #S4#     UNION  {?place wdt:P1376     ?admin_ara.               }
            #S4#     UNION  {?place (p:P31/wdt:P31/wdt:P279*)  wd:Q1637706. }
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q1637706. }
            #S4#     UNION  {?place (p:P31/wdt:P31/wdt:P279*)  wd:Q16861602.}
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q16861602.}
            #S4#     UNION  {?place p:P31/ps:P31  wd:Q188509.  ?place p:P17/ps:P17  wd:Q408. }
            #S4#     UNION  {?place (p:P31/wdt:P31/wdt:P279*)  wd:Q1070990. }
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q1070990. }
            #S4#     UNION  {?place p:P31/wdt:P31/wdt:P279*    wd:Q748149.  }
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q748149.  }
            #S4#     UNION  {?place p:P31/wdt:P31/wdt:P279*    wd:Q735428.  }
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q735428.  }
            #S4#     UNION  {?place p:P31/wdt:P31/wdt:P279*    wd:Q318727.  }
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q318727.  }
            #S4#     UNION  {?place p:P31/wdt:P31/wdt:P279*    wd:Q15284.   }
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q15284.   }
            #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q15284.   }
            #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q532.     }
            #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q15078955.}
            #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q498162.  }
            #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q3389680. }
            #S4#     UNION  {?place p:P31/ps:P31               wd:Q1639634. }
            #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q1639634. }
            #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q2112349. }
            #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q749622.  }

            #S5#      {?place wdt:P190      ?sistercity_x.}

            SERVICE wikibase:around {     # "#ne_name#" , "#ne_adm0name#"
                ?place wdt:P625 ?location.
                bd:serviceParam wikibase:center "Point(16.373064 48.20833)"^^geo:wktLiteral.
                bd:serviceParam wikibase:radius "#distance#".
                bd:serviceParam wikibase:distance ?distance.
            }
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            OPTIONAL { ?place wdt:P31 ?property.  ?property rdfs:label ?pLabel  FILTER (lang(?pLabel) = "en"). }
            OPTIONAL { ?place wdt:P17 ?country.  ?country rdfs:label ?countryLabelx  FILTER (lang(?countryLabelx) = "en").  }
            OPTIONAL { ?place wdt:P17       ?country.     }
            OPTIONAL { ?place wdt:P1566     ?GeoNames_ID. }
            OPTIONAL { ?place wdt:P190      ?sistercity.  }
            OPTIONAL { ?place wdt:P1082     ?population . }
            OPTIONAL { ?sitelink_en  schema:about ?place . ?sitelink_en schema:isPartOf  <https://en.wikipedia.org/>. }
            OPTIONAL { ?sitelink_ceb schema:about ?place . ?sitelink_ceb schema:isPartOf <https://ceb.wikipedia.org/>.}
            OPTIONAL { ?place skos:altLabel ?place_alternative   FILTER((LANG(?place_alternative)) = "en") . }
        }
        GROUP BY ?place ?placeLabel   ?placeDescription
        ORDER BY ?distance
    """

    q=query_template.replace('16.373064',ne_lon).replace('48.20833',ne_lat)
    q=q.replace('#ne_name#',ne_name).replace('#ne_adm0name#',ne_adm0name)

    if  ( -10 <=  float(ne_lon) <= 60)  and  (  float(ne_lat) >30  ):
        if   _step==1:
            q=q.replace('#S1#','').replace('#distance#','50')
        elif _step==2:
            q=q.replace('#S2#','').replace('#distance#','50')
        elif _step==3:
            q=q.replace('#S3#','').replace('#distance#','30')
        elif _step==4:
            q=q.replace('#S4#','').replace('#distance#','30')
        elif _step==5:
            q=q.replace('#S5#','').replace('#distance#','20')
        else:
            print("Internal error, _step: ", _step )
            sys.exit(1)

    else:
        if   _step==1:
            q=q.replace('#S1#','').replace('#distance#','150')
        elif _step==2:
            q=q.replace('#S2#','').replace('#distance#','150')
        elif _step==3:
            q=q.replace('#S3#','').replace('#distance#','50')
        elif _step==4:
            q=q.replace('#S4#','').replace('#distance#','50')
        elif _step==5:
            q=q.replace('#S5#','').replace('#distance#','20')
        else:
            print("Internal error, _step: ", _step )
            sys.exit(1)

    print("_step:",_step)

    while '  ' in q:
        q = q.replace('  ', ' ')
    
    if args.filter_name!='':
        print(q)


    ts = datetime.datetime.now()

    max_score=-1000


    results = None
    retries = 0
    while results == None and retries < 5:
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
            print("ERRwikidata: Got an HTTP Error while querying. Retrying in 30 seconds.", flush=True )
            time.sleep(30)
            retries += 1
            continue


    if results == None and retries >= 5:
        print("Dbpedia request failed ; system stopped! ")
        sys.exit(1)

    _runtime=   (datetime.datetime.now() - ts).total_seconds()



    rc_list_wikidataid=[]
#TODO empty answer ..

    for result in results['results']['bindings']:

        _score=0;

        wd_id = result['place']['value'].split('/')[4]

        # Check if already queryed?
        if wd_id in list_wikidataid:
            print("Already exist:", wd_id)
            continue
        else:
            rc_list_wikidataid.append(wd_id)

        wd_distance = float( result['distance']['value'] )

        if 'placeLabel' in result:
            wd_label = result['placeLabel']['value']
        else:
            wd_label = ''

        if 'placeDescription' in result:
            wd_description = result['placeDescription']['value']
        else:
            wd_description = ''

        if 'type_grp' in result:
            wd_type = "#"+result['type_grp']['value']+"#"
        else:
            wd_type = ''

        if 'countryLabel' in result:
            wd_countrylabel = result['countryLabel']['value']

            cldiff=  - ( 20 -  ( 20 * Levenshtein.jaro_winkler( unidecode.unidecode(ne_adm0name) ,  unidecode.unidecode(wd_countrylabel) )   ) )
            #print( cldiff, ne_adm0name, wd_countrylabel )
            _score+= cldiff  

        else:
            wd_countrylabel =''

        _score+= -10
        if 'sitelink_en' in result:
            wd_sitelink_en = result['sitelink_en']['value']
            if wd_sitelink_en != '':
                _score+=30
        else:
            wd_sitelink_en=''



        if 'sitelink_ceb' in result:
            wd_sitelink_ceb = result['sitelink_ceb']['value']

        else:
            wd_sitelink_ceb=''


        if (wd_sitelink_ceb != '') and  (wd_sitelink_en == ''):
                _score+=  -1000      # penalty for   only ceb import




        if 'GeoNames_ID_grp' in result:
            wd_geonames_id_grp="#"+result['GeoNames_ID_grp']['value']+"#"
        else:
            wd_geonames_id_grp=''

        if 'max_population' in result:
            wd_max_population = result['max_population']['value']
            if wd_max_population!='':
                _score+=8
        else:
            wd_max_population=''

        if 'place_alternative_grp' in result:
            wd_place_alternative_grp="#"+result['place_alternative_grp']['value']+"#"
        else:
            wd_place_alternative_grp=''


        if ('#'+ne_name+'#' in wd_place_alternative_grp)  :
            _in_altnames='Y'
            _score+=42
        else:
            _in_altnames='N'

        wd_has_sistercity=""
        if ('sistercity_sample' in result):
            if result['sistercity_sample']['value'] !=  '':
                wd_has_sistercity="Y"
                _score+=15

        uni_ne_name=unidecode.unidecode(ne_name)
        uni_ne_ls_name=unidecode.unidecode(ne_ls_name)
        uni_ne_namealt=unidecode.unidecode(ne_namealt)

        uni_wd_name=unidecode.unidecode(wd_label)

        _lev_ratio        = Levenshtein.ratio(uni_ne_name, uni_wd_name)
        _lev_distance     = Levenshtein.distance(uni_ne_name, uni_wd_name)
        _lev_jaro         = Levenshtein.jaro(uni_ne_name, uni_wd_name)

        _lev_jaro_winkler     = Levenshtein.jaro_winkler(uni_ne_name, uni_wd_name)
        _lev_jaro_winkler_ls  = Levenshtein.jaro_winkler(uni_ne_ls_name, uni_wd_name)
        _lev_jaro_winkler_alt = Levenshtein.jaro_winkler(uni_ne_namealt, uni_wd_name)

        _max_lev_jaro_winkler = max(_lev_jaro_winkler,_lev_jaro_winkler_ls,_lev_jaro_winkler_alt)

        _match_rating_comparison     = jellyfish.match_rating_comparison(uni_ne_name, uni_wd_name)
        _damerau_levenshtein_distance= jellyfish.damerau_levenshtein_distance(uni_ne_name, uni_wd_name)
        _hamming_distance            = jellyfish.hamming_distance(uni_ne_name, uni_wd_name)

        _score+= _max_lev_jaro_winkler*10;

        if ne_name == wd_label:
            _name_status='R01-Equal'
            _score+=100
        elif ne_name.lower()==wd_label.lower():
            _name_status='R12-Lowcase_equal'
            _score+=99
        elif uni_ne_name==uni_wd_name:
            _name_status='R13-Unidecode_equal'
            _score+=90
        elif uni_ne_ls_name==uni_wd_name:
            _name_status='R31-ls_name eq'
            _score+=60
        elif uni_ne_namealt==uni_wd_name:
            _name_status='R32-namealt eq'
            _score+=60
        elif uni_ne_namealt==uni_wd_name:
            _name_status='R33-namealt eq'
            _score+=60
        elif _max_lev_jaro_winkler == 1.0 :
            _name_status='R41- max(jaro_winkler)=1'
            _score+=50
        elif _max_lev_jaro_winkler >= 0.9 :
            _name_status='R42- max(jaro_winkler) 0.9-1.0'
            _score+=40
        elif _max_lev_jaro_winkler >= 0.8 :
            _name_status='R43- max(jaro_winkler) 0.8-0.9'
            _score+=30
        else:
            _name_status=''


        if wd_distance < 5:
            _score += 10
        elif wd_distance < 10:
            _score += 5
        elif wd_distance > 60:
            _score +=  -30
        elif wd_distance > 30:
            _score +=  -15
        elif wd_distance > 15:
            _score +=  -5

        if ne_geonameid != '' and ('#'+ne_geonameid+'#' in wd_geonames_id_grp)  :
            _geonames_status='EQ'
            _score+=40
        elif ne_geonameid != '' and ne_geonameid != '-1' and wd_geonames_id_grp!='##' and ('#'+ne_geonameid+'#' not in wd_geonames_id_grp)  :
            _geonames_status='NE'
            _score+=0
        else:
            _geonames_status='Na'


        if (ne_wikidataid != '' ) and (wd_id !='' ) and (ne_wikidataid==wd_id):
            _wikidata_status='EQ'
            _score+=15
        elif (ne_wikidataid != '' ) and (wd_id !='' ):
            _wikidata_status='NE'

            # smaller wikidataid is sometimes better
            if float(  ne_wikidataid[1:]) > float(wd_id[1:]):
                _score+=  3
            else:
                _score+= -3

        else:
            _wikidata_status='Na'

        if _score > max_score:
            max_score=_score

        if _score > 140:
            print("@@_score>120:" , ne_name , " :: ",  wd_id, wd_label, wd_description, wd_type )


        c.execute("INSERT INTO wd VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                        ne_xid,
                        ne_fid,
                        ne_name,
                        ne_namealt,
                        ne_adm0name,
                        ne_adm1name,
                        ne_ls_name,
                        ne_scalerank,
                        ne_labelrank,
                        ne_natscale,
                        ne_geonameid,
                        ne_wikidataid,
                        wd_id,
                        wd_label,
                        wd_description,
                        wd_type,
                        wd_countrylabel,
                        wd_geonames_id_grp,
                        wd_place_alternative_grp,
                        wd_sitelink_en,
                        wd_sitelink_ceb,
                        wd_has_sistercity,
                        wd_max_population,
                        wd_distance,
                        _step,
                        _score,
                        _name_status,
                        _geonames_status,
                        _wikidata_status,
                        _in_altnames,
                        _lev_ratio,
                        _lev_distance,
                        _lev_jaro,
                        _lev_jaro_winkler,
                        ts,
                        _runtime
            ))

    conn.commit()
    sys.stdout.flush()
    if max_score <= 30:
        print(" Low score .. stop ", max_score)
  


    return  list_wikidataid + rc_list_wikidataid , max_score








print('- Start Natural-Earth wikidata check - ')

with fiona.open('./natural-earth-vector/10m_cultural/ne_10m_populated_places.shp', 'r') as input:
        i=0
        for pt in input:
            i=i+1

            ne_fid= pt['id']
            ne_lat= str( pt['properties']['LATITUDE']  )
            ne_lon= str( pt['properties']['LONGITUDE'] )
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

            rc1_list_wikidataid, _maxscore1  =getwikidatacity(1, list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                     ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
            print('1:',len(rc1_list_wikidataid) , _maxscore1)


            rc2_list_wikidataid, _maxscore2 =getwikidatacity(2, rc1_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                     ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
            print('2:',len(rc2_list_wikidataid) , _maxscore2)


            if max(_maxscore1,_maxscore2)  < 140:
                rc3_list_wikidataid , _maxscore3 =getwikidatacity(3, rc2_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                     ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
                print('3:',len(rc3_list_wikidataid) , _maxscore3)

                if _maxscore3 < 140:
                    rc4_list_wikidataid , _maxscore4 =getwikidatacity(4, rc3_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                        ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
                    print('4:',len(rc4_list_wikidataid) , _maxscore4)


                    if _maxscore4 < 140:
                        rc5_list_wikidataid , _maxscore5 =getwikidatacity(5, rc4_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                            ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
                        print('5:',len(rc5_list_wikidataid) , _maxscore5)

print (' - End -')

conn.close()

os.system("chmod 666 "+args.database_name)


if args.filter_parallel_id=='':
    print (' - Postprocessing -')
    os.system(" sqlite3 "+args.database_name+" < 05_postprocessing.sql " )

    if args.filter_name!=''  or args.filter_fid!='':
        os.system("""  sqlite3 -line """ +args.database_name+ """    " select * from wd_match;  "     """ )

    print (' - Status -')
    os.system("""  ./proc_report_freq.sh     "_status "     """ )
else:
    os.system("""  sqlite3 -line """ +args.database_name+ """    " select count(*) as N from wd ;  "     """ )    

print (' - JOB end -')

