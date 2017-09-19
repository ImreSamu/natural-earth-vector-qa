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

                ne_fid  text,
                ne_wikidataid text,
                wd_id text,
                ne_name text,
                wd_label text,
                ne_adm0name text ,
                wd_countrylabel text,
                ne_adm1name text ,
                ne_ls_name text,
                ne_namealt text,
                wd_description text,
                wd_type text,
                ne_geonameid text,
                wd_geonames_id_grp text,
                _geonames_status text,
                wd_place_alternative_grp text,
                wd_sitelink_en text,
                wd_sitelink_es text,   
                wd_sitelink_ru text,  
                wd_sitelink_zh text,                                                   
                wd_sitelink_ceb text,
                wd_label_ru text,                
                wd_has_sistercity text,
                wd_max_population text,
                wd_distance real,
                _step text,
                _score real,
                _name_status text,
                _wikidata_status text,
                _in_altnames text,
                _lev_ratio real,
                _lev_distance real,
                _lev_jaro real,
                _lev_jaro_winkler real,
                ne_scalerank text,
                ne_labelrank text,
                ne_natscale text,
                ne_xid text,
                ts timestamp,
                _search_distance real,
                _retries integer, 
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
            (group_concat(distinct  ?placeLabelru ; separator = "#")        as ?placeLabelru)               
            (group_concat(distinct  ?sitelink_en  ; separator = "#")        as ?sitelink_en)
            (group_concat(distinct  ?sitelink_es  ; separator = "#")        as ?sitelink_es)  
            (group_concat(distinct  ?sitelink_ru  ; separator = "#")        as ?sitelink_ru)   
            (group_concat(distinct  ?sitelink_zh  ; separator = "#")        as ?sitelink_zh)                                    
            (group_concat(distinct  ?sitelink_ceb ; separator = "#")        as ?sitelink_ceb)
            (group_concat(distinct  ?countryLabelx; separator = "#")        as ?countryLabel)
            (SAMPLE(?sistercity)                                            as ?sistercity_sample)
            (AVG(?distance)                                                 as ?distance   )
            (MAX(?population)                                               as ?max_population )
            (group_concat(distinct ?place_alternative ; separator = "#")    as ?place_alternative_grp)
            (group_concat(distinct ?GeoNames_ID       ; separator = "#")    as ?GeoNames_ID_grp)
        WITH {
            SELECT DISTINCT ?place ?distance {

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

                    #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q11618417.  }
                    #S4#     UNION  {?place p:P31/ps:P31               wd:Q11618417. }
                    #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q640364.  }
                    #S4#     UNION  {?place p:P31/ps:P31               wd:Q640364. }
                    #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q2555896.  }
                    #S4#     UNION  {?place p:P31/ps:P31               wd:Q2555896. }
                    #S4#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q109108.  }
                    #S4#     UNION  {?place p:P31/ps:P31               wd:Q109108. }



                    #S5#            {?place p:P31/ps:P31/wdt:P279*     wd:Q1763214.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q1763214. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q1840161.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q1840161. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q4249901.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q4249901. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q3685463.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q3685463. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q12081657.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q12081657. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q27676416.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q27676416. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q3076994.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q3076994. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q3360771.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q3360771. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q3685463.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q3685463. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q605291.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q605291. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q1539014.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q1539014. }


  

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q7830262.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q7830262. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q3327862.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q3327862. }


                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q956318.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q956318. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q155239.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q155239. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q27676428.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q27676428. }

                    #S5#     UNION  {?place p:P31/ps:P31  wd:Q5084.  ?place p:P17/ps:P17  wd:Q16. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q17305746.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q17305746. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q14762300.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q14762300. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q17366755.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q17366755. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q3327873.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q3327873. }

                    #S5#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q3788231.  }
                    #S5#     UNION  {?place p:P31/ps:P31               wd:Q3788231. }

            # --- S6 -------------------

                    #S6#            {?place p:P31/ps:P31/wdt:P279*     wd:Q6609799.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q6609799. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q3685430.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q3685430. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q2679157.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q2679157. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q2989470.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q2989470. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q6593035.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q6593035. }


                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q43742.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q43742. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q83020.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q83020. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q2706302.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q2706302. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q482821.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q482821. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q2225003.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q2225003. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q133442.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q133442. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q1500350.  }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q1500350. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q16725943. }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q16725943. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q9316670. }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q9316670. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q1065118. }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q1065118. }
                     
                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q1289426. }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q1289426. }

                    #S6#     UNION  {?place p:P31/ps:P31/wdt:P279*     wd:Q1336099. }
                    #S6#     UNION  {?place p:P31/ps:P31               wd:Q1336099. }
                                   
                    #S6#     {
                    #S6#      ?place (p:P31/wdt:P31/wdt:P279*) wd:Q486972 .
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q131596.    }.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q5084.      }.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q2514025    }.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P36 ?capitalplace  }.
                    #S6#     # FILTER(NOT EXISTS  { ?item rdfs:label ?lang_labelx. FILTER(LANG(?lang_labelx) = "en")  }).
                    #S6#      ?place rdfs:label ?placeLabel_xru  FILTER (lang(?placeLabel_xru) = "ru").
                    #S6#     }
                    #S6#     UNION {
                    #S6#      ?place p:P31/ps:P31  wd:Q486972.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q131596.    }.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q5084.      }.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q2514025    }.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P36 ?capitalplace  }.
                    #S6#      #FILTER(NOT EXISTS  { ?item rdfs:label ?lang_labelx. FILTER(LANG(?lang_labelx) = "en")  }).
                    #S6#      ?place rdfs:label ?placeLabel_xru  FILTER (lang(?placeLabel_xru) = "ru").
                    #S6#     }
                    #S6#     UNION {
                    #S6#      ?place p:P31/ps:P31/wdt:P279*  wd:Q486972.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q131596.    }.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q5084.      }.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P31 wd:Q2514025    }.
                    #S6#      FILTER NOT EXISTS  { ?place wdt:P36 ?capitalplace  }.
                    #S6#      #FILTER(NOT EXISTS  { ?item rdfs:label ?lang_labelx. FILTER(LANG(?lang_labelx) = "en")  }).
                    #S6#      ?place rdfs:label ?placeLabel_xru  FILTER (lang(?placeLabel_xru) = "ru").
                    #S6#     }

                    #S7#     FILTER EXISTS { ?place wdt:P190 ?sistercity_x.}

                    #S8#     VALUES ?GeoNames_ID {"3383494"}
                    #S8#     ?place wdt:P1566 ?GeoNames_ID.

                    #S9#      VALUES ?searchnames {"#ne_name#"@en "#ne_name#"@es "#ne_name#"@sv 
                    #S9#                           "#ne_name#"@de "#ne_name#"@fr "#ne_name#"@pt 
                    #S9#                           "#ne_name#"@it "#ne_name#"@da "#ne_name#"@pl
                    #S9#                           "#ne_name#"@cz "#ne_name#"@sk "#ne_name#"@hu
                    #S9#                           "#ne_name#"@lt "#ne_name#"@et "#ne_name#"@lv                    
                    #S9#                           "#ne_name#"@no "#ne_name#"@nl "#ne_name#"@fi  }  
                    #S9#      ?place rdfs:label ?searchnames .

                    SERVICE wikibase:around {     # "#ne_name#" , "#ne_adm0name#"
                        ?place wdt:P625 ?location.
                        bd:serviceParam wikibase:center "Point(16.373064 48.20833)"^^geo:wktLiteral.
                        bd:serviceParam wikibase:radius "#distance#".
                        bd:serviceParam wikibase:distance ?distance.
                    }
                }
            } AS %places
            WHERE {
            INCLUDE %places .
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en".}
            OPTIONAL {?place rdfs:label ?placeLabelru FILTER (lang(?placeLabelru)="ru").}
            OPTIONAL {?place wdt:P31 ?property. ?property rdfs:label ?pLabel FILTER (lang(?pLabel)="en").}
            OPTIONAL {?place wdt:P17 ?country. ?country rdfs:label ?countryLabelx FILTER (lang(?countryLabelx)="en").}
            OPTIONAL {?place wdt:P17       ?country.}
            OPTIONAL {?place wdt:P1566     ?GeoNames_ID.}
            OPTIONAL {?place wdt:P190      ?sistercity.}
            OPTIONAL {?place wdt:P1082     ?population.}
            OPTIONAL {?sitelink_en  schema:about ?place . ?sitelink_en schema:isPartOf  <https://en.wikipedia.org/>.}
            OPTIONAL {?sitelink_es  schema:about ?place . ?sitelink_es schema:isPartOf  <https://es.wikipedia.org/>.}  
            OPTIONAL {?sitelink_ru  schema:about ?place . ?sitelink_ru schema:isPartOf  <https://ru.wikipedia.org/>.}   
            OPTIONAL {?sitelink_zh  schema:about ?place . ?sitelink_zh schema:isPartOf  <https://zh.wikipedia.org/>.}                                  
            OPTIONAL {?sitelink_ceb schema:about ?place . ?sitelink_ceb schema:isPartOf <https://ceb.wikipedia.org/>.}
            OPTIONAL {?place skos:altLabel ?place_alternative   FILTER((LANG(?place_alternative)) = "en").}
        }
        GROUP BY ?place ?placeLabel   ?placeDescription
        ORDER BY ?distance
    """

    q=query_template.replace('16.373064',ne_lon).replace('48.20833',ne_lat)
    q=q.replace('#ne_name#',ne_name).replace('#ne_adm0name#',ne_adm0name)
    q=q.replace('"3383494"','"'+ne_geonameid+'"')

    if   _step==1:
        q=q.replace('#S1#','')
    elif _step==2:
        q=q.replace('#S2#','')
    elif _step==3:
        q=q.replace('#S3#','')
    elif _step==4:
        q=q.replace('#S4#','')
    elif _step==5:
        q=q.replace('#S5#','')
    elif _step==6:
        q=q.replace('#S6#','')    
    elif _step==7:
        q=q.replace('#S7#','')
    elif _step==8:
        q=q.replace('#S8#','')
    elif _step==9:
        q=q.replace('#S9#','')                
    else:
        print("Internal error, _step: ", _step )
        sys.exit(1)



    search_distance=0
    if  ( -10 <=  float(ne_lon) <= 60)  and  (  float(ne_lat) >30  ):
        if   _step==1:
            search_distance=50
        elif _step==2:
            search_distance=50
        elif _step==3:
            search_distance=50
        elif _step==4:
            search_distance=50
        elif _step==5:
            search_distance=50
        elif _step==6:
            search_distance=50
        elif _step==7:
            search_distance=50                        
        elif _step==8:
            search_distance=1200
        elif _step==9:
            search_distance=100

    else:
        if   _step==1:
            search_distance=150
        elif _step==2:
            search_distance=150
        elif _step==3:
            search_distance=120
        elif _step==4:
            search_distance=100
        elif _step==5:
            search_distance=100            
        elif _step==6:
            search_distance=100
        elif _step==7:
            search_distance=100            
        elif _step==8:
            search_distance=1200
        elif _step==9:
            search_distance=100


    print("_step:",_step , "    search_distance=", search_distance)


    # remove double spaces
    while '  ' in q:
        q = q.replace('  ', ' ')

    # remove comments
    qs=''
    for line in q.splitlines():
        if len(line)>0 and line[:2] != ' #'  and  line[:2] != '#S' :
            qs+=line+'\n'
    q=qs

    ts = datetime.datetime.now()

    max_score=-1000

    results = None
    retries = 0
    max_retries=14
    while results == None and retries <  max_retries:
        try:

            results = None

            sleeptime= retries*10 + 5

            qs=q.replace('#distance#', str(search_distance) )
            print("distance-ok")
            if retries > 0:
                print("Try - retries:",retries,"   Distance:",search_distance," Sleeptime:",sleeptime)
            if args.filter_name!='':
                print(qs)
            sparql.setQuery(qs)
            sparql.setTimeout(2000)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

        except SPARQLExceptions.EndPointNotFound as e:
            print("ERRwikidata-SPARQLExceptions-EndPointNotFound:  Retrying in (seconds) : ",sleeptime, flush=True )
            time.sleep(sleeptime)
            retries += 1
            continue

        except SPARQLExceptions.EndPointInternalError as e:
            print("ERRwikidata-SPARQLExceptions-EndPointInternalError: Retrying in (seconds) : ",sleeptime, flush=True )
            time.sleep(sleeptime)
            retries += 1
            # Decrease search distance
            if retries > 3:
                search_distance=int( search_distance*0.9)
            continue

        except TimeoutError:
            print("ERRwikidata-SPARQLExceptions  TimeOut : Retrying in (seconds) : ",sleeptime, flush=True )
            time.sleep(sleeptime)
            retries += 1
            continue

        except SPARQLExceptions.QueryBadFormed as e:
            print("ERRwikidata-SPARQLExceptions-QueryBadFormed : Check!  "  ,  flush=True )
            return "error"

        except HTTPError as e:
            print("ERRwikidata: Got an HTTPError while querying. Retrying in (seconds) : ",sleeptime, flush=True )
            time.sleep(sleeptime)
            retries += 1
            continue

        except:
            print("ERRwikidata: other error. Retrying in (seconds) : ",sleeptime,  flush=True )
            time.sleep(sleeptime)
            retries += 1
            continue


    if results == None and retries >=  max_retries :
        print("Wikidata request failed ; system stopped! ")
        sys.exit(1)

    _runtime=   (datetime.datetime.now() - ts).total_seconds()



    rc_list_wikidataid=[]
#TODO empty answer ..

    for result in results['results']['bindings']:

        _score=0;

        wd_id = result['place']['value'].split('/')[4]


        wd_distance = float( result['distance']['value'] )

        if 'placeLabel' in result:
            wd_label = result['placeLabel']['value']
        else:
            wd_label = ''



        # Check if already queryed?
        if wd_id in list_wikidataid:
            print("Already exist:", wd_id, wd_label)
            continue
        else:
            rc_list_wikidataid.append(wd_id)

        if 'placeLabelru' in result:
            wd_label_ru = result['placeLabelru']['value']
        else:
            wd_label_ru = ''


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


        if 'sitelink_en' in result:
            wd_sitelink_en = result['sitelink_en']['value']
        else:
            wd_sitelink_en=''


        if wd_sitelink_en != '':
            _score+=   40
        else:
            _score+=  -120
            

        if 'sitelink_es' in result:
            wd_sitelink_es = result['sitelink_es']['value']
        else:
            wd_sitelink_es=''

        if 'sitelink_ru' in result:
            wd_sitelink_ru = result['sitelink_ru']['value']
        else:
            wd_sitelink_ru=''

        if 'sitelink_zh' in result:
            wd_sitelink_zh = result['sitelink_zh']['value']
        else:
            wd_sitelink_zh=''

        if 'sitelink_ceb' in result:
            wd_sitelink_ceb = result['sitelink_ceb']['value']

        else:
            wd_sitelink_ceb=''
 


        if wd_sitelink_en == '':
            if wd_sitelink_es != '':
                _score+= 100
            elif wd_sitelink_ru != '':
                _score+= 80
            elif wd_sitelink_zh != '':
                _score+= 60
            elif wd_sitelink_ceb != '':
                _score+=  -1000        # penalty for   only ceb import






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
            _score+=72
        if ('#'+unidecode.unidecode(ne_name)+'#' in unidecode.unidecode(wd_place_alternative_grp))  :
            _in_altnames='Y'
            _score+=58
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
        uni_ne_adm0name=unidecode.unidecode(ne_adm0name)
        uni_ne_adm1name=unidecode.unidecode(ne_adm1name)

        uni_wd_name=unidecode.unidecode(wd_label)

        if wd_label==wd_id and wd_label_ru != '':    
            _lev_jaro_winkler_ru = Levenshtein.jaro_winkler( uni_ne_name, unidecode.unidecode(wd_label_ru))
        else:
            _lev_jaro_winkler_ru =  0

        _lev_ratio        = Levenshtein.ratio(uni_ne_name, uni_wd_name)
        _lev_distance     = Levenshtein.distance(uni_ne_name, uni_wd_name)
        _lev_jaro         = Levenshtein.jaro(uni_ne_name, uni_wd_name)

        _lev_jaro_winkler       = Levenshtein.jaro_winkler(uni_ne_name, uni_wd_name)
        _lev_jaro_winkler_ls    = Levenshtein.jaro_winkler(uni_ne_ls_name, uni_wd_name)
        _lev_jaro_winkler_alt   = Levenshtein.jaro_winkler(uni_ne_namealt, uni_wd_name)

        _lev_jaro_winkler_adm0  = Levenshtein.jaro_winkler(uni_ne_name+','+uni_ne_adm0name, uni_wd_name )
        _lev_jaro_winkler_adm1  = Levenshtein.jaro_winkler(uni_ne_name+','+uni_ne_adm1name, uni_wd_name )

        _max_lev_jaro_winkler = max(_lev_jaro_winkler,_lev_jaro_winkler_ls,_lev_jaro_winkler_alt,_lev_jaro_winkler_adm0,_lev_jaro_winkler_adm1, _lev_jaro_winkler_ru)

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


        c.execute("INSERT INTO wd VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (

                        ne_fid,
                        ne_wikidataid,
                        wd_id,
                        ne_name,
                        wd_label,
                        ne_adm0name,
                        wd_countrylabel,
                        ne_adm1name,
                        ne_ls_name,
                        ne_namealt,
                        wd_description,
                        wd_type,
                        ne_geonameid,
                        wd_geonames_id_grp,
                        _geonames_status,
                        wd_place_alternative_grp,
                        wd_sitelink_en,
                        wd_sitelink_es,   
                        wd_sitelink_ru,  
                        wd_sitelink_zh,                                                                          
                        wd_sitelink_ceb,
                        wd_label_ru,
                        wd_has_sistercity,
                        wd_max_population,
                        wd_distance,
                        _step,
                        _score,
                        _name_status,
                        _wikidata_status,
                        _in_altnames,
                        _lev_ratio,
                        _lev_distance,
                        _lev_jaro,
                        _lev_jaro_winkler,
                        ne_scalerank,
                        ne_labelrank,
                        ne_natscale,
                        ne_xid,
                        ts,
                        search_distance,
                        retries,
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


            # if max(_maxscore1,_maxscore2)  < 140:
            rc3_list_wikidataid , _maxscore3 =getwikidatacity(3, rc2_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
            print('3:',len(rc3_list_wikidataid) , _maxscore3)

            # if _maxscore3 < 140:
            rc4_list_wikidataid , _maxscore4 =getwikidatacity(4, rc3_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
            print('4:',len(rc4_list_wikidataid) , _maxscore4)


            # if _maxscore4 < 140:
            rc5_list_wikidataid , _maxscore5 =getwikidatacity(5, rc4_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
            print('5:',len(rc5_list_wikidataid) , _maxscore5)

            rc6_list_wikidataid , _maxscore6 =getwikidatacity(6, rc5_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
            print('6:',len(rc6_list_wikidataid) , _maxscore6)

            rc7_list_wikidataid , _maxscore7 =getwikidatacity(7, rc6_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
            print('7:',len(rc7_list_wikidataid) , _maxscore7)


            if ne_geonameid!='-1':
                rc8_list_wikidataid , _maxscore8 =getwikidatacity(8, rc7_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                    ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
                print('8:',len(rc8_list_wikidataid) , _maxscore8)
            else:
                rc8_list_wikidataid = rc7_list_wikidataid
                _maxscore8 = _maxscore7

            if (_maxscore8 < 100)  or ne_adm0name in ('Russia','China','Kazakhstan')  :
                rc9_list_wikidataid , _maxscore9 =getwikidatacity(9, rc8_list_wikidataid, ne_fid, ne_xid, ne_lon, ne_lat, ne_wikidataid,ne_name,
                    ne_namealt,ne_adm0name,ne_adm1name,ne_ls_name,ne_geonameid, ne_scalerank,ne_labelrank,ne_natscale)
                print('9:',len(rc9_list_wikidataid) , _maxscore9)



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

