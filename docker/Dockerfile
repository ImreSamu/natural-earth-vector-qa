FROM python:3-stretch

RUN apt-get update \
    && apt-get install  -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        wget \
        sqlite3 \
        mc \
        nano \
        unzip \
        parallel \
        sudo \
    && rm -rf /var/lib/apt/lists/

RUN echo "install python libs" \
   && pip3  --no-cache-dir install editdistance \
   && pip3  --no-cache-dir install fiona \
   && pip3  --no-cache-dir install jellyfish \
   && pip3  --no-cache-dir install python-Levenshtein \
   && pip3  --no-cache-dir install requests \
   && pip3  --no-cache-dir install shapely \
   && pip3  --no-cache-dir install SPARQLWrapper \
   && pip3  --no-cache-dir install unidecode \
   && rm -fr ~/.cache/pip

WORKDIR /osm
