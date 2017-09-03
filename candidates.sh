#!/bin/bash
    
rm -f 1342-candidates.md
wget https://gist.github.com/nvkelso/06393fcfda298c98571bb3d3a3845e8c/raw/0b77c3e67e3cdd4b113d8084e71b92c8fb01401d/1342-candidates.md
cat 1342-candidates.md | cut -d'|' -f1,2,7,8 | sed 's/ | /,/g' | grep ',wd_wiki_id' | sed 's/ *$//'  >  1342-candidates-cleaned.csv
cat 1342-candidates.md | cut -d'|' -f1,2,7,8 | sed 's/ | /,/g' | grep ',1,'         | sed 's/ *$//' >> 1342-candidates-cleaned.csv
cat 1342-candidates-cleaned.csv | wc -l
