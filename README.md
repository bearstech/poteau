Poteau
======

Send log to [Kibana](https://github.com/elasticsearch/kibana).

"[Poteau](https://fr.wiktionary.org/wiki/poteau)" /pɔ.to/ means "pole" in french.

Install
-------

    pip install -r requirements.txt
    wget https://github.com/tobie/ua-parser/raw/master/regexes.yaml

Test
----

    zcat toto.log.gz | ./poteau/apache.py http://localhost:9200/

Licence
-------

3 terms BSD licence © Mathieu Lecarme.
