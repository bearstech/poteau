Poteau
======

Send log to [Kibana](https://github.com/elasticsearch/kibana).

"[Poteau](https://fr.wiktionary.org/wiki/poteau)" /pɔ.to/ means "pole" in french.

Elastic Search is a secret weapon, and Kibana its favorite querying UI.
Kibana is still rough but promising, and easy to install.
[Logstash](http://www.logstash.net/) is a tool for the real world,
but I just want to test Kibana for forensic investigation.
Poteau is a post mortem analysis tool. Something nasty happens? seek it,
and if it cames back, install Logstash and watch it.


Install
-------

Poteau is a simple and hackable Python tool.

    pip install -r requirements.txt
    wget https://github.com/tobie/ua-parser/raw/master/regexes.yaml

Test
----

You need an Elasticsearch and a Kibana, somewhere.

### Apache

    zcat toto.log.gz | ./poteau/apache.py http://localhost:9200/

### Mail

    python -m poteau.mail http://localhost:9200/ /path/to/some/mbox

Licence
-------

3 terms BSD licence © Mathieu Lecarme.
