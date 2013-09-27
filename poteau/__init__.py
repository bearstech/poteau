from pyelasticsearch.exceptions import ElasticHttpNotFoundError


def bulk_iterate(collection, bulk_size):
    """Agnostic way for bulk iteration"""
    stack = []
    for item in collection:
        stack.append(item)
        if len(stack) >= bulk_size:
            yield stack
            stack = []
    if len(stack) > 0:
        yield stack


class Kibana(object):
    mapping = {
            "@fields": {
                "properties": {},
                "tags": {},
                "@message": {
                    "type": "string"
                    },
                "@source": {
                    "type": "string"
                    },
                "@timestamp": {
                    "type": "date",
                    "format": "dateOptionalTime"
                    },
                "@type": {
                    "type": "string"
                    }
                }
            }
    def __init__(self, es):
        self.es = es

    def _bulk_action_data(self, index, doc_type, doc):
        action = {'index': {'_index': index, '_type': doc_type}}

        if doc.get('id') is not None:
            action['index']['_id'] = doc['id']

        return self.es._encode_json(action), self.es._encode_json(doc)

    def index_documents(self, type_, documents):
        """
https://github.com/logstash/logstash/wiki/logstash%27s-internal-message-format
        """

        def bulk(day, type_, stack):
            if len(stack):
                body = '\n'.join(stack) + '\n'

                self.es.send_request('POST',
                                     ['_bulk'],
                                     body,
                                     encode_body=False)
                return day, len(stack)

        day = None
        stack = []
        indices = set()
        for document in documents:
            day = document['@timestamp'].split('T')[0]
            index_name = 'logstash-%s' % day.replace('-', '.')
            if index_name not in indices:
                try:
                    self.es.get_settings(index_name)
                except ElasticHttpNotFoundError:
                    settings = {
                        'settings': {
                            'analysis': {
                                'analyzer': {
                                    'path': {
                                        'tokenizer': 'keyword'
                                    }
                                }
                            }
                        }
                    }
                    assert self.es.create_index(index_name, settings)['ok']
                    self.es.put_mapping(index_name, type_, {type_: {'properties':
                        self.mapping}})
                    print self.es.get_settings(index_name)
                indices.add(index_name)
            action, data = self._bulk_action_data(index_name, type_, document)
            stack.append(action)
            stack.append(data)
            if len(stack) >= 300:
                yield bulk(day, type_, stack)
                stack = []

        yield bulk(day, type_, stack)
