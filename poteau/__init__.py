
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
        day = None
        stack = []

        def bulk(day, type_, stack):
            if len(stack):
                body = '\n'.join(stack) + '\n'

                self.es.send_request('POST',
                                 ['_bulk'],
                                 body,
                                 encode_body=False)
                return day, len(stack)

        for document in documents:
            day = document['@timestamp'].split('T')[0]
            action, data = self._bulk_action_data(
                    'logstash-%s' % day.replace('-', '.'), type_, document)
            stack.append(action)
            stack.append(data)
            if len(stack) >= 300:
                yield bulk(day, type_, stack)
                stack = []

        yield bulk(day, type_, stack)
