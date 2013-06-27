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

    def index_documents(self, type_, documents):
        """
https://github.com/logstash/logstash/wiki/logstash%27s-internal-message-format
        """
        current = None
        stack = []

        def bulk(current, type_, stack):
            if len(stack):
                self.es.bulk_index('logstash-%s' % current.replace('-', '.'),
                                   type_, stack)
                return current, len(stack)

        for document in documents:
            day = document['@timestamp'].split('T')[0]
            if current is None:
                current = day
            if current != day:
                yield bulk(current, type_, stack)
                stack = [document]
                current = day
            else:
                stack.append(document)
            if len(stack) >= 100:
                yield bulk(current, type_, stack)
                stack = []

        yield bulk(current, type_, stack)
