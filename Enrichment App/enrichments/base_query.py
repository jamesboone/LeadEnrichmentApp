import logging
logger = logging.getLogger(__name__)

__db__ = 'salesforce'


def _set_query_values(limit, orderby):
    logger.debug('_set_query_values')
    limit_string = ''
    if limit:
        limit_string = "limit %s" % (limit,)

    order_by = ''
    if orderby:
        order_by = "order by Company %s" % (orderby)

    return (limit_string, order_by)


def query_string(query, limit=None, orderby=None):
    logger.debug('getting query string')
    limit_string, order_by = _set_query_values(limit, orderby)
    logger.debug("\n\nQuery Being Used\n")
    query = '%s %s\n%s' % (query, order_by, limit_string)
    logger.debug(query)
    return query
