from haystack import connection_router
from django.conf import settings
import logging
import requests

# Get an instance of a logger
logger = logging.getLogger(__name__)

COMPARE_URL='{0}/compare'
QUERY_FORM = 'volume_id:{0} AND parse_type:"{1}"'

def _is_sequence(arg):
    return (not hasattr(arg, "strip") and
            hasattr(arg, "__getitem__") or
            hasattr(arg, "__iter__"))

def basic_search(source, target, language, start=0, rows=10, stopword_list=None):

    if language != 'latin':
        raise Exception('Only latin is supported for now. Sorry.')

    conn_alias = connection_router.for_read()
    if isinstance(conn_alias, (list, tuple)) and len(conn_alias):
        # We can only effectively read from one engine
        conn_alias = conn_alias[0]
    hs_info = settings.HAYSTACK_CONNECTIONS[conn_alias]
    solr_url = hs_info['URL']

    get_params = {
        'wt': 'python', # bitchin
        'tess.sq': QUERY_FORM.format(source.id, 'line'),
        'tess.sf': 'text',
        'tess.sfl': 'volume,author,text,title',
        'tess.tq': QUERY_FORM.format(target.id, 'line'),
        'tess.tf': 'text',
        'tess.tfl': 'volume,author,text,title',
        'start': str(start),
        'rows': str(rows)
    }

    if stopword_list is not None:
        if _is_sequence(stopword_list):
            stopword_list = ','.join(stopword_list)
        elif not isinstance(stopword_list, (str, unicode)):
            raise ValueError('invalid type for stopword_list, expected a string or something iterable')
        get_params['tess.sl'] = str(stopword_list)

    response = requests.get(COMPARE_URL.format(solr_url), params=get_params)
    response.raise_for_status()

    # This couldn't possibly be abused... cough
    return eval(str(response.text))
