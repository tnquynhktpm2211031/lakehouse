ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False

HTTP_HEADERS = {'X-Frame-Options': 'ALLOWALL'}
SUPERSET_WEBSERVER_HTTP_HEADERS = {'X-Frame-Options': 'ALLOWALL'}

# Allow embedding Superset
SUPERSET_FEATURE_EMBEDDED_SUPERSET = True
