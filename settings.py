import os

DEFAULTS = {'DB_NAME': 'irec_performance_testing',
            'COLLECTION_NAME': 'recon',
            'CLUSTER_URL': f'mongodb+srv://',
            'DOCS_PER_BATCH': 1000,
            'INSERT_WEIGHT': 0,
            'FIND_WEIGHT': 0,
            'BULK_INSERT_WEIGHT': 10,
            'AGG_PIPE_WEIGHT': 0}


def init_defaults_from_env():
    for key in DEFAULTS.keys():
        value = os.environ.get(key)
        if value:
            DEFAULTS[key] = value


# get the settings from the environment variables
init_defaults_from_env()
