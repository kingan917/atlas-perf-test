from locust import between

from mongo_user import MongoUser, mongodb_task
from settings import DEFAULTS

import json
import pymongo
import random
import string
import datetime as dt
from collections import deque
from typing import Any, Dict, List, Optional

# Optional pydantic validation (falls back gracefully if not installed)
try:
    from pydantic import BaseModel, root_validator, create_model
    HAS_PYDANTIC = True
except Exception:
    HAS_PYDANTIC = False

# number of cache entries for queries
IDS_TO_CACHE = 1000


class SchemaField:
    def __init__(self, name: str, ftype: str, allowed_values: Optional[List[Any]] = None,
                 unique: bool = False, skewed: bool = False, unique_range: Optional[int] = None):
        self.name = name
        self.ftype = ftype  # 'int' | 'string' | 'date'
        self.allowed_values = allowed_values
        self.unique = unique
        self.skewed = skewed
        self.unique_range = unique_range


class SchemaGenerator:
    def __init__(self, spec: List[SchemaField], faker):
        self.spec = spec
        self.faker = faker
        # Track uniqueness per field
        self._unique_counters: Dict[str, int] = {}
        # Precompute unique-range pools per field
        self._unique_range_pools: Dict[str, deque] = {}
        # Seed to reduce collision risk across distributed workers
        self._seed = random.randint(1, 1_000_000_000)

        for field in self.spec:
            if field.unique:
                self._unique_counters[field.name] = 0
            if field.unique_range:
                rng = field.unique_range
                # Make a rotated deque to vary starting points across workers
                pool = deque(range(rng))
                pool.rotate(self._seed % rng)
                self._unique_range_pools[field.name] = pool

        # Optional Pydantic model for validation
        self._validator_model = None
        if HAS_PYDANTIC:
            field_defs = {}
            allowed_map = {}
            for field in self.spec:
                py_type = int if field.ftype == 'int' else (str if field.ftype == 'string' else dt.datetime)
                field_defs[field.name] = (py_type, ...)
                if field.allowed_values:
                    allowed_map[field.name] = set(field.allowed_values)

            def _check_allowed(values):
                for k, allowed in allowed_map.items():
                    if k in values and values[k] not in allowed:
                        raise ValueError(f"Field {k} must be one of {sorted(list(allowed))}, got {values[k]}")
                return values

            validators = {}
            if allowed_map:
                validators['check_allowed'] = root_validator(pre=True)(
                    lambda cls, v: _check_allowed(v)
                )
            self._validator_model = create_model('ReconRecordModel', __validators__=validators, **field_defs)

    def _gen_value(self, field: SchemaField) -> Any:
        # Allowed values take precedence
        if field.allowed_values:
            return random.choice(field.allowed_values)

        if field.ftype == 'int':
            if field.unique:
                self._unique_counters[field.name] += 1
                # Add large seeded prefix to lower collision probability across workers
                return self._seed * 1_000_000 + self._unique_counters[field.name]
            if field.unique_range:
                pool = self._unique_range_pools[field.name]
                val = pool[0]
                pool.rotate(-1)
                return val
            if field.skewed:
                # 80/20 skew: 80% from a small hot set, 20% spread wider
                if random.random() < 0.8:
                    return random.randint(0, 9)  # hot bucket
                return random.randint(10, 10_000)
            return random.randint(0, 10_000)

        if field.ftype == 'string':
            # Generic alphanumeric if not enumerated
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

        if field.ftype == 'date':
            # Use datetime for MongoDB compatibility
            return self.faker.date_time_between(start_date='-2y', end_date='now')

        # Fallback
        return None

    def generate(self) -> Dict[str, Any]:
        doc = {f.name: self._gen_value(f) for f in self.spec}
        if self._validator_model is not None:
            # Validate types & enumerations
            self._validator_model(**doc)
        return doc


class MongoSampleUser(MongoUser):
    """
    Mongodb workload generator using schema100.json (full validation / richer generation)
    """
    # no delays between operations
    wait_time = between(0.0, 0.0)

    def __init__(self, environment):
        super().__init__(environment)
        self.id_cache: List[int] = []
        self.generator: Optional[SchemaGenerator] = None

    def _load_schema(self) -> List[SchemaField]:
        with open('schema100.json', 'r') as f:
            raw = json.load(f)
        spec = []
        for item in raw:
            spec.append(
                SchemaField(
                    name=item.get('name'),
                    ftype=item.get('type'),
                    allowed_values=item.get('allowed_values'),
                    unique=bool(item.get('unique')) if 'unique' in item else False,
                    skewed=bool(item.get('skewed')) if 'skewed' in item else False,
                    unique_range=item.get('unique_range'),
                )
            )
        return spec

    def generate_new_document(self) -> Dict[str, Any]:
        if not self.generator:
            raise RuntimeError('Schema generator not initialized')
        return self.generator.generate()

    @mongodb_task(weight=int(DEFAULTS['AGG_PIPE_WEIGHT']))
    def run_aggregation_pipeline(self):
        """
        Run an aggregation pipeline on a secondary node
        """
        # Count number of records per STATUS (enum in schema)
        group_by = {
            '$group': {
                '_id': '$STATUS',
                'total_records': {'$sum': 1}
            }
        }

        set_columns = {'$set': {'STATUS': '$_id'}}
        unset_columns = {'$unset': ['_id']}
        order_by = {'$sort': {'total_records': pymongo.DESCENDING}}

        pipeline = [group_by, set_columns, unset_columns, order_by]
        return list(self.collection_secondary.aggregate(pipeline))

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # Prepare the collection and indexes consistent with schema
        # indexes = [
        #     pymongo.IndexModel([('RECON_RECORD_ID', pymongo.ASCENDING)], name='idx_recon_rec_id', unique=False),
        #     pymongo.IndexModel([('STATUS', pymongo.ASCENDING)], name='idx_status'),
        # ]
        indexes = []
        self.collection, self.collection_secondary = self.ensure_collection(DEFAULTS['COLLECTION_NAME'], indexes)

        # Load schema and prepare generator
        spec = self._load_schema()
        self.generator = SchemaGenerator(spec, self.faker)
        self.id_cache = []

    @mongodb_task(weight=int(DEFAULTS['INSERT_WEIGHT']))
    def insert_single_document(self):
        document = self.generate_new_document()

        # cache RECON_RECORD_ID for queries if present
        rec_id = document.get('RECON_RECORD_ID')
        if rec_id is not None:
            if len(self.id_cache) < IDS_TO_CACHE:
                self.id_cache.append(rec_id)
            else:
                # occasionally replace an entry to keep it fresh
                if random.randint(0, 9) == 0:
                    self.id_cache[random.randint(0, len(self.id_cache) - 1)] = rec_id

        self.collection.insert_one(document)

    @mongodb_task(weight=int(DEFAULTS['FIND_WEIGHT']))
    def find_document(self):
        # at least one insert needs to happen
        if not self.id_cache:
            return

        # find a random document using an indexable field
        rec_id = random.choice(self.id_cache)
        self.collection.find_one({'RECON_RECORD_ID': rec_id})

    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=int(DEFAULTS['DOCS_PER_BATCH']))
    def insert_documents_bulk(self):
        docs = [self.generate_new_document() for _ in range(int(DEFAULTS['DOCS_PER_BATCH']))]
        # refresh cache with some of the generated IDs
        for d in docs:
            rid = d.get('RECON_RECORD_ID')
            if rid is not None and len(self.id_cache) < IDS_TO_CACHE:
                self.id_cache.append(rid)
        self.collection.insert_many(docs)

