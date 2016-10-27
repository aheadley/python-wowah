#!/usr/bin/env python2

import uuid
import os.path
import bz2
import glob
import logging
import datetime
import json

# import peewee
from peewee import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger('peewee').setLevel(logging.INFO)

DB_PROXY = Proxy()

class AuctionDataModel(Model):
    guid                = UUIDField(default=uuid.uuid4, index=True, unique=True)

    class Meta:
        database        = DB_PROXY

class Auction(AuctionDataModel):
    auc_id              = IntegerField()
    owner               = CharField(default=u'???', index=True)
    owner_realm         = CharField(default=u'???')

    quantity            = IntegerField()
    buyout              = IntegerField(default=0)

    item_id             = IntegerField(index=True)
    rand                = IntegerField(default=0)
    seed                = IntegerField(default=0)
    context             = IntegerField(default=0)
    item_extra          = TextField(default='{}')

    is_active           = BooleanField(default=True)
    started_at          = DateTimeField(index=True)
    ended_at            = DateTimeField(default=None, null=True, index=True)

    # a mapping of model keys to json object keys
    KEY_MAP             = {
        'auc_id':       'auc',
        'owner':        'owner',
        'owner_realm':  'ownerRealm',
        'quantity':     'quantity',
        'buyout':       'buyout',
        'item_id':      'item',
        'rand':         'rand',
        'seed':         'seed',
        'context':      'context',
    }

    # ignore these keys for the item_extra field
    ITEM_EXTRA_IGNORE_KEYS  = [
        'rand',
        'auc',
        'timeLeft',
        'bid',
        'item',
        'seed',
        'ownerRealm',
        'context',
        'owner',
        'buyout',
        'quantity',
    ]

    @classmethod
    def get_by_auc_id(cls, auc_id, timestamp, day_buffer=7):
        return cls.get(
            (cls.auc_id == auc_id) &
            (cls.started_at <= (timestamp + datetime.timedelta(days=day_buffer))) &
            (cls.started_at >= (timestamp - datetime.timedelta(days=day_buffer)))
        )

    @classmethod
    def json_obj2model_dict(cls, obj, ts):
        model_dict = {
            'started_at': ts,
            'item_extra': json.dumps({k: v for k,v in obj.iteritems() if k not in cls.ITEM_EXTRA_IGNORE_KEYS})
        }
        model_dict.update({k: obj[v] for k,v in cls.KEY_MAP.iteritems()})
        return model_dict

class Snapshot(AuctionDataModel):
    auction             = ForeignKeyField(Auction, related_name='snapshots')
    timestamp           = DateTimeField()

    bid                 = IntegerField()
    time_left           = CharField()

    @classmethod
    def json_obj2model_dict(cls, auction, obj, ts):
        return {
            'auction':      auction,
            'timestamp':    ts,
            'bid':          obj['bid'],
            'time_left':    obj['timeLeft'],
        }

class DataSource(object):
    def __init__(self, path):
        self._path = path.rstrip('/')

    def __iter__(self):
        for data_filename in self._get_data_files():
            logger.info('Reading from: %s', data_filename)
            with bz2.BZ2File(data_filename, 'r') as data_handle:
                try:
                    data = json.load(data_handle)
                except Exception as err:
                    logger.exception(err)
                    continue
            logger.debug('Found %06d auctions...', len(data['auctions']))
            data_ts = datetime.datetime.utcfromtimestamp(
                int(os.path.basename(data_filename).split('-')[1]) / 1000)
            logger.debug('Using timestamp: %s', data_ts)
            yield (data_ts, data)

    def _get_data_files(self):
        return sorted(glob.glob(os.path.join(self._path, '*.json.bz2')))

class DataManager(object):
    def run(self, data_src, batch_size=200):
        # aids = Auction IDs
        for ts, data in data_src:
            active_aids = set(a.auc_id for a in Auction.select().where(
                (Auction.is_active == True) &
                (Auction.started_at >= (ts - datetime.timedelta(days=7))) &
                (Auction.started_at <= ts)
            ))
            logger.debug('Found active auction IDs: %d', len(active_aids))

            ended_aids = list(active_aids - set(a['auc'] for a in data['auctions']))
            logger.debug('Found ended auction IDs: %d', len(ended_aids))
            with DB_PROXY.atomic():
                for i in range(0, len(ended_aids), batch_size):
                    Auction.update(is_active=False, ended_at=ts).where(
                        (Auction.auc_id << ended_aids[i:i+batch_size]) & #probably over a size limit
                        (Auction.started_at >= (ts - datetime.timedelta(days=7))) &
                        (Auction.started_at <= ts)
                    ).execute()
            del ended_aids

            new_auctions = [a for a in data['auctions'] if a['auc'] not in active_aids]
            logger.debug('Found new auctions: %d', len(new_auctions))
            with DB_PROXY.atomic():
                for i in range(0, len(new_auctions), batch_size):
                    Auction.insert_many(
                        [Auction.json_obj2model_dict(a, ts) for a in new_auctions[i:i+batch_size]]
                    ).execute()

            new_auction_models = {a.auc_id: a for a in Auction.select().where(Auction.started_at == ts)}
            new_auction_objs = {a['auc']: a for a in new_auctions}
            new_aids = new_auction_models.keys()
            del new_auctions
            logger.debug('Inserting new auction snapshots: %d', len(new_auction_models))
            with DB_PROXY.atomic():
                for i in range(0, len(new_auction_models), batch_size):
                    Snapshot.insert_many(
                        [Snapshot.json_obj2model_dict(new_auction_models[aid], new_auction_objs[aid], ts) \
                            for aid in new_aids[i:i+batch_size]]
                    ).execute()
            del new_auction_models
            del new_auction_objs

            new_snapshots = [a for a in data['auctions'] if a['auc'] in active_aids]
            new_snapshot_models = {a.auc_id: a for a in Auction.select().where(
                (Auction.is_active == True) &
                (Auction.started_at >= (ts - datetime.timedelta(days=7))) &
                (Auction.started_at < ts)
            )}
            new_snapshot_objs = {a['auc']: a for a in new_snapshots}
            new_aids = new_snapshot_models.keys()
            del new_snapshots
            logger.debug('Inserting old auction snapshots: %d', len(new_snapshot_models))
            with DB_PROXY.atomic():
                for i in range(0, len(new_snapshot_models), batch_size):
                    Snapshot.insert_many(
                        [Snapshot.json_obj2model_dict(new_snapshot_models[aid], new_snapshot_objs[aid], ts) \
                            for aid in new_aids[i:i+batch_size]]
                    ).execute()

    def build_auction_model(self, ts, auc):
        item_extra = json.dumps({k: v for k,v in auc.iteritems() if k not in Auction.ITEM_EXTRA_IGNORE_KEYS})
        auc = Auction(
            item_extra=item_extra,
            started_at=ts,
            **{k: auc[v] for k,v in Auction.KEY_MAP.iteritems()}
        )
        return auc

    def build_snapshot_model(self, ts, auc_data, auc_model):
        snap = Snapshot(
            auction=auc_model,
            timestamp=ts,
            bid=auc_data['bid'],
            time_left=auc_data['timeLeft'],
        )
        return snap


if __name__ == '__main__':
    import sys

    data_dir = sys.argv[1]
    db_fn = sys.argv[2]

    db = SqliteDatabase(db_fn)
    DB_PROXY.initialize(db)
    if not os.path.exists(db_fn):
        db.create_tables([Auction, Snapshot])

    ds = DataSource(data_dir)
    dm = DataManager()
    dm.run(ds, batch_size=50)
