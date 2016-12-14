#!/usr/bin/env python2

import uuid
import os.path
import bz2
import glob
import logging
import datetime
import json
import re
import itertools

# import peewee
from peewee import *
from playhouse.db_url import connect as db_url_connect
from tqdm import tqdm

# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger('peewee').setLevel(logging.INFO)
LOG_FORMAT = logging.Formatter('[%(asctime)s] %(levelname)8s - %(name)s: %(message)s')
LOG_HANDLER = logging.StreamHandler()
LOG_HANDLER.setFormatter(LOG_FORMAT)
logger.addHandler(LOG_HANDLER)
logger.setLevel(logging.DEBUG)

OPTION_DISABLE_PROGRESS_BAR         = True

class GlobalMeta:
    database            = Proxy()

class DataModel(Model):
    # guid                = UUIDField(default=uuid.uuid4, index=True, unique=True)
    Meta = GlobalMeta

class Auction(DataModel):
    auc_id              = IntegerField(index=True)
    owner               = CharField(default=None, null=True, index=True)
    owner_realm         = CharField(default=None, null=True)

    quantity            = IntegerField(
        constraints=[Check('quantity > 0')])
    buyout              = IntegerField(null=True, index=True)

    item_id             = IntegerField(index=True,
        constraints=[Check('item_id > 0')])
    rand                = IntegerField(default=0)
    seed                = IntegerField(default=0)
    context             = IntegerField(default=0)

    started_at          = DateTimeField(index=True)
    ended_at            = DateTimeField(default=None, null=True, index=True)

    created_at          = DateTimeField(default=datetime.datetime.utcnow)
    est_result          = CharField(default=None, null=True)
    est_ended_at        = DateTimeField(default=None, null=True)
    est_started_at      = DateTimeField(default=None, null=True)

    class Meta(GlobalMeta):
        indexes         = (
            (('auc_id', 'owner_realm', 'started_at'), True),
        )

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

    ITEM_META_IGNORE_KEYS   = KEY_MAP.values() + [
        'timeLeft',
        'bid',
    ]

    @classmethod
    def get_by_id(cls, auc_id, timestamp=None, day_buffer=7):
        dt = datetime.timedelta(days=day_buffer)
        if timestamp is None:
            timestamp = datetime.datetime.utcnow()
        return cls.get(
            (cls.auc_id == auc_id) &
            cls.started_at.between(timestamp - dt, timestamp + dt)
        )

    @classmethod
    def from_json(cls, obj, ts):
        model_dict = {
            'started_at': ts,
        }
        for k,v in cls.KEY_MAP.iteritems():
            model_dict[k] = obj[v]
        return model_dict

    @property
    def bid_count(self):
        snaps = sorted(self.snapshots, key=lambda s: s.timestamp)
        bid_count = len([None for i in range(len(snaps)-1) if snaps[i+1].bid - snaps[i].bid])
        return bid_count

    @property
    def buyout_ppi(self):
        return (self.buyout / self.quantity) if self.buyout is not None else None

    def estimate_ended_at(self):
        # if self.ended_at is not None:
        #     return self.ended_at
        snaps = sorted(self.snapshots, key=lambda s: s.timestamp)
        return self.started_at + \
            (datetime.timedelta(seconds=snaps[0].time_left) - \
                (len(snaps) * datetime.timedelta(hours=1))) + \
            (self.bid_count * datetime.timedelta(minutes=5))


    def estimate_result(self, force=False):
        if self.ended_at is None:
            raise ValueError('Auction has not ended')
        if self.est_result is not None and not force:
            return self.est_result
        snaps = Snapshot.select().where(Snapshot.auction == self).order_by(Snapshot.timestamp.asc())
        auction_siblings = self.get_siblings()
        final_snapshot_siblings = snaps[-1].get_siblings()

        run_time = self.ended_at - self.started_at
        time_expired = snaps[-1].time_left <= Snapshot.TIME_LEFT_ENUM['MEDIUM']
        had_bids = snaps[0].bid != max(s.bid for s in snaps)
        was_bid_only = self.buyout is None
        was_lowest_bid = (len(final_snapshot_siblings) == 0) or \
            (snaps[-1].bid_ppi == min(s.bid_ppi for s in final_snapshot_siblings) or \
            all(s.auction.ended_at == self.ended_at for s in final_snapshot_siblings \
                if s.bid_ppi < snaps[-1].bid_ppi))
        # have to include the bid_only condition or the results might not make sense
        was_lowest_buyout = (not was_bid_only) and \
            ((len(auction_siblings) == 0) or \
            ((self.buyout_ppi == min(a.buyout_ppi for a in auction_siblings) or \
            all(a.ended_at == self.ended_at for a in auction_siblings \
                if a.buyout_ppi < self.buyout_ppi))))

        if time_expired:
            if had_bids or (was_lowest_bid and \
                    run_time.total_seconds() > Snapshot.TIME_LEFT_ENUM['LONG']):
                result = 'WON_BID'
            else:
                result = 'EXPIRED'
        else:
            if was_lowest_buyout:
                result = 'WON_BUYOUT'
            else:
                result = 'CANCELLED'

        # self.est_result = result
        # self.save()
        return result

    # this can have false positives due to the (lack of) granularity of sample times
    def get_siblings(self, strict=False):
        conditions = \
            (Auction.id != self.id) & \
            (Auction.item_id == self.item_id) & \
            Auction.buyout.is_null(self.buyout is None)
        if self.ended_at is None:
            conditions &= \
                ((Auction.started_at >= self.started_at) | \
                Auction.ended_at.is_null(True))
        else:
            conditions &= \
                (Auction.started_at.between(self.started_at, self.ended_at) | \
                Auction.ended_at.between(self.started_at, self.ended_at))
        return Auction.select().where(conditions).order_by(Auction.started_at.asc())

class Snapshot(DataModel):
    auction             = ForeignKeyField(Auction, related_name='snapshots')
    timestamp           = DateTimeField(index=True)

    bid                 = IntegerField(
        constraints=[Check('bid > 0')])
    time_left           = IntegerField()

    TIME_LEFT_ENUM      = {
        'VERY_LONG':        48 * 60 * 60,
        'LONG':             12 * 60 * 60,
        'MEDIUM':            2 * 60 * 60,
        'SHORT':                 30 * 60,
    }

    @classmethod
    def from_json(cls, auction, obj, ts):
        return {
            'auction':      auction,
            'timestamp':    ts,
            'bid':          obj['bid'],
            'time_left':    obj['timeLeft'],
        }

    @property
    def bid_ppi(self):
        return self.bid / self.auction.quantity

    def get_siblings(self, strict=True):
        return Snapshot.select().join(Auction).where(
            (Snapshot.id != self.id) &
            (Auction.item_id == self.auction.item_id) &
            (Snapshot.timestamp == self.timestamp)
        )

class ItemAttribute(DataModel):
    auction             = ForeignKeyField(Auction, related_name='item_attrs')

    attribute           = CharField(index=True)
    value               = IntegerField(index=True)

    @classmethod
    def from_json(cls, auction, obj):
        for k in set(obj.keys()) - set(Auction.ITEM_META_IGNORE_KEYS):
            key = k
            if k == 'modifiers':
                for m in obj[k]:
                    key = '{}-{}-{}'.format(k, 'type', m['type'])
                    yield {'auction': auction, 'attribute': key, 'value': m['value']}
            elif k == 'bonusLists':
                for b in obj[k]:
                    key = 'bonusListId'
                    yield {'auction': auction, 'attribute': key, 'value': b['bonusListId']}
            else:
                yield {'auction': auction, 'attribute': key, 'value': obj[key]}

class ParsedFile(DataModel):
    realm_key           = CharField()
    hash                = CharField()
    timestamp           = DateTimeField(index=True, unique=True)

MODELS = [Auction, Snapshot, ItemAttribute, ParsedFile]

def db_connect(db_url, meta_model=GlobalMeta):
    db = db_url_connect(db_url)
    meta_model.database.initialize(db)
    meta_model.database.create_tables(MODELS, safe=True)
    return db

class DataSource(object):
    RE_DATA_FN = re.compile(r'auctions-(?P<ts>[0-9]{13})-(?P<hash>[0-9a-f]{32})\.json(?:\.bz2)?')

    def __init__(self, path, skip_before=0):
        self._skip_before = datetime.datetime.utcfromtimestamp(skip_before)
        self._path = path.rstrip('/')

    def __iter__(self):
        # auctions-1478015194000-e02305572b12efe069bed00ba1106f77.json.bz2
        for data_filename in self._get_data_files():
            dump_ts, realm_hash = self._parse_fn(data_filename)
            if dump_ts <= self._skip_before:
                logger.debug('Skipping file: %s', data_filename)
                continue
            logger.info('Reading from: %s', data_filename)
            logger.debug('Using timestamp: %s', dump_ts)
            with bz2.BZ2File(data_filename, 'r') as data_handle:
                try:
                    data = json.load(data_handle)
                except Exception as err:
                    logger.exception(err)
                    continue
            logger.debug('Found %06d auctions...', len(data['auctions']))
            yield self._clean_data(data, dump_ts, realm_hash)

    def _clean_data(self, data, ts, realm_hash):
        logger.debug('Pre-processing dump data...')
        realms = {r['name']: r['slug'] for r in data['realms']}
        for a in tqdm(data['auctions'], disable=OPTION_DISABLE_PROGRESS_BAR):
            if a['owner'] == '???':
                a['owner'] = None
            a['ownerRealm'] = realms.get(a['ownerRealm'])
            if a['buyout'] == 0:
                a['buyout'] = None
            a['timeLeft'] = Snapshot.TIME_LEFT_ENUM[a['timeLeft']]
        data['timestamp'] = ts
        data['realm_hash'] = realm_hash
        data['realm_key'] = '+'.join(sorted(realms.values()))
        return data

    def _get_data_files(self):
        return sorted(glob.glob(os.path.join(self._path, 'auctions-*.json.bz2')))

    def _parse_fn(self, fn):
        fn = os.path.basename(fn)
        m = self.RE_DATA_FN.match(fn)
        if m:
            ts, h = m.group('ts'), m.group('hash')
            ts = datetime.datetime.utcfromtimestamp(int(ts) / 1000)
            return (ts, h)
        else:
            raise ValueError('Unable to parse filename: %s' % fn)

class DataManager(object):
    def import_data(self, data_src, batch_size=50, day_buffer=7):
        # aids = Auction IDs
        for data in data_src:
            ts = data['timestamp']
            dt = datetime.timedelta(days=day_buffer)
            try:
                f = ParsedFile.get(ParsedFile.timestamp == ts)
                logger.debug('File has been parsed before: %s', f.timestamp)
                continue
            except DoesNotExist:
                pass
            logger.debug('Finding active auctions...')
            active_aids = set(a.auc_id for a in Auction.select().where(
                Auction.ended_at.is_null(True) &
                Auction.started_at.between(ts - dt, ts)
            ))
            logger.debug('Found active auction IDs: %d', len(active_aids))

            logger.debug('Finding ended auctions...')
            ended_aids = list(active_aids - set(a['auc'] for a in data['auctions']))
            logger.debug('Found ended auction IDs: %d', len(ended_aids))
            with GlobalMeta.database.atomic():
                for i in tqdm(range(0, len(ended_aids), batch_size), disable=OPTION_DISABLE_PROGRESS_BAR):
                    Auction.update(ended_at=ts).where(
                        (Auction.auc_id << ended_aids[i:i+batch_size]) & #probably over a size limit
                        Auction.started_at.between(ts - dt, ts)
                    ).execute()
            del ended_aids

            logger.debug('Finding new auctions...')
            new_auctions = [a for a in data['auctions'] if a['auc'] not in active_aids]
            logger.debug('Found new auctions: %d', len(new_auctions))
            with GlobalMeta.database.atomic():
                for i in tqdm(range(0, len(new_auctions), batch_size), disable=OPTION_DISABLE_PROGRESS_BAR):
                    Auction.insert_many(
                        [Auction.from_json(a, ts) for a in new_auctions[i:i+batch_size]]
                    ).execute()

            logger.debug('Finding new auction snapshots...')
            new_auction_models = {a.auc_id: a for a in Auction.select().where(Auction.started_at == ts)}
            new_auction_objs = {a['auc']: a for a in new_auctions}
            new_aids = new_auction_models.keys()
            del new_auctions
            logger.debug('Inserting new auction snapshots: %d', len(new_auction_models))
            with GlobalMeta.database.atomic():
                for i in tqdm(range(0, len(new_auction_models), batch_size), disable=OPTION_DISABLE_PROGRESS_BAR):
                    Snapshot.insert_many(
                        [Snapshot.from_json(new_auction_models[aid], new_auction_objs[aid], ts) \
                            for aid in new_aids[i:i+batch_size]]
                    ).execute()
            logger.debug('Finding new item attributes...')
            new_item_metas = list(itertools.chain.from_iterable(
                ItemAttribute.from_json(a, new_auction_objs[aid]) \
                for aid, a in new_auction_models.iteritems()
            ))
            logger.debug('Inserting new item attributes: %d', len(new_item_metas))
            with GlobalMeta.database.atomic():
                for i in tqdm(range(0, len(new_item_metas), batch_size), disable=OPTION_DISABLE_PROGRESS_BAR):
                    ItemAttribute.insert_many(new_item_metas[i:i+batch_size]).execute()
            del new_auction_models
            # del new_auction_objs
            del new_item_metas

            logger.debug('Finding old auction snapshots...')
            new_snapshots = [a for a in data['auctions'] if a['auc'] in active_aids]
            new_snapshot_models = {a.auc_id: a for a in Auction.select().where(
                Auction.ended_at.is_null(True) &
                Auction.started_at.between(ts - dt, ts - datetime.timedelta(seconds=1))
            )}
            new_snapshot_objs = {a['auc']: a for a in new_snapshots}
            new_aids = new_snapshot_models.keys()
            del new_snapshots
            logger.debug('Inserting old auction snapshots: %d', len(new_snapshot_models))
            with GlobalMeta.database.atomic():
                for i in tqdm(range(0, len(new_snapshot_models), batch_size), disable=OPTION_DISABLE_PROGRESS_BAR):
                    Snapshot.insert_many(
                        [Snapshot.from_json(new_snapshot_models[aid], new_snapshot_objs[aid], ts) \
                            for aid in new_aids[i:i+batch_size]]
                    ).execute()

            ParsedFile.create(realm_key=data['realm_key'], hash=data['realm_hash'], timestamp=ts)


if __name__ == '__main__':
    import sys
    import optparse

    parser = optparse.OptionParser()

    parser.add_option('-b', '--batch-size', type='int', default=50)
    parser.add_option('--day-buffer', type='int', default=7)
    parser.add_option('-p', '--progress', action='store_true', default=False)
    parser.add_option('-s', '--skip-before', type='int', default=0)

    opts, args = parser.parse_args()
    data_path, db_url = args
    OPTION_DISABLE_PROGRESS_BAR = not opts.progress

    db_connect(db_url)

    ds = DataSource(data_path, opts.skip_before)
    dm = DataManager()
    dm.import_data(ds, batch_size=opts.batch_size, day_buffer=opts.day_buffer)
