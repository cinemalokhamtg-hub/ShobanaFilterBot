#  @MrMNTG @MusammilN
#please give credits https://github.com/MN-BOTS/ShobanaFilterBot
import logging
from struct import pack
import re
import base64
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from marshmallow.exceptions import ValidationError

from info import DATABASE_URI, DATABASE_NAME, COLLECTION_NAME, USE_CAPTION_FILTER
from database.sqldb import sqldb_enabled, get_conn

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

USE_SQLDB = sqldb_enabled()

if not USE_SQLDB:
    from umongo import Instance, Document, fields
    from motor.motor_asyncio import AsyncIOMotorClient

    client = AsyncIOMotorClient(DATABASE_URI)
    db = client[DATABASE_NAME]
    instance = Instance.from_db(db)

    @instance.register
    class Media(Document):
        file_id = fields.StrField(attribute='_id')
        file_ref = fields.StrField(allow_none=True)
        file_name = fields.StrField(required=True)
        file_size = fields.IntField(required=True)
        file_type = fields.StrField(allow_none=True)
        mime_type = fields.StrField(allow_none=True)
        caption = fields.StrField(allow_none=True)

        class Meta:
            indexes = ('$file_name', )
            collection_name = COLLECTION_NAME

else:
    class SqlMediaDoc(dict):
        def __getattr__(self, item):
            return self.get(item)

    class SqlDeleteResult:
        def __init__(self, deleted_count: int):
            self.deleted_count = deleted_count

    def _match_filter(row: dict, filter_: dict) -> bool:
        if not filter_:
            return True

        if '$or' in filter_:
            if not any(_match_filter(row, sub) for sub in filter_['$or']):
                return False
            rest = {k: v for k, v in filter_.items() if k != '$or'}
            return _match_filter(row, rest)

        for key, val in filter_.items():
            if key == '_id':
                if isinstance(val, dict) and '$in' in val:
                    if row.get('_id') not in set(val['$in']):
                        return False
                elif row.get('_id') != val:
                    return False
            else:
                row_val = row.get(key)
                if hasattr(val, 'search'):
                    if row_val is None or not val.search(str(row_val)):
                        return False
                else:
                    if row_val != val:
                        return False
        return True

    class SqlCursor:
        def __init__(self, rows, as_docs=False):
            self.rows = rows
            self.as_docs = as_docs
            self._skip = 0
            self._limit = None

        def sort(self, key, order):
            reverse = order == -1
            if key == '$natural':
                self.rows.sort(key=lambda x: x.get('_rowid', 0), reverse=reverse)
            else:
                self.rows.sort(key=lambda x: x.get(key), reverse=reverse)
            return self

        def skip(self, n):
            self._skip = n
            return self

        def limit(self, n):
            self._limit = n
            return self

        async def to_list(self, length=None):
            rows = self.rows[self._skip:]
            lim = self._limit if self._limit is not None else length
            if lim is not None:
                rows = rows[:lim]
            if self.as_docs:
                return [SqlMediaDoc({k: v for k, v in r.items() if k != '_rowid'}) for r in rows]
            return [{k: v for k, v in r.items() if k != '_rowid'} for r in rows]

    class SqlCollection:
        async def delete_one(self, filter_):
            with get_conn() as conn:
                rows = conn.execute("SELECT rowid, file_id FROM media").fetchall()
                deleted = 0
                for r in rows:
                    row = {'_rowid': r['rowid'], '_id': r['file_id']}
                    if _match_filter(row, filter_):
                        conn.execute("DELETE FROM media WHERE rowid=?", (r['rowid'],))
                        deleted = 1
                        break
                conn.commit()
                return SqlDeleteResult(deleted)

        async def delete_many(self, filter_):
            with get_conn() as conn:
                rows = conn.execute("SELECT rowid, file_id, file_name, file_size, mime_type FROM media").fetchall()
                ids = [r['rowid'] for r in rows if _match_filter({'_rowid': r['rowid'], '_id': r['file_id'], 'file_name': r['file_name'], 'file_size': r['file_size'], 'mime_type': r['mime_type']}, filter_)]
                for rid in ids:
                    conn.execute("DELETE FROM media WHERE rowid=?", (rid,))
                conn.commit()
                return SqlDeleteResult(len(ids))

        async def drop(self):
            with get_conn() as conn:
                conn.execute("DROP TABLE IF EXISTS media")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS media (
                        file_id TEXT PRIMARY KEY,
                        file_ref TEXT,
                        file_name TEXT NOT NULL,
                        file_size INTEGER NOT NULL,
                        file_type TEXT,
                        mime_type TEXT,
                        caption TEXT
                    )
                    """
                )
                conn.commit()

        def find(self, filter_, projection=None):
            with get_conn() as conn:
                rows = conn.execute("SELECT rowid, file_id, file_name FROM media").fetchall()
                data = []
                for r in rows:
                    row = {'_rowid': r['rowid'], '_id': r['file_id'], 'file_name': r['file_name']}
                    if _match_filter(row, filter_):
                        if projection and projection.get('_id') == 1:
                            data.append({'_rowid': r['rowid'], '_id': r['file_id']})
                        else:
                            data.append(row)
                return SqlCursor(data, as_docs=False)

    class Media:
        collection = SqlCollection()

        @staticmethod
        async def count_documents(filter_=None):
            with get_conn() as conn:
                rows = conn.execute(
                    "SELECT rowid, file_id, file_name, caption, file_type, mime_type, file_size FROM media"
                ).fetchall()
                count = 0
                for r in rows:
                    row = {
                        '_rowid': r['rowid'],
                        '_id': r['file_id'],
                        'file_name': r['file_name'],
                        'caption': r['caption'],
                        'file_type': r['file_type'],
                        'mime_type': r['mime_type'],
                        'file_size': r['file_size'],
                    }
                    if _match_filter(row, filter_ or {}):
                        count += 1
                return count

        @staticmethod
        def find(filter_=None):
            with get_conn() as conn:
                rows = conn.execute(
                    "SELECT rowid, file_id, file_ref, file_name, file_size, file_type, mime_type, caption FROM media"
                ).fetchall()
                data = []
                for r in rows:
                    row = {
                        '_rowid': r['rowid'],
                        '_id': r['file_id'],
                        'file_id': r['file_id'],
                        'file_ref': r['file_ref'],
                        'file_name': r['file_name'],
                        'file_size': r['file_size'],
                        'file_type': r['file_type'],
                        'mime_type': r['mime_type'],
                        'caption': r['caption'],
                    }
                    if _match_filter(row, filter_ or {}):
                        data.append(row)
                return SqlCursor(data, as_docs=True)

    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media (
                file_id TEXT PRIMARY KEY,
                file_ref TEXT,
                file_name TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                file_type TEXT,
                mime_type TEXT,
                caption TEXT
            )
            """
        )
        conn.commit()


async def save_file(media):
    """Save file in database"""

    # TODO: Find better way to get same file_id for same media to avoid duplicates
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"(_|\-|\.|\+)", " ", str(media.file_name))

    if USE_SQLDB:
        try:
            with get_conn() as conn:
                conn.execute(
                    "INSERT INTO media(file_id, file_ref, file_name, file_size, file_type, mime_type, caption) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        file_id,
                        file_ref,
                        file_name,
                        media.file_size,
                        media.file_type,
                        media.mime_type,
                        media.caption.html if media.caption else None,
                    ),
                )
                conn.commit()
            logger.info(f'{getattr(media, "file_name", "NO_FILE")} is saved to database')
            return True, 1
        except Exception as e:
            if 'UNIQUE constraint failed' in str(e):
                logger.warning(f'{getattr(media, "file_name", "NO_FILE")} is already saved in database')
                return False, 0
            logger.exception('Error occurred while saving file in database')
            return False, 2

    try:
        file = Media(
            file_id=file_id,
            file_ref=file_ref,
            file_name=file_name,
            file_size=media.file_size,
            file_type=media.file_type,
            mime_type=media.mime_type,
            caption=media.caption.html if media.caption else None,
        )
    except ValidationError:
        logger.exception('Error occurred while saving file in database')
        return False, 2
    else:
        try:
            await file.commit()
        except DuplicateKeyError:
            logger.warning(
                f'{getattr(media, "file_name", "NO_FILE")} is already saved in database'
            )

            return False, 0
        else:
            logger.info(f'{getattr(media, "file_name", "NO_FILE")} is saved to database')
            return True, 1


async def get_search_results(query, file_type=None, max_results=10, offset=0, filter=False):
    """For given query return (results, next_offset)"""

    query = query.strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]')

    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except Exception:
        return []

    if USE_CAPTION_FILTER:
        filter = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        filter = {'file_name': regex}

    if file_type:
        filter['file_type'] = file_type

    total_results = await Media.count_documents(filter)
    next_offset = offset + max_results

    if next_offset > total_results:
        next_offset = ''

    cursor = Media.find(filter)
    cursor.sort('$natural', -1)
    cursor.skip(offset).limit(max_results)
    files = await cursor.to_list(length=max_results)

    return files, next_offset, total_results


async def get_file_details(query):
    filter = {'file_id': query} if USE_SQLDB else {'file_id': query}
    cursor = Media.find(filter)
    filedetails = await cursor.to_list(length=1)
    return filedetails


def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0

    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0

            r += bytes([i])

    return base64.urlsafe_b64encode(r).decode().rstrip("=")


def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")


def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref

from collections import defaultdict


async def get_movie_list(limit=20):
    cursor = Media.find().sort("$natural", -1).limit(100)
    files = await cursor.to_list(length=100)
    results = []

    for file in files:
        name = getattr(file, "file_name", "")
        if not re.search(r"(s\d{1,2}|season\s*\d+).*?(e\d{1,2}|episode\s*\d+)", name, re.I):
            results.append(name)
        if len(results) >= limit:
            break
    return results


async def get_series_grouped(limit=30):
    cursor = Media.find().sort("$natural", -1).limit(150)
    files = await cursor.to_list(length=150)
    grouped = defaultdict(list)

    for file in files:
        name = getattr(file, "file_name", "")
        match = re.search(r"(.*?)(?:S\d{1,2}|Season\s*\d+).*?(?:E|Ep|Episode)?(\d{1,2})", name, re.I)
        if match:
            title = match.group(1).strip().title()
            episode = int(match.group(2))
            grouped[title].append(episode)

    return {
        title: sorted(set(eps))[:10]
        for title, eps in grouped.items() if eps
    }
