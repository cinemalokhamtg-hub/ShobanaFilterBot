# https://github.com/odysseusmax/animated-lamp/blob/master/bot/database/database.py
#  @MrMNTG @MusammilN
#please give credits https://github.com/MN-BOTS/ShobanaFilterBot
import motor.motor_asyncio
from info import DATABASE_NAME, DATABASE_URI, IMDB, IMDB_TEMPLATE, MELCOW_NEW_USERS, P_TTI_SHOW_OFF, SINGLE_BUTTON, SPELL_CHECK_REPLY, PROTECT_CONTENT
from database.sqldb import sqldb_enabled, get_conn

USE_SQLDB = sqldb_enabled()


class _AsyncRows:
    def __init__(self, rows):
        self._rows = rows

    def __aiter__(self):
        self._iter = iter(self._rows)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class Database:
    def __init__(self, uri, database_name):
        self.use_sql = USE_SQLDB
        if not self.use_sql:
            self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
            self.db = self._client[database_name]
            self.col = self.db.users
            self.grp = self.db.groups
            self.config = self.db.config
        else:
            with get_conn() as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, is_banned INTEGER DEFAULT 0, ban_reason TEXT DEFAULT '')")
                conn.execute("CREATE TABLE IF NOT EXISTS groups (id INTEGER PRIMARY KEY, title TEXT, is_disabled INTEGER DEFAULT 0, reason TEXT DEFAULT '', settings TEXT)")
                conn.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
                conn.commit()

    def new_user(self, id, name):
        return dict(
            id=id,
            name=name,
            ban_status=dict(
                is_banned=False,
                ban_reason="",
            ),
        )

    def new_group(self, id, title):
        return dict(
            id=id,
            title=title,
            chat_status=dict(
                is_disabled=False,
                reason="",
            ),
        )

    async def add_user(self, id, name):
        if not self.use_sql:
            user = self.new_user(id, name)
            await self.col.insert_one(user)
            return
        with get_conn() as conn:
            conn.execute("INSERT OR IGNORE INTO users(id, name, is_banned, ban_reason) VALUES (?, ?, 0, '')", (int(id), str(name)))
            conn.commit()

    async def is_user_exist(self, id):
        if not self.use_sql:
            user = await self.col.find_one({'id': int(id)})
            return bool(user)
        with get_conn() as conn:
            row = conn.execute("SELECT 1 FROM users WHERE id=?", (int(id),)).fetchone()
            return bool(row)

    async def total_users_count(self):
        if not self.use_sql:
            return await self.col.count_documents({})
        with get_conn() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM users").fetchone()[0])

    async def remove_ban(self, id):
        if not self.use_sql:
            ban_status = dict(is_banned=False, ban_reason='')
            await self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})
            return
        with get_conn() as conn:
            conn.execute("UPDATE users SET is_banned=0, ban_reason='' WHERE id=?", (int(id),))
            conn.commit()

    async def ban_user(self, user_id, ban_reason="No Reason"):
        if not self.use_sql:
            ban_status = dict(is_banned=True, ban_reason=ban_reason)
            await self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})
            return
        with get_conn() as conn:
            conn.execute("UPDATE users SET is_banned=1, ban_reason=? WHERE id=?", (str(ban_reason), int(user_id)))
            conn.commit()

    async def get_ban_status(self, id):
        default = dict(is_banned=False, ban_reason='')
        if not self.use_sql:
            user = await self.col.find_one({'id': int(id)})
            if not user:
                return default
            return user.get('ban_status', default)
        with get_conn() as conn:
            row = conn.execute("SELECT is_banned, ban_reason FROM users WHERE id=?", (int(id),)).fetchone()
            if not row:
                return default
            return {'is_banned': bool(row['is_banned']), 'ban_reason': row['ban_reason'] or ''}

    async def get_all_users(self):
        if not self.use_sql:
            return self.col.find({})
        with get_conn() as conn:
            rows = conn.execute("SELECT id FROM users").fetchall()
            return _AsyncRows([{'id': row['id']} for row in rows])

    async def delete_user(self, user_id):
        if not self.use_sql:
            await self.col.delete_many({'id': int(user_id)})
            return
        with get_conn() as conn:
            conn.execute("DELETE FROM users WHERE id=?", (int(user_id),))
            conn.commit()

    async def get_banned(self):
        if not self.use_sql:
            users = self.col.find({'ban_status.is_banned': True})
            chats = self.grp.find({'chat_status.is_disabled': True})
            b_chats = [chat['id'] async for chat in chats]
            b_users = [user['id'] async for user in users]
            return b_users, b_chats
        with get_conn() as conn:
            b_users = [row['id'] for row in conn.execute("SELECT id FROM users WHERE is_banned=1").fetchall()]
            b_chats = [row['id'] for row in conn.execute("SELECT id FROM groups WHERE is_disabled=1").fetchall()]
            return b_users, b_chats

    async def add_chat(self, chat, title):
        if not self.use_sql:
            chat = self.new_group(chat, title)
            await self.grp.insert_one(chat)
            return
        with get_conn() as conn:
            conn.execute("INSERT OR IGNORE INTO groups(id, title, is_disabled, reason, settings) VALUES (?, ?, 0, '', NULL)", (int(chat), str(title)))
            conn.commit()

    async def get_chat(self, chat):
        if not self.use_sql:
            chat = await self.grp.find_one({'id': int(chat)})
            return False if not chat else chat.get('chat_status')
        with get_conn() as conn:
            row = conn.execute("SELECT is_disabled, reason FROM groups WHERE id=?", (int(chat),)).fetchone()
            if not row:
                return False
            return {'is_disabled': bool(row['is_disabled']), 'reason': row['reason'] or ''}

    async def re_enable_chat(self, id):
        if not self.use_sql:
            chat_status = dict(is_disabled=False, reason="")
            await self.grp.update_one({'id': int(id)}, {'$set': {'chat_status': chat_status}})
            return
        with get_conn() as conn:
            conn.execute("UPDATE groups SET is_disabled=0, reason='' WHERE id=?", (int(id),))
            conn.commit()

    async def update_settings(self, id, settings):
        if not self.use_sql:
            await self.grp.update_one({'id': int(id)}, {'$set': {'settings': settings}})
            return
        import json
        with get_conn() as conn:
            conn.execute("UPDATE groups SET settings=? WHERE id=?", (json.dumps(settings), int(id)))
            conn.commit()

    async def get_settings(self, id):
        default = {
            'button': SINGLE_BUTTON,
            'botpm': P_TTI_SHOW_OFF,
            'file_secure': PROTECT_CONTENT,
            'imdb': IMDB,
            'spell_check': SPELL_CHECK_REPLY,
            'welcome': MELCOW_NEW_USERS,
            'template': IMDB_TEMPLATE
        }
        if not self.use_sql:
            chat = await self.grp.find_one({'id': int(id)})
            if chat:
                return chat.get('settings', default)
            return default
        import json
        with get_conn() as conn:
            row = conn.execute("SELECT settings FROM groups WHERE id=?", (int(id),)).fetchone()
            if not row or not row['settings']:
                return default
            try:
                return json.loads(row['settings'])
            except Exception:
                return default

    async def disable_chat(self, chat, reason="No Reason"):
        if not self.use_sql:
            chat_status = dict(is_disabled=True, reason=reason)
            await self.grp.update_one({'id': int(chat)}, {'$set': {'chat_status': chat_status}})
            return
        with get_conn() as conn:
            conn.execute("UPDATE groups SET is_disabled=1, reason=? WHERE id=?", (str(reason), int(chat)))
            conn.commit()

    async def total_chat_count(self):
        if not self.use_sql:
            return await self.grp.count_documents({})
        with get_conn() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0])

    async def get_all_chats(self):
        if not self.use_sql:
            return self.grp.find({})
        with get_conn() as conn:
            rows = conn.execute("SELECT id FROM groups").fetchall()
            return _AsyncRows([{'id': row['id']} for row in rows])

    async def set_auth_channels(self, channels: list[int]):
        if not self.use_sql:
            await self.config.update_one({"_id": "auth_channels"}, {"$set": {"channels": channels}}, upsert=True)
            return
        import json
        with get_conn() as conn:
            conn.execute("INSERT INTO config(key, value) VALUES('auth_channels', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(channels),))
            conn.commit()

    async def get_auth_channels(self) -> list[int]:
        if not self.use_sql:
            doc = await self.config.find_one({"_id": "auth_channels"})
            if doc and "channels" in doc:
                return doc["channels"]
            return []
        import json
        with get_conn() as conn:
            row = conn.execute("SELECT value FROM config WHERE key='auth_channels'").fetchone()
            if not row:
                return []
            try:
                return json.loads(row['value'])
            except Exception:
                return []

    async def get_db_size(self):
        if not self.use_sql:
            return (await self.db.command("dbstats"))['dataSize']
        with get_conn() as conn:
            row = conn.execute("PRAGMA page_count").fetchone()[0]
            page = conn.execute("PRAGMA page_size").fetchone()[0]
            return int(row) * int(page)


db = Database(DATABASE_URI, DATABASE_NAME)

#  @MrMNTG @MusammilN
#please give credits https://github.com/MN-BOTS/ShobanaFilterBot
