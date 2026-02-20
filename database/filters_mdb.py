import logging

import pymongo
from pyrogram import enums

from info import DATABASE_URI, DATABASE_NAME
from database.sqldb import sqldb_enabled, get_conn

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

USE_SQLDB = sqldb_enabled()

if not USE_SQLDB:
    myclient = pymongo.MongoClient(DATABASE_URI)
    mydb = myclient[DATABASE_NAME]
else:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS filters (
                group_id TEXT NOT NULL,
                text TEXT NOT NULL,
                reply TEXT,
                btn TEXT,
                file TEXT,
                alert TEXT,
                PRIMARY KEY (group_id, text)
            )
            """
        )
        conn.commit()


async def add_filter(grp_id, text, reply_text, btn, file, alert):
    if not USE_SQLDB:
        mycol = mydb[str(grp_id)]
        data = {
            'text': str(text),
            'reply': str(reply_text),
            'btn': str(btn),
            'file': str(file),
            'alert': str(alert)
        }
        try:
            mycol.update_one({'text': str(text)}, {"$set": data}, upsert=True)
        except Exception:
            logger.exception('Some error occured!', exc_info=True)
        return

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO filters(group_id, text, reply, btn, file, alert)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id, text) DO UPDATE SET
                reply=excluded.reply,
                btn=excluded.btn,
                file=excluded.file,
                alert=excluded.alert
            """,
            (str(grp_id), str(text), str(reply_text), str(btn), str(file), str(alert)),
        )
        conn.commit()


async def find_filter(group_id, name):
    if not USE_SQLDB:
        mycol = mydb[str(group_id)]
        query = mycol.find({"text": name})
        try:
            for file in query:
                reply_text = file['reply']
                btn = file['btn']
                fileid = file['file']
                try:
                    alert = file['alert']
                except Exception:
                    alert = None
            return reply_text, btn, alert, fileid
        except Exception:
            return None, None, None, None

    with get_conn() as conn:
        row = conn.execute(
            "SELECT reply, btn, alert, file FROM filters WHERE group_id=? AND text=? LIMIT 1",
            (str(group_id), str(name)),
        ).fetchone()
        if not row:
            return None, None, None, None
        return row["reply"], row["btn"], row["alert"], row["file"]


async def get_filters(group_id):
    if not USE_SQLDB:
        mycol = mydb[str(group_id)]
        texts = []
        query = mycol.find()
        try:
            for file in query:
                text = file['text']
                texts.append(text)
        except Exception:
            pass
        return texts

    with get_conn() as conn:
        rows = conn.execute(
            "SELECT text FROM filters WHERE group_id=?",
            (str(group_id),),
        ).fetchall()
        return [row["text"] for row in rows]


async def delete_filter(message, text, group_id):
    if not USE_SQLDB:
        mycol = mydb[str(group_id)]
        myquery = {'text': text}
        query = mycol.count_documents(myquery)
        if query == 1:
            mycol.delete_one(myquery)
            await message.reply_text(
                f"'`{text}`'  deleted. I'll not respond to that filter anymore.",
                quote=True,
                parse_mode=enums.ParseMode.MARKDOWN
            )
        else:
            await message.reply_text("Couldn't find that filter!", quote=True)
        return

    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM filters WHERE group_id=? AND text=?",
            (str(group_id), str(text)),
        )
        conn.commit()

    if cur.rowcount == 1:
        await message.reply_text(
            f"'`{text}`'  deleted. I'll not respond to that filter anymore.",
            quote=True,
            parse_mode=enums.ParseMode.MARKDOWN
        )
    else:
        await message.reply_text("Couldn't find that filter!", quote=True)


async def del_all(message, group_id, title):
    if not USE_SQLDB:
        if str(group_id) not in mydb.list_collection_names():
            await message.edit_text(f"Nothing to remove in {title}!")
            return

        mycol = mydb[str(group_id)]
        try:
            mycol.drop()
            await message.edit_text(f"All filters from {title} has been removed")
        except Exception:
            await message.edit_text("Couldn't remove all filters from group!")
            return
        return

    with get_conn() as conn:
        cur = conn.execute("DELETE FROM filters WHERE group_id=?", (str(group_id),))
        conn.commit()
    if cur.rowcount >= 0:
        await message.edit_text(f"All filters from {title} has been removed")
    else:
        await message.edit_text("Couldn't remove all filters from group!")


async def count_filters(group_id):
    if not USE_SQLDB:
        mycol = mydb[str(group_id)]
        count = mycol.count()
        return False if count == 0 else count

    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as count FROM filters WHERE group_id=?",
            (str(group_id),),
        ).fetchone()
        count = row["count"] if row else 0
        return False if count == 0 else count


async def filter_stats():
    if not USE_SQLDB:
        collections = mydb.list_collection_names()

        if "CONNECTION" in collections:
            collections.remove("CONNECTION")

        totalcount = 0
        for collection in collections:
            mycol = mydb[collection]
            count = mycol.count()
            totalcount += count

        totalcollections = len(collections)

        return totalcollections, totalcount

    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(DISTINCT group_id) as groups, COUNT(*) as total FROM filters"
        ).fetchone()
        return int(row["groups"]), int(row["total"])
