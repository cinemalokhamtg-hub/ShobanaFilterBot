import logging

import pymongo

from info import DATABASE_URI, DATABASE_NAME
from database.sqldb import sqldb_enabled, get_conn

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

USE_SQLDB = sqldb_enabled()

if not USE_SQLDB:
    myclient = pymongo.MongoClient(DATABASE_URI)
    mydb = myclient[DATABASE_NAME]
    mycol = mydb['CONNECTION']
else:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS connections (
                user_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(user_id, group_id)
            )
            """
        )
        conn.commit()


async def add_connection(group_id, user_id):
    if not USE_SQLDB:
        query = mycol.find_one(
            {"_id": user_id},
            {"_id": 0, "active_group": 0}
        )
        if query is not None:
            group_ids = [x["group_id"] for x in query["group_details"]]
            if group_id in group_ids:
                return False

        group_details = {
            "group_id": group_id
        }

        data = {
            '_id': user_id,
            'group_details': [group_details],
            'active_group': group_id,
        }

        if mycol.count_documents({"_id": user_id}) == 0:
            try:
                mycol.insert_one(data)
                return True
            except Exception:
                logger.exception('Some error occurred!', exc_info=True)
        else:
            try:
                mycol.update_one(
                    {'_id': user_id},
                    {
                        "$push": {"group_details": group_details},
                        "$set": {"active_group": group_id}
                    }
                )
                return True
            except Exception:
                logger.exception('Some error occurred!', exc_info=True)
        return False

    try:
        with get_conn() as conn:
            exists = conn.execute(
                "SELECT 1 FROM connections WHERE user_id=? AND group_id=?",
                (str(user_id), str(group_id)),
            ).fetchone()
            if exists:
                return False

            conn.execute("UPDATE connections SET is_active=0 WHERE user_id=?", (str(user_id),))
            conn.execute(
                "INSERT INTO connections(user_id, group_id, is_active) VALUES (?, ?, 1)",
                (str(user_id), str(group_id)),
            )
            conn.commit()
            return True
    except Exception as e:
        logger.exception(f'Some error occurred! {e}', exc_info=True)
        return False


async def active_connection(user_id):
    if not USE_SQLDB:
        query = mycol.find_one(
            {"_id": user_id},
            {"_id": 0, "group_details": 0}
        )
        if not query:
            return None
        group_id = query['active_group']
        return int(group_id) if group_id is not None else None

    with get_conn() as conn:
        row = conn.execute(
            "SELECT group_id FROM connections WHERE user_id=? AND is_active=1 LIMIT 1",
            (str(user_id),),
        ).fetchone()
        if not row:
            return None
        try:
            return int(row["group_id"])
        except Exception:
            return row["group_id"]


async def all_connections(user_id):
    if not USE_SQLDB:
        query = mycol.find_one(
            {"_id": user_id},
            {"_id": 0, "active_group": 0}
        )
        if query is not None:
            return [x["group_id"] for x in query["group_details"]]
        return None

    with get_conn() as conn:
        rows = conn.execute(
            "SELECT group_id FROM connections WHERE user_id=?",
            (str(user_id),),
        ).fetchall()
        if not rows:
            return None
        return [row["group_id"] for row in rows]


async def if_active(user_id, group_id):
    if not USE_SQLDB:
        query = mycol.find_one(
            {"_id": user_id},
            {"_id": 0, "group_details": 0}
        )
        return query is not None and query['active_group'] == group_id

    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM connections WHERE user_id=? AND group_id=? AND is_active=1",
            (str(user_id), str(group_id)),
        ).fetchone()
        return row is not None


async def make_active(user_id, group_id):
    if not USE_SQLDB:
        update = mycol.update_one(
            {'_id': user_id},
            {"$set": {"active_group": group_id}}
        )
        return update.modified_count != 0

    with get_conn() as conn:
        conn.execute("UPDATE connections SET is_active=0 WHERE user_id=?", (str(user_id),))
        cur = conn.execute(
            "UPDATE connections SET is_active=1 WHERE user_id=? AND group_id=?",
            (str(user_id), str(group_id)),
        )
        conn.commit()
        return cur.rowcount != 0


async def make_inactive(user_id):
    if not USE_SQLDB:
        update = mycol.update_one(
            {'_id': user_id},
            {"$set": {"active_group": None}}
        )
        return update.modified_count != 0

    with get_conn() as conn:
        cur = conn.execute("UPDATE connections SET is_active=0 WHERE user_id=?", (str(user_id),))
        conn.commit()
        return cur.rowcount != 0


async def delete_connection(user_id, group_id):
    if not USE_SQLDB:
        try:
            update = mycol.update_one(
                {"_id": user_id},
                {"$pull": {"group_details": {"group_id": group_id}}}
            )
            if update.modified_count == 0:
                return False
            query = mycol.find_one({"_id": user_id}, {"_id": 0})
            if len(query["group_details"]) >= 1:
                if query['active_group'] == group_id:
                    prvs_group_id = query["group_details"][len(query["group_details"]) - 1]["group_id"]
                    mycol.update_one(
                        {'_id': user_id},
                        {"$set": {"active_group": prvs_group_id}}
                    )
            else:
                mycol.update_one(
                    {'_id': user_id},
                    {"$set": {"active_group": None}}
                )
            return True
        except Exception as e:
            logger.exception(f'Some error occurred! {e}', exc_info=True)
            return False

    try:
        with get_conn() as conn:
            active = conn.execute(
                "SELECT is_active FROM connections WHERE user_id=? AND group_id=?",
                (str(user_id), str(group_id)),
            ).fetchone()
            cur = conn.execute(
                "DELETE FROM connections WHERE user_id=? AND group_id=?",
                (str(user_id), str(group_id)),
            )
            if cur.rowcount == 0:
                conn.commit()
                return False

            if active and active["is_active"] == 1:
                last = conn.execute(
                    "SELECT group_id FROM connections WHERE user_id=? ORDER BY rowid DESC LIMIT 1",
                    (str(user_id),),
                ).fetchone()
                if last:
                    conn.execute(
                        "UPDATE connections SET is_active=1 WHERE user_id=? AND group_id=?",
                        (str(user_id), last["group_id"]),
                    )
            conn.commit()
            return True
    except Exception as e:
        logger.exception(f'Some error occurred! {e}', exc_info=True)
        return False
