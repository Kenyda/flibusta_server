from typing import List, Optional, Type
import pathlib
from abc import ABC

import asyncpg
from aiohttp import web

from config import Config as config


async def preapare_db(app: web.Application):
    pool = await asyncpg.create_pool(user=config.DB_USER, password=config.DB_PASSWORD,
                                     database=config.DB_NAME, host=config.DB_HOST)

    for _class in [AuthorAnnotations, AuthorsBD, BookAnnotations, BooksDB, 
                   Sequence, SequenceName, TablesCreator]:  # type: Type[ConfigurableDB]
        _class.configurate(pool)

    await TablesCreator.create_tables()


SQL_FOLDER = pathlib.Path("./sql")


class ConfigurableDB(ABC):
    pool: asyncpg.pool.Pool

    @classmethod
    def configurate(cls, pool: asyncpg.pool.Pool):
        cls.pool = pool


class TablesCreator(ConfigurableDB):
    CREATE_BOOK_TABLE = open(SQL_FOLDER / "create_book_table.sql").read().format(config.DB_USER)
    CREATE_AUTHOR_TABLE = open(SQL_FOLDER / "create_author_table.sql").read().format(config.DB_USER)
    CREATE_BOOK_AUTHOR_TABLE = open(SQL_FOLDER / "create_book_author_table.sql").read().format(config.DB_USER)
    CREATE_SEQUENCE_NAME_TABLE = open(SQL_FOLDER / "create_sequence_name_table.sql").read().format(config.DB_USER)
    CREATE_SEQUENCE_TABLE = open(SQL_FOLDER / "create_sequence_table.sql").read().format(config.DB_USER)
    CREATE_BOOK_ANNOTATION_TABLE = open(SQL_FOLDER / "create_book_annotation_table.sql").read().format(config.DB_USER)
    CREATE_AUTHOR_ANNOTATION_TABLE = open(SQL_FOLDER / "create_author_annotation_table.sql").read().format(config.DB_USER)

    @classmethod
    async def create_tables(cls):
        await cls.pool.execute(cls.CREATE_BOOK_TABLE)
        await cls.pool.execute(cls.CREATE_AUTHOR_TABLE)
        await cls.pool.execute(cls.CREATE_BOOK_AUTHOR_TABLE)
        await cls.pool.execute(cls.CREATE_SEQUENCE_NAME_TABLE)
        await cls.pool.execute(cls.CREATE_SEQUENCE_TABLE)
        await cls.pool.execute(cls.CREATE_BOOK_ANNOTATION_TABLE)
        await cls.pool.execute(cls.CREATE_AUTHOR_ANNOTATION_TABLE)


class BooksDB(ConfigurableDB):
    BOOK_BY_ID = open(SQL_FOLDER / "book_by_id.sql").read()
    BOOK_SEARCH_COUNT = open(SQL_FOLDER / "book_search_count.sql").read()
    BOOK_SEARCH = open(SQL_FOLDER / "book_search.sql").read()
    BOOK_RANDOM = open(SQL_FOLDER / "book_random.sql").read()

    @classmethod
    async def by_id(cls, book_id: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            result = await conn.fetch(cls.BOOK_BY_ID, book_id)
            return result[0]["json_build_object"] if result else None

    @classmethod
    async def search(cls, query: str, allowed_langs: List[str], limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(cls.BOOK_SEARCH_COUNT, allowed_langs, query))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(cls.BOOK_SEARCH,
                                       allowed_langs, query, limit, limit * (page - 1)))[0]["array_to_json"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def random(cls, allowed_langs: List[str]):
        async with cls.pool.acquire() as conn:
            result = await conn.fetch(cls.BOOK_RANDOM, allowed_langs)
            return result[0]["json_build_object"] if result else None


class AuthorsBD(ConfigurableDB):
    AUTHOR_BY_ID = open(SQL_FOLDER / "author_by_id.sql").read()
    AUTHOR_BY_ID_COUNT = open(SQL_FOLDER / "author_by_id_count.sql").read()
    AUTHOR_SEARCH_COUNT = open(SQL_FOLDER / "author_search_count.sql").read()
    AUTHOR_SEARCH = open(SQL_FOLDER / "author_search.sql").read()
    AUTHOR_RANDOM_ID = open(SQL_FOLDER / "author_random_id.sql").read()
    AUTHOR_INFO_BY_ID = open(SQL_FOLDER / "author_info_by_id.sql").read()

    @classmethod
    async def by_id(cls, author_id: int, allowed_langs: List[str], limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(cls.AUTHOR_BY_ID_COUNT, author_id, allowed_langs))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(cls.AUTHOR_BY_ID, allowed_langs, author_id,
                                       limit, limit * (page - 1)))[0]["json_build_object"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def search(cls, query: str, allowed_langs: List[str], limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(cls.AUTHOR_SEARCH_COUNT, query, allowed_langs))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(cls.AUTHOR_SEARCH, query, allowed_langs,
                                       limit, limit * (page - 1)))[0]["array_to_json"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def random(cls, allowed_langs: List[str]):
        async with cls.pool.acquire() as conn:
            author_id = (await conn.fetch(cls.AUTHOR_RANDOM_ID, allowed_langs))[0]["id"]
            return (await conn.fetch(cls.AUTHOR_INFO_BY_ID, author_id))[0]["json_build_object"]


class SequenceName(ConfigurableDB):
    SEQUENCENAME_BY_ID_COUNT = open(SQL_FOLDER / "sequencename_by_id_count.sql").read()
    SEQUENCENAME_BY_ID = open(SQL_FOLDER / "sequencename_by_id.sql").read()
    SEQUENCENAME_SEARCH_COUNT = open(SQL_FOLDER / "sequencename_search_count.sql").read()
    SEQUENCENAME_SEARCH = open(SQL_FOLDER / "sequencename_search.sql").read()
    SEQUENCENAME_RANDOM = open(SQL_FOLDER / "sequencename_random.sql").read()

    @classmethod
    async def by_id(cls, allowed_langs, seq_id: int, limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(cls.SEQUENCENAME_BY_ID_COUNT, seq_id, allowed_langs))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(cls.SEQUENCENAME_BY_ID, allowed_langs, seq_id,
                                       limit, limit * (page - 1)))[0]["json_build_object"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def search(cls, allowed_langs, query: str, limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(cls.SEQUENCENAME_SEARCH_COUNT, query, allowed_langs))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(cls.SEQUENCENAME_SEARCH, query, allowed_langs,
                                       limit, limit * (page - 1)))[0]["array_to_json"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def random(cls, allowed_langs: List[str]):
        async with cls.pool.acquire() as conn:
            return (await conn.fetch(cls.SEQUENCENAME_RANDOM, allowed_langs))[0]["json_build_object"]


class Sequence(ConfigurableDB):
    SEQUENCE_BY_BOOK_ID = open(SQL_FOLDER / "sequence_by_book_id.sql").read()

    @classmethod
    async def by_book_id(cls, book_id: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            result = (await conn.fetch(cls.SEQUENCE_BY_BOOK_ID, book_id))[0]
            return result["array_agg"] if result["array_agg"] else []


class BookAnnotations(ConfigurableDB):
    BOOK_ANNOTATION_BY_ID = open(SQL_FOLDER / "book_annotation_by_id.sql").read()

    @classmethod
    async def by_id(cls, book_id: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            result = (await conn.fetch(cls.BOOK_ANNOTATION_BY_ID, book_id))
            return result[0]["json_build_object"] if result else None


class AuthorAnnotations(ConfigurableDB):
    AUTHOR_ANNOTATION_BY_ID = open(SQL_FOLDER / "author_annotation_by_id.sql").read()

    @classmethod
    async def by_id(cls, author_id: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            result = (await conn.fetch(cls.AUTHOR_ANNOTATION_BY_ID, author_id))
            return result[0]["json_build_object"] if result else None
