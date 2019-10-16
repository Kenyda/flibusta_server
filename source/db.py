from typing import List, Optional, Type
import pathlib
from abc import abstractclassmethod, ABC

import asyncpg
from aiohttp import web
from asyncpg import Record

from config import Config as config


async def preapare_db(app: web.Application):
    pool = await asyncpg.create_pool(user=config.DB_USER, password=config.DB_PASSWORD,
                                     database=config.DB_NAME, host=config.DB_HOST)

    await create_tables(pool)

    for _class in [AuthorAnnotations, AuthorsBD, BookAnnotations, BooksDB, Sequence, SequenceName]:  # type: Type[ConfigurableDB]
        _class.configurate(pool)


SQL_FOLDER = pathlib.Path("./sql")


class Requests:
    create_book_table = open(SQL_FOLDER / "create_book_table.sql").read().format(config.DB_USER)
    create_author_table = open(SQL_FOLDER / "create_author_table.sql").read().format(config.DB_USER)
    create_book_author_table = open(SQL_FOLDER / "create_book_author_table.sql").read().format(config.DB_USER)
    create_sequence_name_table = open(SQL_FOLDER / "create_sequence_name_table.sql").read().format(config.DB_USER)
    create_sequence_table = open(SQL_FOLDER / "create_sequence_table.sql").read().format(config.DB_USER)
    create_book_annotation_table = open(SQL_FOLDER / "create_book_annotation_table.sql").read().format(config.DB_USER)
    create_author_annotation_table = open(SQL_FOLDER / "create_author_annotation_table.sql").read().format(config.DB_USER)

    update_indexes = open(SQL_FOLDER / "update_indexes.sql").read()

    book_by_id = open(SQL_FOLDER / "book_by_id.sql").read()
    book_search_count = open(SQL_FOLDER / "book_search_count.sql").read()
    book_search = open(SQL_FOLDER / "book_search.sql").read()
    book_random = open(SQL_FOLDER / "book_random.sql").read()

    author_by_id = open(SQL_FOLDER / "author_by_id.sql").read()
    author_by_id_count = open(SQL_FOLDER / "author_by_id_count.sql").read()
    author_search_count = open(SQL_FOLDER / "author_search_count.sql").read()
    author_search = open(SQL_FOLDER / "author_search.sql").read()
    author_random_id = open(SQL_FOLDER / "author_random_id.sql").read()
    author_info_by_id = open(SQL_FOLDER / "author_info_by_id.sql").read()

    sequencename_by_id_count = open(SQL_FOLDER / "sequencename_by_id_count.sql").read()
    sequencename_by_id = open(SQL_FOLDER / "sequencename_by_id.sql").read()
    sequencename_search_count = open(SQL_FOLDER / "sequencename_search_count.sql").read()
    sequencename_search = open(SQL_FOLDER / "sequencename_search.sql").read()
    sequencename_random = open(SQL_FOLDER / "sequencename_random.sql").read()

    sequence_by_book_id = open(SQL_FOLDER / "sequence_by_book_id.sql").read()

    book_annotations_by_id = open(SQL_FOLDER / "book_annotation_by_id.sql").read()

    author_annotations_by_id = open(SQL_FOLDER / "author_annotation_by_id.sql").read()


async def create_book_table(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:  # type: asyncpg.Connection
        await conn.execute(Requests.create_book_table)


async def create_author_table(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:  # type: asyncpg.Connection
        await conn.execute(Requests.create_author_table)


async def create_book_author_table(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:  # type: asyncpg.Connection
        await conn.execute(Requests.create_book_author_table)


async def create_sequence_name_table(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:  # type: asyncpg.Connection
        await conn.execute(Requests.create_sequence_name_table)


async def create_sequence_table(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:  # type: asyncpg.Connection
        await conn.execute(Requests.create_sequence_table)


async def create_book_annotation_table(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:  # type: asyncpg.Connection
        await conn.execute(Requests.create_book_annotation_table)


async def create_author_annotation_table(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:  # type: asyncpg.Connection
        await conn.execute(Requests.create_author_annotation_table)


async def create_tables(pool: asyncpg.pool.Pool):
    await create_book_table(pool)
    await create_author_table(pool)
    await create_book_author_table(pool)
    await create_sequence_name_table(pool)
    await create_sequence_table(pool)
    await create_book_annotation_table(pool)
    await create_author_annotation_table(pool)


async def update_indexes(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:  # type: asyncpg.Connection
        await conn.execute(Requests.update_indexes)


async def get_short_name(author) -> str:
    temp = ''
    if author['last_name']:
        temp += author['last_name']
    if author['first_name']:
        if temp:
            temp += " "
        temp += author['first_name'][0]
    if author['middle_name']:
        if temp:
            temp += " "
        temp += author['middle_name'][0]
    return temp


class ConfigurableDB(ABC):
    pool: asyncpg.pool.Pool

    @classmethod
    def configurate(cls, pool: asyncpg.pool.Pool):
        cls.pool = pool


class BooksDB(ConfigurableDB):
    @classmethod
    async def by_id(cls, book_id: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            result = await conn.fetch(Requests.book_by_id, book_id)
            return result[0]["json_build_object"] if result else None

    @classmethod
    async def search(cls, query: str, allowed_langs: List[str], limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(Requests.book_search_count, allowed_langs, query))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(Requests.book_search,
                                       allowed_langs, query, limit, limit * (page - 1)))[0]["array_to_json"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def random(cls, allowed_langs: List[str]):
        async with cls.pool.acquire() as conn:
            result = await conn.fetch(Requests.book_random, allowed_langs)
            return result[0]["json_build_object"] if result else None


class AuthorsBD(ConfigurableDB):
    @classmethod
    async def by_id(cls, author_id: int, allowed_langs: List[str], limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(Requests.author_by_id_count, author_id, allowed_langs))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(Requests.author_by_id, allowed_langs, author_id,
                                       limit, limit * (page - 1)))[0]["json_build_object"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def search(cls, query: str, allowed_langs: List[str], limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(Requests.author_search_count, query, allowed_langs))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(Requests.author_search, query, allowed_langs,
                                       limit, limit * (page - 1)))[0]["array_to_json"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def random(cls, allowed_langs: List[str]):
        async with cls.pool.acquire() as conn:
            author_id = (await conn.fetch(Requests.author_random_id, allowed_langs))[0]["id"]
            return (await conn.fetch(Requests.author_info_by_id, author_id))[0]["json_build_object"]


class SequenceName(ConfigurableDB):
    @classmethod
    async def by_id(cls, allowed_langs, seq_id: int, limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(Requests.sequencename_by_id_count, seq_id, allowed_langs))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(Requests.sequencename_by_id, allowed_langs, seq_id,
                                       limit, limit * (page - 1)))[0]["json_build_object"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def search(cls, allowed_langs, query: str, limit: int, page: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            count = (await conn.fetch(Requests.sequencename_search_count, query, allowed_langs))[0]["count"]
            if count == 0:
                return None
            result = (await conn.fetch(Requests.sequencename_search, query, allowed_langs,
                                       limit, limit * (page - 1)))[0]["array_to_json"]
            return "{" + f'"result": {result}, "count": {count}' + "}"

    @classmethod
    async def random(cls, allowed_langs: List[str]):
        async with cls.pool.acquire() as conn:
            return (await conn.fetch(Requests.sequencename_random, allowed_langs))[0]["json_build_object"]


class Sequence(ConfigurableDB):
    @classmethod
    async def by_book_id(cls, book_id: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            result = (await conn.fetch(Requests.sequence_by_book_id, book_id))[0]
            return result["array_agg"] if result["array_agg"] else []


class BookAnnotations(ConfigurableDB):
    @classmethod
    async def by_id(cls, book_id: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            result = (await conn.fetch(Requests.book_annotations_by_id, book_id))
            return result[0]["json_build_object"] if result else None


class AuthorAnnotations(ConfigurableDB):
    @classmethod
    async def by_id(cls, author_id: int):
        async with cls.pool.acquire() as conn:  # type: asyncpg.Connection
            result = (await conn.fetch(Requests.author_annotations_by_id, author_id))
            return result[0]["json_build_object"] if result else None
