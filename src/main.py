import io
import json
import zipfile

import aiohttp
import asyncio

import transliterate as transliterate
from aioify import aioify

from aiohttp import web
from aiohttp.web_response import json_response
from peewee import fn, Expression, OP
from peewee_async import Manager

from db import *


def clean():
    BookAuthor.drop_table(True)
    Author.drop_table(True)
    Book.drop_table(True)


Author.create_table(True)
Book.create_table(True)
BookAuthor.create_table(True)

database.close()

mg = Manager(database)


def unzip(file_bytes, file_type):
    try:
        zip_file = zipfile.ZipFile(io.BytesIO(file_bytes))
    except zipfile.BadZipFile as err:
        print(err)
        return
    for name in zip_file.namelist():  # type: str
        if file_type in name.lower():
            return zip_file.read(name)


unzip = aioify(unzip)


async def download(book_id: int, file_type: str, use_proxy=False):
    if use_proxy:
        url = "http://flibustahezeous3.onion/b/"
        proxy = config.TOR_PROXIES
    else:
        url = "http://flibusta.is/b/"
        proxy = None
    if file_type in ("fb2", "epub", "mobi"):
        url += f"{book_id}/{file_type}"
    else:
        url += f"{book_id}/download"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, allow_redirects=True, proxy=proxy) as resp:  # type:  aiohttp.ClientResponse
            if resp.headers.get("Content-Type") and "text/html" in resp.headers.get("Content-Type"):
                if use_proxy:
                    return None
                else:
                    return await download(book_id, file_type, use_proxy=True)
            if resp.headers.get("Content-Type") == "application/zip":
                return await unzip(await resp.read(), file_type)
            return await resp.content.read()


async def get_filename(book: Book, file_type: str) -> str:
    filename = '_'.join([a.short for a in book.authors]) + '_-_' if book.authors else ''
    filename += book.title if book.title[-1] != ' ' else book.title[:-1]
    filename = transliterate.translit(filename, 'ru', reversed=True)

    for c in "(),….’!\"?»«':":
        filename = filename.replace(c, '')

    for c, r in (('—', '-'), ('/', '_'), ('№', 'N'), (' ', '_'), ('–', '-'), ('á', 'a'), (' ', '_')):
        filename = filename.replace(c, r)

    return filename + '.' + file_type


class BookHandler:
    @staticmethod
    async def by_id(request: web.Request):
        id_ = request.match_info.get("id", None)
        if id_ is None:
            raise web.HTTPBadRequest
        try:
            book = await mg.get(Book, id=id_)
            return json_response(data=book.dict)
        except peewee.DoesNotExist:
            raise web.HTTPNoContent

    @staticmethod
    async def search(request: web.Request):
        query = request.match_info.get("query", None)
        if query is None:
            raise web.HTTPBadRequest
        langs_query = (Book.lang == "ru")
        allowed_langs = request.match_info.get("allowed_langs", None)
        if allowed_langs is not None:
            for lang in json.loads(allowed_langs):
                langs_query |= (Book.lang == lang)
        books = [x for x in await mg.execute(Book.select().where(
            Expression(Book.search_content, OP.TS_MATCH, fn.plainto_tsquery(("russian", query))) & langs_query
            ).order_by(fn.ts_rank(Book.search_content, fn.plainto_tsquery(("russian", query)))).desc())]
        return json_response(data=[b.dict for b in books])

    @staticmethod
    async def by_author(request: web.Request):
        id_ = request.match_info.get("id", None)
        if id_ is None:
            raise web.HTTPBadRequest
        langs = ["ru"]
        allowed_langs = request.match_info.get("allowed_langs", None)
        if allowed_langs is not None:
            langs.extend(json.loads(allowed_langs))
        try:
            author = await mg.get(Author, id=id_)
            books = sorted([b.book for b in author.bookauthor_set if b.book.lang in langs], 
                key=lambda book: book.title.replace('«', '').replace('»', '').replace('"', ''))
            return json_response(data=[b.dict for b in books])
        except peewee.DoesNotExist:
            raise web.HTTPNoContent

    @staticmethod
    async def download(request: web.Request):
        book_id = request.match_info.get("id", None)
        file_type = request.match_info.get("type", None)
        if book_id is None:
            raise web.HTTPBadRequest
        try:
            book = await mg.get(Book, id=book_id)
            book.download_count += 1
            mg.update(book)

            if file_type is not None:
                book_bytes = await download(book_id, file_type)
                if not book_bytes:
                    raise web.HTTPNoContent
                response = web.Response(body=book_bytes)
                filename = await get_filename(book, file_type)
                response.headers.add("Content-Disposition",
                                     f"attachment; filename={filename}")
                return response
            else:
                return web.Response()
        except peewee.DoesNotExist:
            raise web.HTTPNoContent


class AuthorHandler:
    @staticmethod
    async def by_id(request: web.Request):
        id_ = request.match_info.get("id", None)
        if id_ is None:
            raise web.HTTPBadRequest
        try:
            return json_response(data=(await mg.get(Author, id=id_)).dict)
        except peewee.DoesNotExist:
            raise web.HTTPNoContent

    @staticmethod
    async def search(request: web.Request):
        query = request.match_info.get("query", None)
        if query is None:
            raise web.HTTPBadRequest
        authors = [x for x in await mg.execute(Author.select().where(
            Expression(Author.search_content, OP.TS_MATCH, fn.plainto_tsquery(query,))).order_by(
            fn.ts_rank(Author.search_content, fn.plainto_tsquery(("russian", query))).desc()))]
        return json_response(data=[a.dict for a in authors])


async def update(request: web.Request):
    async def processing_authors():
        async def update_authors(to_update: set, authors_d: dict, old_authors_d: dict):
            for author in to_update:
                a: Author = old_authors_d[author]
                if a.first_name == authors_d[author][1] and a.middle_name == authors_d[author][2] and a.last_name == \
                        authors_d[author][3]:
                    continue
                _, a.first_name, a.middle_name, a.last_name = authors_d[author]
                a.search_content = fn.to_tsvector(" ".join((a.first_name, a.middle_name, a.last_name)))
                await mg.update(a)

        async def create_authors(to_create: set, authors_d: dict):
            for author in to_create:
                a = authors_d[author]
                await mg.create(Author, id=a[0], first_name=a[1], middle_name=a[2], last_name=a[3],
                                search_content=fn.to_tsvector(" ".join((a[1], a[2], a[3]))))

        async def delete_authors(to_delete: set):
            for author in to_delete:
                await mg.delete(await mg.execute(Author.get(id=author)))

        with open("authors.csv", "r", encoding="utf8") as f:
            authors = [author.replace("\n", "").split(";") for author in f.readlines()]
        old_authors = [a for a in await mg.execute(Author.select(Author.id))]

        authors_dict = dict()
        for author in authors:
            authors_dict.update({int(author[0]): author})

        old_authors_dict = dict()
        for author in old_authors:
            old_authors_dict.update({author.id: author})

        new_authors_ids = set(authors_dict.keys())
        old_authors_ids = {a.id for a in old_authors}

        del authors
        del old_authors

        update_ids = new_authors_ids & old_authors_ids
        create_ids = new_authors_ids - old_authors_ids
        delete_ids = old_authors_ids - new_authors_ids

        del new_authors_ids
        del old_authors_ids

        await update_authors(update_ids, authors_dict, old_authors_dict),
        await create_authors(create_ids, authors_dict),
        await delete_authors(delete_ids)

    async def processing_books():
        async def update_books(to_update: set, books_d: dict, old_books_d: dict):
            for book in to_update:
                b: Book = old_books_d[book]
                if b.title == books_d[book][1] and b.lang == books_d[book][2] and b.file_type == books_d[book][3]:
                    continue
                _, b.title, b.lang, b.file_type = books_d[book]
                b.search_content = fn.to_tsvector("russian", b.title)
                b.download_count = 0
                await mg.update(b)

        async def create_books(to_create: set, books_d: dict):
            for book in to_create:
                b = books_d[book]
                await mg.create(Book, id=b[0], title=b[1], lang=b[2], file_type=b[3], download_count=0,
                                search_content=fn.to_tsvector("russian", b[1]))

        async def delete_books(to_delete: set):
            for book in to_delete:
                await mg.delete(await mg.execute(Book.get(id=book)))

        with open("books.csv", "r", encoding="utf8") as f:
            books = [book.replace("\n", "").split(";") for book in f.readlines()]
        old_books = [b for b in await mg.execute(Book.select(Book.id))]

        books_dict = dict()
        for book in books:
            books_dict.update({int(book[0]): book})

        old_books_dict = dict()
        for book in old_books:
            old_books_dict.update({book.id: book})

        new_books_ids = set(books_dict.keys())
        old_books_ids = {b.id for b in old_books}

        del books
        del old_books

        update_ids = new_books_ids & old_books_ids
        create_ids = new_books_ids - old_books_ids
        delete_ids = old_books_ids - new_books_ids

        del new_books_ids
        del old_books_ids

        await update_books(update_ids, books_dict, old_books_dict)
        await create_books(create_ids, books_dict)
        await delete_books(delete_ids)

    async def update_book_author():
        with open("book_author.csv", "r", encoding="utf8") as f:
            book_author = [tuple(map(int, ba.replace("\n", "").split(";"))) for ba in f.readlines()]

        book_author_dict = dict()
        for ba in book_author:
            if book_author_dict.get(ba[0]):
                book_author_dict[ba[0]].append(ba[1])
            else:
                book_author_dict.update({ba[0]: [ba[1]]})

        for key in book_author_dict.keys():
            b: Book = await mg.get(Book, id=key)
            bas = [x.author_id for x in
                   await mg.execute(BookAuthor.select(BookAuthor.author).where(BookAuthor.book == b))]
            for ba in bas:
                if ba not in book_author_dict[key]:
                    a: Author = await mg.get(Author, id=ba)
                    await mg.delete(await mg.get(BookAuthor, book=b, author=a))
            for t_ba in book_author_dict[key]:
                if t_ba not in bas:
                    a: Author = await mg.get(Author, id=t_ba)
                    await mg.create(BookAuthor, book=b, author=a)

    await processing_authors()
    await processing_books()
    await update_book_author()

    return web.json_response(text="ok")


if __name__ == "__main__":
    import platform

    if platform.system() == "Linux":
        try:
            import uvloop

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            print("Use uvloop!")
        except ImportError:
            print("Install uvloop for best speed!")

    app = web.Application()

    app.add_routes((
        web.get("/book/{id}", BookHandler.by_id),
        web.get("/book/search/{query}", BookHandler.search),
        web.get("/book/search/{query}/{allowed_langs}", BookHandler.search),
        web.get("/book/author/{id}", BookHandler.by_author),
        web.get("/book/author/{id}/{allowed_langs}", BookHandler.by_author),
        web.get("/book/download/{id}", BookHandler.download),
        web.get("/book/download/{id}/{type}", BookHandler.download),
        web.get("/author/{id}", AuthorHandler.by_id),
        web.get("/author/search/{query}", AuthorHandler.search),
        web.get("/update", update)
    ))

    web.run_app(app, host=config.SERVER_HOST, port=config.SERVER_PORT)
