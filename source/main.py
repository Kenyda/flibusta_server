import io
import ujson as json
import zipfile

import aiohttp
from aiohttp import ClientTimeout
import asyncio
import concurrent.futures

import transliterate as transliterate
from aioify import aioify

from aiohttp.web_response import json_response

from db import *
from exceptions import *


process_pool_executor = concurrent.futures.ProcessPoolExecutor(10)


def unzip(file_bytes, file_type):
    zip_file = zipfile.ZipFile(io.BytesIO(file_bytes))
    for name in zip_file.namelist():  # type: str
        if file_type in name.lower():
            return zip_file.read(name)
    raise FileNotFoundError


async def download(book_id: int, file_type: str, type_:int=3, retry:int=3):
    # type 0: flibustahezeous3.onion
    # type 1: flibusta.appspot.com
    # type 2: flibusta.is
    # type 3: flibs.in
    while type_ > 0:
        print(f"download {book_id} {file_type} {type_} {retry}")
        url: str = ""
        cookies: Optional[dict] = None
        proxy: Optional[str] = None

        if type_ in (0, 1, 2):
            basic_url = None
            if type_ == 0:
                basic_url = "http://flibustahezeous3.onion"
            elif type_ == 1:
                basic_url = "https://flibusta.appspot.com"
            elif type_ == 2:
                basic_url = "http://flibusta.is"
            else:
                raise Exception()

            if type_ == 0:
                proxy="http://185.93.108.227:8118"

            if file_type in ("fb2", "epub", "mobi"):
                url = basic_url + f"/b/{book_id}/{file_type}"
            else:
                url = basic_url + f"/b/{book_id}/download"

            if type_ in [1]:
                cookies = {'SESS717db4750c98b34dc0a0cf14a0c49e88': 'dfd17c8195cecd84a6fc02392729bfc5'}

            # if type_ in [1, 2] and file_type in ("fb2", "epub", "mobi"):
            #     setup_format_url = basic_url + f"/AJAX.php?op=setuseropt&o=D&v={file_type}"

        elif type_ == 3:
            url = f"https://flibs.in/d?b={book_id}&f={file_type}"

        print(url)

        try:
            # if setup_format_url:
            #     print("start setup")
            #     async with aiohttp.ClientSession(cookies=cookies, timeout=ClientTimeout(total=60 * 60)) as session:
            #         async with session.get(setup_format_url, allow_redirects=True, max_redirects=50, proxy=proxy) as resp:  # type: aiohttp.ClientResponse
            #             print(f"setup {resp.status}")

            async with aiohttp.ClientSession(cookies=cookies, timeout=ClientTimeout(total=60 * 60)) as session:
                async with session.get(url, allow_redirects=True, max_redirects=50, proxy=proxy) as resp:  # type: aiohttp.ClientResponse
                    if resp.headers.get("Content-Type") and "text/html" in resp.headers.get("Content-Type") or resp.status != 200:
                        if "Мы зарегистрировали подозрительный трафик, исходящий из вашей сети." in await resp.text():
                            raise CaptchaException("Captcha")
                        raise NotBookException("NotBookException")
                    if resp.headers.get("Content-Type") == "application/zip":
                        return await asyncio.get_event_loop().run_in_executor(
                            process_pool_executor, unzip, await resp.read(), file_type)
                    return await resp.content.read()
        except (aiohttp.ServerDisconnectedError, aiohttp.ClientOSError, aiohttp.ClientPayloadError,
                aiohttp.client_exceptions.ClientConnectorError, zipfile.BadZipFile,
                CaptchaException, NotBookException, FileNotFoundError) as e:
            print(e)

        retry -= 1

        if retry <= 0:
            type_ -= 1
            retry = 3

    return None


async def download_image(type_: str, path: str):
    proxy: Optional[str] = None
    pfix: Optional[str] = None
    if type_ == "book":
        pfix = "ib"
    elif type_ == "author":
        pfix = "ia"
    url = f"https://flibusta.is/{pfix}/{path}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, allow_redirects=True, proxy=proxy, max_redirects=50) as resp:
                if resp.status != 200:
                    return None
                return await resp.content.read()
    except aiohttp.client_exceptions.ServerDisconnectedError:
        return None


async def get_filename(book, file_type: str) -> str:
    filename = '_'.join([await get_short_name(a) for a in book["authors"]]) + '_-_' if book["authors"] else ''
    filename += book["title"] if book["title"][-1] != ' ' else book["title"][:-1]
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
        response = await BooksDB.by_id(int(id_))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)

    @staticmethod
    async def search(request: web.Request):
        query = request.match_info.get("query", None)
        allowed_langs = request.match_info.get("allowed_langs", None)
        limit = request.match_info.get("limit", None)
        page = request.match_info.get("page", None)
        if None in [query, allowed_langs, limit, page]:
            raise web.HTTPBadRequest
        response = await BooksDB.search(query, json.loads(allowed_langs),
                                        int(limit), int(page))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)

    @staticmethod
    async def random(request: web.Request):
        allowed_langs = request.match_info.get("allowed_langs", None)
        if not allowed_langs:
            raise web.HTTPBadRequest
        response = await BooksDB.random(json.loads(allowed_langs))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)

    @staticmethod
    async def download(request: web.Request):
        # ToDO: try download from telegram if book exist in channel
        book_id = request.match_info.get("id", None)
        file_type = request.match_info.get("type", None)
        if book_id is None and file_type is None:
            raise web.HTTPBadRequest

        book = json.loads(await BooksDB.by_id(int(book_id)))

        if not book:
            raise web.HTTPNoContent

        book_bytes = await download(book_id, file_type)
        if not book_bytes:
            raise web.HTTPNoContent
        response = web.Response(body=book_bytes)
        filename = await get_filename(book, file_type)
        response.headers.add("Content-Disposition",
                             f"attachment; filename={filename}")
        return response


class AuthorHandler:
    @staticmethod
    async def by_id(request: web.Request):
        id_ = request.match_info.get("id", None)
        allowed_langs = request.match_info.get("allowed_langs", None)
        limit = request.match_info.get("limit", None)
        page = request.match_info.get("page", None)
        if None in [id_, allowed_langs, limit, page]:
            raise web.HTTPBadRequest
        response = await AuthorsBD.by_id(int(id_), json.loads(allowed_langs), int(limit), int(page))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)

    @staticmethod
    async def search(request: web.Request):
        query = request.match_info.get("query", None)
        allowed_langs = request.match_info.get("allowed_langs", None)
        limit = request.match_info.get("limit", None)
        page = request.match_info.get("page", None)
        if None in [query, allowed_langs, limit, page]:
            raise web.HTTPBadRequest
        response = await AuthorsBD.search(query, json.loads(allowed_langs),
                                          int(limit), int(page))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)

    @staticmethod
    async def random(request: web.Request):
        allowed_langs = request.match_info.get("allowed_langs", None)
        if allowed_langs is None:
            raise web.HTTPBadRequest
        response = await AuthorsBD.random(json.loads(allowed_langs))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)


class SequenceHandler:
    @staticmethod
    async def by_id(request: web.Request):
        id_ = request.match_info.get("id", None)
        allowed_langs = request.match_info.get("allowed_langs", None)
        limit = request.match_info.get("limit", None)
        page = request.match_info.get("page", None)
        if None in [id_, allowed_langs, limit, page]:
            raise web.HTTPBadRequest
        response = await SequenceName.by_id(json.loads(allowed_langs), int(id_), int(limit), int(page))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)

    @staticmethod
    async def search(request: web.Request):
        query = request.match_info.get("query", None)
        allowed_langs = request.match_info.get("allowed_langs", None)
        limit = request.match_info.get("limit", None)
        page = request.match_info.get("page", None)
        if None in [query, allowed_langs, limit, page]:
            raise web.HTTPBadRequest
        response = await SequenceName.search(json.loads(allowed_langs), query, int(limit), int(page))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)

    @staticmethod
    async def random(request: web.Request):
        allowed_langs = request.match_info.get("allowed_langs", None)
        if allowed_langs is None:
            raise web.HTTPBadRequest
        response = await SequenceName.random(json.loads(allowed_langs))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)


class BookAnnotationHandler:
    @staticmethod
    async def by_id(request: web.Request):
        id_ = request.match_info.get("id", None)
        if id_ is None:
            raise web.HTTPBadRequest
        response = await BookAnnotations.by_id(int(id_))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)

    @staticmethod
    async def image(request: web.Request):
        id_ = request.match_info.get("id", None)
        if id_ is None:
            raise web.HTTBadRequest
        response = await BookAnnotations.by_id(int(id_))
        if not response:
            raise web.HTTPNoContent
        path = json.loads(response)['file']
        if path == None:
            raise web.HTTPNoContent
        image = await download_image("book", path)
        if not image:
            raise web.HTTPNoContent
        result = web.Response(body=image)
        result.headers.add("Content-Type", "image/png")
        return result


class AuthorAnnotationHandler:
    @staticmethod
    async def by_id(request: web.Request):
        id_ = request.match_info.get("id", None)
        if id_ is None:
            raise web.HTTPBadRequest
        response = await AuthorAnnotations.by_id(int(id_))
        if not response:
            raise web.HTTPNoContent
        return json_response(body=response)

    @staticmethod
    async def image(request: web.Request):
        id_ = request.match_info.get("id", None)
        if id_ is None:
            raise web.HTTBadRequest
        response = await AuthorAnnotations.by_id(int(id_))
        if not response:
            raise web.HTTPNoContent
        path = json.loads(response)['file']
        if path == None:
            raise web.HTTPNoContent
        image = await download_image("author", path)
        if not image:
            raise web.HTTPNoContent
        result = web.Response(body=image)
        result.headers.add("Content-Type", "image/png")
        return result


if __name__ == "__main__":
    import platform

    if platform.system() == "Linux":
        try:
            # noinspection PyUnresolvedReferences
            import uvloop

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            print("Use uvloop!")
        except ImportError:
            print("Install uvloop for best speed!")

    app = web.Application()
    # update.app = app

    app.on_startup.append(preapare_db)

    app.add_routes((
        web.get("/book/{id}", BookHandler.by_id),
        web.get("/book/search/{allowed_langs}/{limit}/{page}/{query}", BookHandler.search),
        web.get("/book/download/{id}/{type}", BookHandler.download),
        web.get("/book/random/{allowed_langs}", BookHandler.random),
        web.get("/author/search/{allowed_langs}/{limit}/{page}/{query}", AuthorHandler.search),
        web.get("/author/{id}/{allowed_langs}/{limit}/{page}", AuthorHandler.by_id),
        web.get("/author/random/{allowed_langs}", AuthorHandler.random),
        web.get("/sequence/{id}/{allowed_langs}/{limit}/{page}", SequenceHandler.by_id),
        web.get("/sequence/search/{allowed_langs}/{limit}/{page}/{query}", SequenceHandler.search),
        web.get("/sequence/random/{allowed_langs}", SequenceHandler.random),
        web.get("/annotation/book/{id}", BookAnnotationHandler.by_id),
        web.get("/annotation/book/image/{id}", BookAnnotationHandler.image),
        web.get("/annotation/author/{id}", AuthorAnnotationHandler.by_id),
        web.get("/annotation/author/image/{id}", AuthorAnnotationHandler.image)
    ))

    web.run_app(app, host=config.SERVER_HOST, port=config.SERVER_PORT)
