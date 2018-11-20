from typing import List

import peewee
import peewee_asyncext

from playhouse.postgres_ext import TSVectorField

import config

database = peewee_asyncext.PooledPostgresqlExtDatabase(config.DB_NAME, host=config.DB_HOST,
                                                       user=config.DB_USER, password=config.DB_PASSWORD,
                                                       register_hstore=False)


class FlibustaModel(peewee.Model):
    class Meta:
        database = database


class Book(FlibustaModel):
    id = peewee.IntegerField(primary_key=True)
    title = peewee.CharField()
    lang = peewee.CharField(2)
    file_type = peewee.CharField(4)
    download_count = peewee.BigIntegerField()
    search_content = TSVectorField()

    @property
    def authors(self) -> List["Author"]:
        return [x.author for x in self.bookauthor_set]

    @property
    def dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "lang": self.lang,
            "file_type": self.file_type,
            "authors": [a.dict for a in self.authors]
        }


class Author(FlibustaModel):
    id = peewee.IntegerField(primary_key=True)
    first_name = peewee.CharField()
    last_name = peewee.CharField()
    middle_name = peewee.CharField()
    search_content = TSVectorField()

    @property
    def dict(self):
        return {
            "id": self.id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "middle_name": self.middle_name
        }

    @property
    def short(self) -> str:
        temp = ''
        if self.last_name:
            temp += self.last_name
        if self.first_name:
            if temp:
                temp += " "
            temp += self.first_name[0]
        if self.middle_name:
            if temp:
                temp += " "
            temp += self.middle_name[0]
        return temp


class BookAuthor(FlibustaModel):
    book = peewee.ForeignKeyField(Book)
    author = peewee.ForeignKeyField(Author)
