import gzip
import os
import threading
import requests
import pymysql.cursors

import config

TOR_URL = 'http://flibustahezeous3.onion'
BASIC_URL = 'http://flibusta.is'

files = ['lib.libavtor.sql',
         'lib.libbook.sql',
         'lib.libavtorname.sql']


def processing_file(file_: str):
    print(f"Downloading {file_}...")
    try:
        r = requests.get(BASIC_URL + '/sql/' + file_ + '.gz')
    except Exception as e:
        print(e)
        try:
            r = requests.get(TOR_URL + '/sql/' + file_ + '.gz',
                             proxies=config.TOR_PROXIES)
        except Exception as e:
            print(e)
            return
    with open(file_ + '.gz', "wb") as f:
        f.write(r.content)
    if not os.path.exists('../databases/'):
        os.mkdir('../databases/')
    with gzip.open(file_ + '.gz', "rb") as ziped:
        with open('../databases/' + file_, "wb") as f:
            f.write(ziped.read())
    os.remove(file_ + '.gz')


def get_connection(db=False):
    return pymysql.connect(
        host=config.TEMP_DB_HOST,
        user=config.TEMP_DB_USER,
        password=config.TEMP_PASSWORD,
        db=config.TEMP_DB_NAME if db else None,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )


def create_temp_db():
    global_connection = get_connection()
    try:
        print("Creating temp database")
        with global_connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE temp;")

        global_connection.commit()
    finally:
        global_connection.close()


def drop_temp_db():
    global_connection = get_connection()
    try:
        print("Droping temp database")
        with global_connection.cursor() as cursor:
            cursor.execute("DROP DATABASE temp;")

        global_connection.commit()
    finally:
        global_connection.close()


def clean_temp_db():
    connection = get_connection(True)
    try:
        print("Clean data")
        with connection.cursor() as cursor:
            print("Clean books")
            cursor.execute(
                "DELETE FROM temp.libbook WHERE Deleted<>0 OR (Lang<>'ru' AND Lang<>'uk' AND Lang<>'be')"
                "OR (FileType<>'djvu' AND FileType<>'pdf' AND FileType<>'doc' AND FileType<>'fb2'"
                "AND FileType<>'epub' AND FileType<>'mobi');")

            print("Clean book-author")
            cursor.execute(
                "DELETE FROM temp.libavtor WHERE BookId NOT IN (SELECT BookId FROM temp.libbook);"
            )

            print("Clean author")
            cursor.execute(
                "DELETE FROM temp.libavtorname WHERE AvtorId NOT IN (SELECT AvtorId FROM temp.libavtor);"
            )
        connection.commit()
    finally:
        connection.close()


def dump_to_csv():
    def remove_wrong_ch(s: str):
        return s.replace(";", "").replace("\n", " ")

    def dump_books():
        connection = get_connection(True)
        try:
            with connection.cursor() as cursor:
                print("Get books...")
                cursor.execute("SELECT BookId, Title, Lang, FileType FROM temp.libbook;")
                result = cursor.fetchall()
                print("Books has get...")
                with open("books.csv", "w") as f:
                    for r in result:
                        f.write(";".join((str(r["BookId"]), remove_wrong_ch(r["Title"]),
                                          remove_wrong_ch(r["Lang"]), remove_wrong_ch(r["FileType"]))))
                        f.write("\n")
                print("Books dumped...")
        finally:
            connection.close()

    def dump_authors():
        connection = get_connection(True)
        try:
            with connection.cursor() as cursor:
                print("Get authors names...")
                cursor.execute("SELECT AvtorId, FirstName, MiddleName, LastName FROM temp.libavtorname;")
                result = cursor.fetchall()
                print("Authors names has get...")
                with open("authors.csv", "w") as f:
                    for r in result:
                        f.write(";".join((str(r["AvtorId"]), remove_wrong_ch(r["FirstName"]),
                                          remove_wrong_ch(r["MiddleName"]), remove_wrong_ch(r["LastName"]))))
                        f.write("\n")
                print("Authors names dumped...")
        finally:
            connection.close()

    def dump_book_author():
        connection = get_connection(True)
        try:
            with connection.cursor() as cursor:
                print("Get book-author...")
                cursor.execute("SELECT BookId, AvtorId FROM temp.libavtor;")
                print("Book-author has get...")
                result = cursor.fetchall()
                with open("book_author.csv", "w") as f:
                    for r in result:
                        f.write(";".join((str(r["BookId"]), str(r["AvtorId"]))))
                        f.write("\n")
                print("Book-author dumped...")
        finally:
            connection.close()

    threads = [
        threading.Thread(target=dump_books),
        threading.Thread(target=dump_authors()),
        threading.Thread(target=dump_book_author())
    ]

    for th in threads:
        th.start()

    for th in threads:
        th.join()


def update():
    threads = [threading.Thread(target=processing_file, args=(file_,)) for file_ in files]

    for th in threads:
        th.start()

    os.system("sudo /etc/init.d/mysql start")

    try:
        drop_temp_db()
    except pymysql.err.InternalError:
        pass

    create_temp_db()

    for th in threads:
        th.join()

    for file_ in files:
        print(f'Import {file_}')
        os.system(
            f"mysql -u{config.TEMP_DB_USER} -p{config.TEMP_PASSWORD} {config.TEMP_DB_NAME} < ../databases/{file_}"
        )

    clean_temp_db()

    dump_to_csv()

    drop_temp_db()

    os.system("sudo /etc/init.d/mysql stop")


if __name__ == "__main__":
    update()
    requests.get(f"http://localhost:{config.SERVER_PORT}/update")
