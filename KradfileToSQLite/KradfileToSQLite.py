
from argparse import ArgumentParser, FileType, Action
import sqlite3
import sys
import codecs
import os

def parse_cmdline():
    parser = ArgumentParser()
    parser.add_argument("--kradfile", help="path to the kradfile", default="kradfile")
    parser.add_argument("--kradfile2", help="path to the kradfile2", default="kradfile2")
    parser.add_argument("--sqlitefile", help="path to the sqlite database to create", required=True)
    return parser.parse_args()


def create_database(name):
    sqlitefile = name

    if len(sqlitefile) < 3 or sqlitefile[-3:] != ".db":
        sqlitefile = sqlitefile + ".db"

    if os.path.exists(sqlitefile):
        os.remove(sqlitefile)

    database = sqlite3.connect(sqlitefile)

    c = database.cursor()
    c.execute("CREATE TABLE radicals (data TEXT NOT NULL)")
    c.execute("CREATE TABLE kanji (data TEXT NOT NULL)")
    c.execute("CREATE TABLE kanji_radical (kanji_id INTEGER, radical_id INTEGER)")

    return database


def get_radical_id(radical, database):
    c = database.cursor()
    radical_id = 0

    c.execute("SELECT rowid FROM radicals WHERE data = ?", [radical])
    row = c.fetchone()

    if row is None:
        c.execute("INSERT INTO radicals (data) VALUES (?)", [radical])
        radical_id = c.lastrowid
    else:
        radical_id = row[0]

    return radical_id


def import_data_file(database, kradfile):
    c = database.cursor()
    counter = 0

    for line in kradfile:
        line = line.strip()

        if line[0] == "#" or line[0] == " ":
            continue

        try:
            kanji,radicals = line.split(":")
        except:
            continue

        c.execute("INSERT INTO kanji (data) VALUES (?)", [line[0]])
        kanji_id = c.lastrowid
        
        for radical in radicals.split():
            radical_id = get_radical_id(radical.strip(), database)
            c.execute("INSERT INTO kanji_radical (kanji_id, radical_id) VALUES (?, ?)", (kanji_id, radical_id))

        counter += 1
        if not counter % 100:
            print("#", end = "", flush = True)

    database.commit()


def main():
    args = parse_cmdline()

	# Example query for get the list of radicas of specific kanji
    # select data from radicals, kanji_radical where radicals.rowid = kanji_radical.radical_id and kanji_radical.kanji_id = (select rowid from kanji where data = '鰯')
	# Example query for get the list kanji having specific radical
    # select data from kanji, kanji_radical where kanji.rowid = kanji_radical.kanji_id and kanji_radical.radical_id = (select rowid from radicals where data = '田')

    kradfile = codecs.open(args.kradfile, 'r', 'euc-jp')
    kradfile2 = codecs.open(args.kradfile2, 'r', 'euc-jp')
    database = create_database(args.sqlitefile.strip())

    print("Start conversion: ", end = "", flush = True)

    import_data_file(database, kradfile)
    import_data_file(database, kradfile2)

    print("\nConversion finished")

    kradfile.close()
    kradfile2.close()
    database.close()

    
if __name__ == '__main__':
    main()