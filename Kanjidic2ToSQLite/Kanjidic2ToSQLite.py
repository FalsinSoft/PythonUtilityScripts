
from argparse import ArgumentParser, FileType, Action
import xml.etree.ElementTree as ET
import sqlite3
import sys
import os

dbtable = dict()

def parse_cmdline():
    parser = ArgumentParser()
    parser.add_argument("--kanjidic2file", help="path to the .xml Kanjidic2 file", default="kanjidic2.xml")
    parser.add_argument("--sqlitefile", help="path to the sqlite database to create", required=True)
    parser.add_argument("--dbtableprefix", help="prefix of db tables (ex. kanjidict)", default="kanjidict")
    parser.add_argument("--appendtables", help="append tables into an existing database file")
    return parser.parse_args()


def create_database(name, prefix, append):
    sqlitefile = name
    global dbtable

    if(prefix != ""):
        prefix += "_"

    dbtable["kanji"] = prefix + "kanji"
    dbtable["reading"] = prefix + "reading"
    dbtable["meaning"] = prefix + "meaning"
    dbtable["nanori"] = prefix + "nanori"

    if len(sqlitefile) < 3 or sqlitefile[-3:] != ".db":
        sqlitefile = sqlitefile + ".db"

    if append == False and os.path.exists(sqlitefile):
        os.remove(sqlitefile)

    database = sqlite3.connect(sqlitefile)
    c = database.cursor()

    table_name = dbtable["kanji"]
    c.execute("CREATE TABLE " + table_name + " ("
	    "literal TEXT DEFAULT '',"
	    "codepoint_jis208 TEXT DEFAULT '',"
	    "codepoint_jis212 TEXT DEFAULT '',"
	    "codepoint_jis213 TEXT DEFAULT '',"
	    "codepoint_ucs TEXT DEFAULT '',"
	    "radical_classical TEXT DEFAULT '',"
	    "radical_nelson_c TEXT DEFAULT '',"
	    "grade INTEGER DEFAULT 0,"
	    "stroke_count INTEGER DEFAULT 0,"
	    "variant_jis208 TEXT DEFAULT '',"
	    "variant_jis212 TEXT DEFAULT '',"
	    "variant_jis213 TEXT DEFAULT '',"
	    "variant_deroo TEXT DEFAULT '',"
	    "variant_njecd TEXT DEFAULT '',"
	    "variant_s_h TEXT DEFAULT '',"
	    "variant_nelson_c TEXT DEFAULT '',"
	    "variant_oneill TEXT DEFAULT '',"
	    "variant_ucs TEXT DEFAULT '',"
	    "freq INTEGER DEFAULT 0,"
	    "rad_name TEXT DEFAULT '',"
	    "jlpt INTEGER DEFAULT 0,"
	    "q_code_skip TEXT DEFAULT '',"
        "q_code_skip_misclass TEXT DEFAULT '',"
	    "q_code_sh_desc TEXT DEFAULT '',"
	    "q_code_four_corner TEXT DEFAULT '',"
	    "q_code_deroo TEXT DEFAULT ''"
	    ")")
    c.execute("CREATE INDEX " + table_name + "_literal_index ON " + table_name + " (literal)")

    table_name = dbtable["reading"]
    c.execute("CREATE TABLE " + table_name + " ("
		"kanji_id INTEGER,"
		"type TEXT,"
		"text TEXT"
		")")
    c.execute("CREATE INDEX " + table_name + "_kanji_id_index ON " + table_name + " (kanji_id)")
    c.execute("CREATE INDEX " + table_name + "_text_index ON " + table_name + " (text)")

    table_name = dbtable["meaning"]
    c.execute("CREATE TABLE " + table_name + " ("
		"kanji_id INTEGER,"
		"text TEXT"
		")")
    c.execute("CREATE INDEX " + table_name + "_kanji_id_index ON " + table_name + " (kanji_id)")
    c.execute("CREATE INDEX " + table_name + "_text_index ON " + table_name + " (text)")

    table_name = dbtable["nanori"]
    c.execute("CREATE TABLE " + table_name + " ("
		"kanji_id INTEGER,"
		"text TEXT"
		")")
    c.execute("CREATE INDEX " + table_name + "_kanji_id_index ON " + table_name + " (kanji_id)")
    c.execute("CREATE INDEX " + table_name + "_text_index ON " + table_name + " (text)")

    return database


def parse_codepoint(codepoint, kanji_id, database):
    c = database.cursor()

    for item in codepoint:
        if item.tag == "cp_value":
            code = item.attrib.get("cp_type")
            if code == "jis208":
                c.execute("UPDATE " + dbtable["kanji"] + " SET codepoint_jis208 = ? WHERE rowid = ?", (item.text, kanji_id))
            elif code == "jis212":
                c.execute("UPDATE " + dbtable["kanji"] + " SET codepoint_jis212 = ? WHERE rowid = ?", (item.text, kanji_id))
            elif code == "jis213":
                c.execute("UPDATE " + dbtable["kanji"] + " SET codepoint_jis213 = ? WHERE rowid = ?", (item.text, kanji_id))
            elif code == "ucs":
                c.execute("UPDATE " + dbtable["kanji"] + " SET codepoint_ucs = ? WHERE rowid = ?", (item.text, kanji_id))


def parse_radical(radical, kanji_id, database):
    c = database.cursor()

    for item in radical:
        if item.tag == "rad_value":
            type = item.attrib.get("rad_type")
            if type == "classical":
                c.execute("UPDATE " + dbtable["kanji"] + " SET radical_classical = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "nelson_c":
                c.execute("UPDATE " + dbtable["kanji"] + " SET radical_nelson_c = ? WHERE rowid = ?", (item.text, kanji_id))


def parse_query_code(query_code, kanji_id, database):
    c = database.cursor()

    for item in query_code:
        if item.tag == "query_code":
            type = item.attrib.get("qc_type")
            if type == "skip":
                c.execute("UPDATE " + dbtable["kanji"] + " SET q_code_skip = ?, q_code_skip_misclass = ? WHERE rowid = ?", (item.text, item.attrib.get("skip_misclass", ""), kanji_id))
            elif type == "sh_desc":
                c.execute("UPDATE " + dbtable["kanji"] + " SET q_code_sh_desc = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "four_corner":
                c.execute("UPDATE " + dbtable["kanji"] + " SET q_code_four_corner = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "deroo":
                c.execute("UPDATE " + dbtable["kanji"] + " SET q_code_deroo = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "misclass":
                c.execute("UPDATE " + dbtable["kanji"] + " SET q_code_misclass = ? WHERE rowid = ?", (item.text, kanji_id))


def parse_misc(misc, kanji_id, database):
    c = database.cursor()

    for item in misc:
        if item.tag == "grade":
            c.execute("UPDATE " + dbtable["kanji"] + " SET grade = ? WHERE rowid = ?", (item.text, kanji_id))
        elif item.tag == "stroke_count":
            c.execute("UPDATE " + dbtable["kanji"] + " SET stroke_count = ? WHERE rowid = ?", (item.text, kanji_id))
        elif item.tag == "variant":
            type = item.attrib.get("var_type")
            if type == "jis208":
                c.execute("UPDATE " + dbtable["kanji"] + " SET variant_jis208 = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "jis212":
                c.execute("UPDATE " + dbtable["kanji"] + " SET variant_jis212 = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "jis213":
                c.execute("UPDATE " + dbtable["kanji"] + " SET variant_jis213 = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "deroo":
                c.execute("UPDATE " + dbtable["kanji"] + " SET variant_deroo = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "njecd":
                c.execute("UPDATE " + dbtable["kanji"] + " SET variant_njecd = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "s_h":
                c.execute("UPDATE " + dbtable["kanji"] + " SET variant_s_h = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "nelson_c":
                c.execute("UPDATE " + dbtable["kanji"] + " SET variant_nelson_c = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "oneill":
                c.execute("UPDATE " + dbtable["kanji"] + " SET variant_oneill = ? WHERE rowid = ?", (item.text, kanji_id))
            elif type == "ucs":
                c.execute("UPDATE " + dbtable["kanji"] + " SET variant_ucs = ? WHERE rowid = ?", (item.text, kanji_id))
        elif item.tag == "freq":
            c.execute("UPDATE " + dbtable["kanji"] + " SET freq = ? WHERE rowid = ?", (item.text, kanji_id))
        elif item.tag == "rad_name":
            c.execute("UPDATE " + dbtable["kanji"] + " SET rad_name = ? WHERE rowid = ?", (item.text, kanji_id))
        elif item.tag == "jlpt":
            c.execute("UPDATE " + dbtable["kanji"] + " SET jlpt = ? WHERE rowid = ?", (item.text, kanji_id))
        elif item.tag == "query_code":
            parse_query_code(item, kanji_id, database)


def parse_rmgroup(rmgroup, kanji_id, database):
    c = database.cursor()

    for item in rmgroup:
        if item.tag == "reading":
            type = item.attrib.get("r_type")
            if type == "ja_on" or type == "ja_kun":
                c.execute("INSERT INTO " + dbtable["reading"] + " (kanji_id, type, text) VALUES (?,?,?)", (kanji_id, type, item.text))
        elif item.tag == "meaning":
            if(item.attrib.get("m_lang") == None):
                c.execute("INSERT INTO " + dbtable["meaning"] + " (kanji_id, text) VALUES (?,?)", (kanji_id, item.text))


def parse_reading_meaning(reading_meaning, kanji_id, database):
    c = database.cursor()

    for item in reading_meaning:
        if item.tag == "rmgroup":
            parse_rmgroup(item, kanji_id, database)
        elif item.tag == "nanori":
            c.execute("INSERT INTO " + dbtable["nanori"] + " (kanji_id, text) VALUES (?,?)", (kanji_id, item.text))


def parse_character(character, database):
    c = database.cursor()
    c.execute("INSERT INTO " + dbtable["kanji"] + " DEFAULT VALUES")
    kanji_id = c.lastrowid

    for item in character:
        if item.tag == "literal":
            c.execute("UPDATE " + dbtable["kanji"] + " SET literal = ? WHERE rowid = ?", (item.text, kanji_id))
        elif item.tag == "codepoint":
            parse_codepoint(item, kanji_id, database)
        elif item.tag == "radical":
            parse_radical(item, kanji_id, database)
        elif item.tag == "misc":
            parse_misc(item, kanji_id, database)
        elif item.tag == "reading_meaning":
            parse_reading_meaning(item, kanji_id, database)


def parse_kanjidic2(file, database):
    tree = ET.parse(file)
    root = tree.getroot()
    counter = 0

    if root.tag != "kanjidic2":
        print("Invalid kanjidic2 file")
        return

    for item in root:
        if item.tag == "character":
            parse_character(item, database)
            counter += 1
            if not counter % 100:
                print("#", end="", flush=True)

    database.commit()


def main():
    args = parse_cmdline()
    appendtables = False

    if args.appendtables is not None:
        appendtables = True

    print("Create database...", end="\n", flush=True)
    database = create_database(args.sqlitefile.strip(), args.dbtableprefix.strip(), appendtables)

    print("Start importing Kanjidic2 data:", end="", flush=True)
    parse_kanjidic2(args.kanjidic2file, database)

    database.close()

    
if __name__ == '__main__':
    main()