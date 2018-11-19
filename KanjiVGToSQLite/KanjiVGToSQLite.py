
from argparse import ArgumentParser, FileType, Action
import xml.etree.ElementTree as ET
import sqlite3
import sys
import os

# For store the groups tree into database the nested set model
# method has been used. Check this post for details:
# https://falsinsoft.blogspot.com/2013/01/tree-in-sql-database-nested-set-model.html

kvg = "{http://kanjivg.tagaini.net}"

position_id_list = {
		"left" : 0,
		"right" : 1,
		"top" : 2,
		"bottom" : 3,
		"nyo" : 4,
		"tare" : 5,
		"kamae" : 6,
		"kamae1" : 7,
		"kamae2" : 8
	}
radical_id_list = {
		"general" : 0,
		"nelson" : 1,
		"tradit" : 2
	}

def parse_cmdline():
    parser = ArgumentParser()
    parser.add_argument("--kanjivgfile", help="path to the .xml KanjiVG file", default="kanjivg.xml")
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

    c.execute("CREATE TABLE kanji ("
        "character TEXT"
        ")")

    c.execute("CREATE TABLE groups ("
        "kanji_id INTEGER,"
        "lft INTEGER,"
        "rgt INTEGER,"
        "sequence INTEGER,"
        "element TEXT DEFAULT NULL,"
        "original TEXT DEFAULT NULL,"
        "position INTEGER DEFAULT NULL,"
        "variant INTEGER DEFAULT NULL,"
        "partial INTEGER DEFAULT NULL,"
        "number INTEGER DEFAULT NULL,"
        "radical INTEGER DEFAULT NULL,"
        "phon TEXT DEFAULT NULL,"
        "tradForm INTEGER DEFAULT NULL,"
        "radicalForm INTEGER DEFAULT NULL"
        ")")

    c.execute("CREATE TABLE strokes ("
        "group_id INTEGER,"
        "sequence INTEGER,"
        "type TEXT,"
        "path TEXT"
        ")")

    c.execute("CREATE INDEX kanji_index ON kanji (character)")
    c.execute("CREATE INDEX groups_index ON groups (kanji_id)")
    c.execute("CREATE INDEX tree_index ON groups (lft,rgt)")
    c.execute("CREATE INDEX strokes_index ON strokes (group_id)")

    return database


def parse_path(path, group_id, database):
    c = database.cursor()

    id = path.attrib.get("id")
    type = path.attrib.get(kvg+"type")
    d = path.attrib.get("d")

    sequence = int(id[11:])

    c.execute("INSERT INTO strokes (group_id, sequence, type, path) VALUES (?,?,?,?)", [group_id, sequence, type, d])


def parse_group(group, kanji_id, parent_lft, database):
    c = database.cursor()

    id = group.attrib.get("id")
    element = group.attrib.get(kvg+"element")
    original = group.attrib.get(kvg+"original")
    position = group.attrib.get(kvg+"position")
    variant = group.attrib.get(kvg+"variant")
    partial = group.attrib.get(kvg+"partial")
    number = group.attrib.get(kvg+"number")
    radical = group.attrib.get(kvg+"radical")
    phon = group.attrib.get(kvg+"phon")
    tradForm = group.attrib.get(kvg+"tradForm")
    radicalForm = group.attrib.get(kvg+"radicalForm")

    sequence = 0
    if id.find("-g") != -1:
        sequence = int(id[11:])

    if parent_lft > 0:
        c.execute("UPDATE groups SET lft = lft + 2 WHERE kanji_id = ? AND lft > ?", [kanji_id, parent_lft])
        c.execute("UPDATE groups SET rgt = rgt + 2 WHERE kanji_id = ? AND rgt > ?", [kanji_id, parent_lft])
   
    lft = parent_lft + 1
    rgt = parent_lft + 2

    query = "INSERT INTO groups (kanji_id, lft, rgt, sequence"
    values = [kanji_id, lft, rgt, sequence]

    if element != None:
        values.append(element)
        query += ", element"
    if original != None:
        values.append(original)
        query += ", original"
    if position != None:
        values.append(position_id_list[position])
        query += ", position"
    if variant != None:
        if variant == "true":
            values.append(1)
        else:
            values.append(0)
        query += ", variant"
    if partial != None:
        if partial == "true":
            values.append(1)
        else:
            values.append(0)
        query += ", partial"
    if number != None:
        values.append(number)
        query += ", number"
    if radical != None:
        values.append(radical_id_list[radical])
        query += ", radical"
    if phon != None:
        values.append(phon)
        query += ", phon"
    if tradForm != None:
        if tradForm == "true":
            values.append(1)
        else:
            values.append(0)
        query += ", tradForm"
    if radicalForm != None:
        if radicalForm == "true":
            values.append(1)
        else:
            values.append(0)
        query += ", radicalForm"

    query += ") VALUES (?,?,?,?"
    for i in range(0, len(values)-4):
        query += ",?"
    query += ")"
    
    c.execute(query, values)
    group_id = c.lastrowid

    for item in group:
        if item.tag == "g":
            parse_group(item, kanji_id, lft, database)
        elif item.tag == "path":
            parse_path(item, group_id, database)


def parse_kanji(kanji, database):
    kanji_code = kanji.attrib.get("id")
    
    if kanji_code == None or len(kanji_code) != 15 or kanji_code[0:10] != "kvg:kanji_":
        print("Invalid 'id' attribute")
        return

    kanji_code = int(kanji_code[10:15], 16)

    if kanji_code >= 0x4E00 and kanji_code <= 0x9FBF: # Only kanji are stored into database
        c = database.cursor()
        c.execute("INSERT INTO kanji (character) VALUES (?)", chr(kanji_code))
        kanji_id = c.lastrowid
    
        if len(kanji) != 1 or kanji[0].tag != "g":
            print("Invalid kanji format")
            return

        parse_group(kanji[0], kanji_id, 0, database)


def parse_kanjisv(file, database):
    tree = ET.parse(file)
    root = tree.getroot()
    counter = 0

    if root.tag != "kanjivg":
        print("Invalid kanjivg file")
        return

    for item in root:
        if item.tag == "kanji":
            parse_kanji(item, database)
            counter += 1
            if not counter % 100:
                print("#", end="", flush=True)

    database.commit()


def main():
    args = parse_cmdline()

    print("Create database...", end="\n", flush=True)
    database = create_database(args.sqlitefile.strip())

    print("Start importing KanjiVG data:", end="", flush=True)
    parse_kanjisv(args.kanjivgfile, database)

    database.close()

    
if __name__ == '__main__':
    main()