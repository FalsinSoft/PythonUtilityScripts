
from argparse import ArgumentParser, FileType, Action
import xml.etree.ElementTree as ET
import sqlite3
import sys
import os
import codecs

# Comment xml elements you don't want to be imported into database (please
# note, if you comment main elements like, for example, "k_ele", "r_ele" or 
# "sense" all the child elements will not be imported as well)
xml_elements = [
    "k_ele",
    "keb",
    "ke_inf",
    "ke_pri",
    "r_ele",
    "reb",
    "re_nokanji",
    "re_restr",
    "re_inf",
    "re_pri",
    "sense",
    "stagk",
    "stagr",
    "pos",
    "xref",
    "ant",
    "field",
    "misc",
    "s_inf",
    "dial",
    "gloss"
    ]

dbtable = dict()

def parse_cmdline():
    parser = ArgumentParser()
    parser.add_argument("--jmdictfile", help="path to the .xml JMdict file", default="JMdict_e")
    parser.add_argument("--sqlitefile", help="path to the sqlite database to create", required=True)
    parser.add_argument("--dbtableprefix", help="prefix of db tables (ex. jmdict)", default="jmdict")
    parser.add_argument("--appendtables", help="append tables into an existing database file")
    return parser.parse_args()


def create_database(name, prefix, append):
    sqlitefile = name
    global dbtable

    if(prefix != ""):
        prefix += "_"

    dbtable["entry"] = prefix + "entry"
    dbtable["k_ele"] = prefix + "k_ele"
    dbtable["k_ele_ke_inf"] = prefix + "k_ele_ke_inf"
    dbtable["k_ele_ke_pri"] = prefix + "k_ele_ke_pri"
    dbtable["r_ele"] = prefix + "r_ele"
    dbtable["r_ele_re_restr"] = prefix + "r_ele_re_restr"
    dbtable["r_ele_re_inf"] = prefix + "r_ele_re_inf"
    dbtable["r_ele_re_pri"] = prefix + "r_ele_re_pri"
    dbtable["sense"] = prefix + "sense"
    dbtable["sense_stagk"] = prefix + "sense_stagk"
    dbtable["sense_stagr"] = prefix + "sense_stagr"
    dbtable["sense_pos"] = prefix + "sense_pos"
    dbtable["sense_xref"] = prefix + "sense_xref"
    dbtable["sense_ant"] = prefix + "sense_ant"
    dbtable["sense_field"] = prefix + "sense_field"
    dbtable["sense_misc"] = prefix + "sense_misc"
    dbtable["sense_s_inf"] = prefix + "sense_s_inf"
    dbtable["sense_dial"] = prefix + "sense_dial"
    dbtable["sense_gloss"] = prefix + "sense_gloss"

    if len(sqlitefile) < 3 or sqlitefile[-3:] != ".db":
        sqlitefile = sqlitefile + ".db"

    if append == False and os.path.exists(sqlitefile):
        os.remove(sqlitefile)

    database = sqlite3.connect(sqlitefile)
    c = database.cursor()

    c.execute("CREATE TABLE " + dbtable["entry"] + " (ent_seq INTEGER DEFAULT 0)")

    if "k_ele" in xml_elements:
        table_name = dbtable["k_ele"]
        c.execute("CREATE TABLE " + table_name + " (entry_id INTEGER, keb TEXT DEFAULT '')")
        c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (entry_id)")
        c.execute("CREATE INDEX " + table_name + "_keb_index ON " + table_name + " (keb)")

        if "ke_inf" in xml_elements:
            table_name = dbtable["k_ele_ke_inf"]
            c.execute("CREATE TABLE " + table_name + " (k_ele_id INTEGER, ke_inf TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (k_ele_id)")

        if "ke_pri" in xml_elements:
            table_name = dbtable["k_ele_ke_pri"]
            c.execute("CREATE TABLE " + table_name + " (k_ele_id INTEGER, ke_pri TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (k_ele_id)")

    if "r_ele" in xml_elements:
        table_name = dbtable["r_ele"]
        c.execute("CREATE TABLE " + table_name + " (entry_id INTEGER, reb TEXT DEFAULT '', re_nokanji TEXT DEFAULT '')")
        c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (entry_id)")
        c.execute("CREATE INDEX " + table_name + "_reb_index ON " + table_name + " (reb)")

        if "re_restr" in xml_elements:
            table_name = dbtable["r_ele_re_restr"]
            c.execute("CREATE TABLE " + table_name + " (r_ele_id INTEGER, re_restr TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (r_ele_id)")

        if "re_inf" in xml_elements:
            table_name = dbtable["r_ele_re_inf"]
            c.execute("CREATE TABLE " + table_name + " (r_ele_id INTEGER, re_inf TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (r_ele_id)")

        if "re_pri" in xml_elements:
            table_name = dbtable["r_ele_re_pri"]
            c.execute("CREATE TABLE " + table_name + " (r_ele_id INTEGER, re_pri TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (r_ele_id)")

    if "sense" in xml_elements:
        table_name = dbtable["sense"]
        c.execute("CREATE TABLE " + table_name + " (entry_id INTEGER)")
        c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (entry_id)")

        if "stagk" in xml_elements:
            table_name = dbtable["sense_stagk"]
            c.execute("CREATE TABLE " + table_name + " (sense_id INTEGER, stagk TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (sense_id)")

        if "stagr" in xml_elements:
            table_name = dbtable["sense_stagr"]
            c.execute("CREATE TABLE " + table_name + " (sense_id INTEGER, stagr TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (sense_id)")

        if "pos" in xml_elements:
            table_name = dbtable["sense_pos"]
            c.execute("CREATE TABLE " + dbtable["sense_pos"] + " (sense_id INTEGER, pos TEXT)")
            c.execute("CREATE INDEX " + dbtable["sense_pos"] + "_id_index ON " + dbtable["sense_pos"] + " (sense_id)")

        if "xref" in xml_elements:
            table_name = dbtable["sense_xref"]
            c.execute("CREATE TABLE " + table_name + " (sense_id INTEGER, xref TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (sense_id)")

        if "ant" in xml_elements:
            table_name = dbtable["sense_ant"]
            c.execute("CREATE TABLE " + table_name + " (sense_id INTEGER, ant TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (sense_id)")

        if "field" in xml_elements:
            table_name = dbtable["sense_field"]
            c.execute("CREATE TABLE " + table_name + " (sense_id INTEGER, field TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (sense_id)")

        if "misc" in xml_elements:
            table_name = dbtable["sense_misc"]
            c.execute("CREATE TABLE " + table_name + " (sense_id INTEGER, misc TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (sense_id)")

        if "s_inf" in xml_elements:
            table_name = dbtable["sense_s_inf"]
            c.execute("CREATE TABLE " + table_name + " (sense_id INTEGER, s_inf TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (sense_id)")

        if "dial" in xml_elements:
            table_name = dbtable["sense_dial"]
            c.execute("CREATE TABLE " + table_name + " (sense_id INTEGER, dial TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (sense_id)")

        if "gloss" in xml_elements:
            table_name = dbtable["sense_gloss"]
            c.execute("CREATE TABLE " + table_name + " (sense_id INTEGER, gloss TEXT)")
            c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (sense_id)")

    return database


def parse_k_ele(k_ele, entry_id, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO " + dbtable["k_ele"] + " (entry_id) VALUES (?)", [entry_id])
    k_ele_id = c.lastrowid

    for item in k_ele:
        if item.tag not in xml_elements:
            continue
        if item.tag == "keb":
            c.execute("UPDATE " + dbtable["k_ele"] + " SET keb = ? WHERE rowid = ?", (item.text, k_ele_id))
        elif item.tag == "ke_inf":
            c.execute("INSERT INTO " + dbtable["k_ele_ke_inf"] + " (k_ele_id, ke_inf) VALUES (?, ?)", (k_ele_id, dtd[item.text]))
        elif item.tag == "ke_pri":
            c.execute("INSERT INTO " + dbtable["k_ele_ke_pri"] + " (k_ele_id, ke_pri) VALUES (?, ?)", (k_ele_id, item.text))


def parse_r_ele(r_ele, entry_id, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO " + dbtable["r_ele"] + " (entry_id) VALUES (?)", [entry_id])
    r_ele_id = c.lastrowid

    for item in r_ele:
        if item.tag not in xml_elements:
            continue
        if item.tag == "reb":
            c.execute("UPDATE " + dbtable["r_ele"] + " SET reb = ? WHERE rowid = ?", (item.text, r_ele_id))
        elif item.tag == "re_nokanji":
            if(item.text != None):
                c.execute("UPDATE " + dbtable["r_ele"] + " SET re_nokanji = ? WHERE rowid = ?", (item.text, r_ele_id))
        elif item.tag == "re_restr":
            c.execute("INSERT INTO " + dbtable["r_ele_re_restr"] + " (r_ele_id, re_restr) VALUES (?, ?)", (r_ele_id, item.text))
        elif item.tag == "re_inf":
            c.execute("INSERT INTO " + dbtable["r_ele_re_inf"] + " (r_ele_id, re_inf) VALUES (?, ?)", (r_ele_id, dtd[item.text]))
        elif item.tag == "re_pri":
            c.execute("INSERT INTO " + dbtable["r_ele_re_pri"] + " (r_ele_id, re_pri) VALUES (?, ?)", (r_ele_id, item.text))


def parse_sense(sense, entry_id, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO " + dbtable["sense"] + " (entry_id) VALUES (?)", [entry_id])
    sense_id = c.lastrowid

    for item in sense:
        if item.tag not in xml_elements:
            continue
        if item.tag == "stagk":
            c.execute("INSERT INTO " + dbtable["sense_stagk"] + " (sense_id, stagk) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "stagr":
            c.execute("INSERT INTO " + dbtable["sense_stagr"] + " (sense_id, stagr) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "pos":
            c.execute("INSERT INTO " + dbtable["sense_pos"] + " (sense_id, pos) VALUES (?, ?)", (sense_id, dtd[item.text]))
        elif item.tag == "xref":
            c.execute("INSERT INTO " + dbtable["sense_xref"] + " (sense_id, xref) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "ant":
            c.execute("INSERT INTO " + dbtable["sense_ant"] + " (sense_id, ant) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "field":
            c.execute("INSERT INTO " + dbtable["sense_field"] + " (sense_id, field) VALUES (?, ?)", (sense_id, dtd[item.text]))
        elif item.tag == "misc":
            c.execute("INSERT INTO " + dbtable["sense_misc"] + " (sense_id, misc) VALUES (?, ?)", (sense_id, dtd[item.text]))
        elif item.tag == "s_inf":
            c.execute("INSERT INTO " + dbtable["sense_s_inf"] + " (sense_id, s_inf) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "dial":
            c.execute("INSERT INTO " + dbtable["sense_dial"] + " (sense_id, dial) VALUES (?, ?)", (sense_id, dtd[item.text]))
        elif item.tag == "gloss":
            c.execute("INSERT INTO " + dbtable["sense_gloss"] + " (sense_id, gloss) VALUES (?, ?)", (sense_id, item.text))


def parse_entry(entry, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO " + dbtable["entry"] + " DEFAULT VALUES")
    entry_id = c.lastrowid
    
    for item in entry:
        if item.tag not in xml_elements:
            continue
        if item.tag == "ent_seq":
            c.execute("UPDATE " + dbtable["entry"] + " SET ent_seq = ? WHERE rowid = ?", (item.text, entry_id))
        elif item.tag == "k_ele":
            parse_k_ele(item, entry_id, dtd, database)
        elif item.tag == "r_ele":
            parse_r_ele(item, entry_id, dtd, database)
        elif item.tag == "sense":
            parse_sense(item, entry_id, dtd, database)


def load_xml_dtd(file):
    xml_file = codecs.open(file, "r", "utf8")
    value_list = []
    code_list = []

    # Currently the only way I found for extract the xml DTD list
    # is to manually get entity lines and make a small parsing
    for line in xml_file:
        if len(line) >= 2 and line[0:2] == "]>":
            break
        if len(line) >= 15 and line[0:8] == "<!ENTITY":
            code_list.append(line.split(" ")[1])
            value_list.append(line.split("\"")[1])
            
    xml_file.close()

    return dict(zip(value_list, code_list))


def parse_jmdict(file, database):
    dtd = load_xml_dtd(file)
    tree = ET.parse(file)
    root = tree.getroot()
    counter = 0

    if root.tag != "JMdict":
        print("Invalid JMdict file")
        return

    for item in root:
        if item.tag == "entry":
            parse_entry(item, dtd, database)
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

    print("Start importing JMDict data:", end="", flush=True)
    parse_jmdict(args.jmdictfile, database)

    database.close()

    
if __name__ == '__main__':
    main()
