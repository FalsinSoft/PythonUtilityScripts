
from argparse import ArgumentParser, FileType, Action
import xml.etree.ElementTree as ET
import sqlite3
import sys
import os
import codecs

dbtable = dict()

def parse_cmdline():
    parser = ArgumentParser()
    parser.add_argument("--jmnedictfile", help="path to the .xml JMnedict file", default="JMnedict.xml")
    parser.add_argument("--sqlitefile", help="path to the sqlite database to create", required=True)
    parser.add_argument("--dbtableprefix", help="prefix of db tables (ex. jmnedict)", default="jmnedict")
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
    dbtable["trans"] = prefix + "trans"
    dbtable["trans_name_type"] = prefix + "trans_name_type"
    dbtable["trans_xref"] = prefix + "trans_xref"
    dbtable["trans_trans_det"] = prefix + "trans_trans_det"

    if len(sqlitefile) < 3 or sqlitefile[-3:] != ".db":
        sqlitefile = sqlitefile + ".db"

    if append == False and os.path.exists(sqlitefile):
        os.remove(sqlitefile)

    database = sqlite3.connect(sqlitefile)
    c = database.cursor()

    c.execute("CREATE TABLE " + dbtable["entry"] + " (ent_seq INTEGER DEFAULT 0)")

    table_name = dbtable["k_ele"]
    c.execute("CREATE TABLE " + table_name + " (entry_id INTEGER, keb TEXT DEFAULT '')")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (entry_id)")
    c.execute("CREATE INDEX " + table_name + "_keb_index ON " + table_name + " (keb)")

    table_name = dbtable["k_ele_ke_inf"]
    c.execute("CREATE TABLE " + table_name + " (k_ele_id INTEGER, ke_inf TEXT)")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (k_ele_id)")

    table_name = dbtable["k_ele_ke_pri"]
    c.execute("CREATE TABLE " + table_name + " (k_ele_id INTEGER, ke_pri TEXT)")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (k_ele_id)")

    table_name = dbtable["r_ele"]
    c.execute("CREATE TABLE " + table_name + " (entry_id INTEGER, reb TEXT DEFAULT '')")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (entry_id)")
    c.execute("CREATE INDEX " + table_name + "_reb_index ON " + table_name + " (reb)")

    table_name = dbtable["r_ele_re_restr"]
    c.execute("CREATE TABLE " + table_name + " (r_ele_id INTEGER, re_restr TEXT)")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (r_ele_id)")

    table_name = dbtable["r_ele_re_inf"]
    c.execute("CREATE TABLE " + table_name + " (r_ele_id INTEGER, re_inf TEXT)")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (r_ele_id)")

    table_name = dbtable["r_ele_re_pri"]
    c.execute("CREATE TABLE " + table_name + " (r_ele_id INTEGER, re_pri TEXT)")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (r_ele_id)")

    table_name = dbtable["trans"]
    c.execute("CREATE TABLE " + table_name + " (entry_id INTEGER)")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (entry_id)")

    table_name = dbtable["trans_name_type"]
    c.execute("CREATE TABLE " + table_name + " (trans_id INTEGER, name_type TEXT)")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (trans_id)")

    table_name = dbtable["trans_xref"]
    c.execute("CREATE TABLE " + table_name + " (trans_id INTEGER, xref TEXT)")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (trans_id)")

    table_name = dbtable["trans_trans_det"]
    c.execute("CREATE TABLE " + table_name + " (trans_id INTEGER, trans_det TEXT)")
    c.execute("CREATE INDEX " + table_name + "_id_index ON " + table_name + " (trans_id)")

    return database


def parse_k_ele(k_ele, entry_id, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO " + dbtable["k_ele"] + " (entry_id) VALUES (?)", [entry_id])
    k_ele_id = c.lastrowid

    for item in k_ele:
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
        if item.tag == "reb":
            c.execute("UPDATE " + dbtable["r_ele"] + " SET reb = ? WHERE rowid = ?", (item.text, r_ele_id))
        elif item.tag == "re_restr":
            c.execute("INSERT INTO " + dbtable["r_ele_re_restr"] + " (r_ele_id, re_restr) VALUES (?, ?)", (r_ele_id, item.text))
        elif item.tag == "re_inf":
            c.execute("INSERT INTO " + dbtable["r_ele_re_inf"] + " (r_ele_id, re_inf) VALUES (?, ?)", (r_ele_id, dtd[item.text]))
        elif item.tag == "re_pri":
            c.execute("INSERT INTO " + dbtable["r_ele_re_pri"] + " (r_ele_id, re_pri) VALUES (?, ?)", (r_ele_id, item.text))


def parse_trans(trans, entry_id, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO " + dbtable["trans"] + " (entry_id) VALUES (?)", [entry_id])
    trans_id = c.lastrowid

    for item in trans:
        if item.tag == "name_type":
            c.execute("INSERT INTO " + dbtable["trans_name_type"] + " (trans_id, name_type) VALUES (?, ?)", (trans_id, dtd[item.text]))
        elif item.tag == "xref":
            c.execute("INSERT INTO " + dbtable["trans_xref"] + " (trans_id, xref) VALUES (?, ?)", (trans_id, item.text))
        elif item.tag == "trans_det":
            c.execute("INSERT INTO " + dbtable["trans_trans_det"] + " (trans_id, trans_det) VALUES (?, ?)", (trans_id, item.text))


def parse_entry(entry, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO " + dbtable["entry"] + " DEFAULT VALUES")
    entry_id = c.lastrowid
    
    for item in entry:
        if item.tag == "ent_seq":
            c.execute("UPDATE " + dbtable["entry"] + " SET ent_seq = ? WHERE rowid = ?", (item.text, entry_id))
        elif item.tag == "k_ele":
            parse_k_ele(item, entry_id, dtd, database)
        elif item.tag == "r_ele":
            parse_r_ele(item, entry_id, dtd, database)
        elif item.tag == "trans":
            parse_trans(item, entry_id, dtd, database)


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


def parse_jmnedict(file, database):
    dtd = load_xml_dtd(file)
    tree = ET.parse(file)
    root = tree.getroot()
    counter = 0

    if root.tag != "JMnedict":
        print("Invalid JMnedict file")
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

    print("Start importing JMnedict data:", end="", flush=True)
    parse_jmnedict(args.jmnedictfile, database)

    database.close()

    
if __name__ == '__main__':
    main()
