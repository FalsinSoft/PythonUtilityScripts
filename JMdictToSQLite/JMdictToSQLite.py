
from argparse import ArgumentParser, FileType, Action
import xml.etree.ElementTree as ET
import sqlite3
import sys
import os
import codecs

def parse_cmdline():
    parser = ArgumentParser()
    parser.add_argument("--jmdictfile", help="path to the .xml JMdict file", default="JMdict_e")
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

    c.execute("CREATE TABLE entry (ent_seq INTEGER DEFAULT 0)")

    c.execute("CREATE TABLE k_ele (entry_id INTEGER, keb TEXT DEFAULT '')")
    c.execute("CREATE TABLE k_ele_ke_inf (k_ele_id INTEGER, ke_inf TEXT)")
    c.execute("CREATE TABLE k_ele_ke_pri (k_ele_id INTEGER, ke_pri TEXT)")

    c.execute("CREATE TABLE r_ele (entry_id INTEGER, reb TEXT DEFAULT '', re_nokanji TEXT DEFAULT '')")
    c.execute("CREATE TABLE r_ele_re_restr (r_ele_id INTEGER, re_restr TEXT)")
    c.execute("CREATE TABLE r_ele_re_inf (r_ele_id INTEGER, re_inf TEXT)")
    c.execute("CREATE TABLE r_ele_re_pri (r_ele_id INTEGER, re_pri TEXT)")

    c.execute("CREATE TABLE sense (entry_id INTEGER)")
    c.execute("CREATE TABLE sense_stagk (sense_id INTEGER, stagk TEXT)")
    c.execute("CREATE TABLE sense_stagr (sense_id INTEGER, stagr TEXT)")
    c.execute("CREATE TABLE sense_pos (sense_id INTEGER, pos TEXT)")
    c.execute("CREATE TABLE sense_xref (sense_id INTEGER, xref TEXT)")
    c.execute("CREATE TABLE sense_ant (sense_id INTEGER, ant TEXT)")
    c.execute("CREATE TABLE sense_field (sense_id INTEGER, field TEXT)")
    c.execute("CREATE TABLE sense_misc (sense_id INTEGER, misc TEXT)")
    c.execute("CREATE TABLE sense_s_inf (sense_id INTEGER, s_inf TEXT)")
    c.execute("CREATE TABLE sense_dial (sense_id INTEGER, dial TEXT)")
    c.execute("CREATE TABLE sense_gloss (sense_id INTEGER, gloss TEXT)")

    c.execute("CREATE INDEX id_k_ele_index ON k_ele (entry_id)")
    c.execute("CREATE INDEX reb_k_ele_index ON k_ele (keb)")
    c.execute("CREATE INDEX id_k_ele_ke_inf_index ON k_ele_ke_inf (k_ele_id)")
    c.execute("CREATE INDEX id_k_ele_ke_pri_index ON k_ele_ke_pri (k_ele_id)")

    c.execute("CREATE INDEX id_r_ele_index ON r_ele (entry_id)")
    c.execute("CREATE INDEX reb_r_ele_index ON r_ele (reb)")
    c.execute("CREATE INDEX id_r_ele_re_restr_index ON r_ele_re_restr (r_ele_id)")
    c.execute("CREATE INDEX id_r_ele_re_inf_index ON r_ele_re_inf (r_ele_id)")
    c.execute("CREATE INDEX id_r_ele_re_pri_index ON r_ele_re_pri (r_ele_id)")

    c.execute("CREATE INDEX id_sense_index ON sense (entry_id)")
    c.execute("CREATE INDEX id_sense_stagk_index ON sense_stagk (sense_id)")
    c.execute("CREATE INDEX id_sense_stagr_index ON sense_stagr (sense_id)")
    c.execute("CREATE INDEX id_sense_pos_index ON sense_pos (sense_id)")
    c.execute("CREATE INDEX id_sense_xref_index ON sense_xref (sense_id)")
    c.execute("CREATE INDEX id_sense_ant_index ON sense_ant (sense_id)")
    c.execute("CREATE INDEX id_sense_field_index ON sense_field (sense_id)")
    c.execute("CREATE INDEX id_sense_misc_index ON sense_misc (sense_id)")
    c.execute("CREATE INDEX id_sense_s_inf_index ON sense_s_inf (sense_id)")
    c.execute("CREATE INDEX id_sense_dial_index ON sense_dial (sense_id)")
    c.execute("CREATE INDEX id_sense_gloss_index ON sense_gloss (sense_id)")

    return database


def parse_k_ele(k_ele, entry_id, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO k_ele (entry_id) VALUES (?)", [entry_id])
    k_ele_id = c.lastrowid

    for item in k_ele:
        if item.tag == "keb":
            c.execute("UPDATE k_ele SET keb = ? WHERE rowid = ?", (item.text, k_ele_id))
        elif item.tag == "ke_inf":
            c.execute("INSERT INTO k_ele_ke_inf (k_ele_id, ke_inf) VALUES (?, ?)", (k_ele_id, dtd[item.text]))
        elif item.tag == "ke_pri":
            c.execute("INSERT INTO k_ele_ke_pri (k_ele_id, ke_pri) VALUES (?, ?)", (k_ele_id, item.text))


def parse_r_ele(r_ele, entry_id, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO r_ele (entry_id) VALUES (?)", [entry_id])
    r_ele_id = c.lastrowid

    for item in r_ele:
        if item.tag == "reb":
            c.execute("UPDATE r_ele SET reb = ? WHERE rowid = ?", (item.text, r_ele_id))
        elif item.tag == "re_nokanji":
            if(item.text != None):
                c.execute("UPDATE r_ele SET re_nokanji = ? WHERE rowid = ?", (item.text, r_ele_id))
        elif item.tag == "re_restr":
            c.execute("INSERT INTO r_ele_re_restr (r_ele_id, re_restr) VALUES (?, ?)", (r_ele_id, item.text))
        elif item.tag == "re_inf":
            c.execute("INSERT INTO r_ele_re_inf (r_ele_id, re_inf) VALUES (?, ?)", (r_ele_id, dtd[item.text]))
        elif item.tag == "re_pri":
            c.execute("INSERT INTO r_ele_re_pri (r_ele_id, re_pri) VALUES (?, ?)", (r_ele_id, item.text))


def parse_sense(sense, entry_id, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO sense (entry_id) VALUES (?)", [entry_id])
    sense_id = c.lastrowid

    for item in sense:
        if item.tag == "stagk":
            c.execute("INSERT INTO sense_stagk (sense_id, stagk) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "stagr":
            c.execute("INSERT INTO sense_stagr (sense_id, stagr) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "pos":
            c.execute("INSERT INTO sense_pos (sense_id, pos) VALUES (?, ?)", (sense_id, dtd[item.text]))
        elif item.tag == "xref":
            c.execute("INSERT INTO sense_xref (sense_id, xref) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "ant":
            c.execute("INSERT INTO sense_ant (sense_id, ant) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "field":
            c.execute("INSERT INTO sense_field (sense_id, field) VALUES (?, ?)", (sense_id, dtd[item.text]))
        elif item.tag == "misc":
            c.execute("INSERT INTO sense_misc (sense_id, misc) VALUES (?, ?)", (sense_id, dtd[item.text]))
        elif item.tag == "s_inf":
            c.execute("INSERT INTO sense_s_inf (sense_id, s_inf) VALUES (?, ?)", (sense_id, item.text))
        elif item.tag == "dial":
            c.execute("INSERT INTO sense_dial (sense_id, dial) VALUES (?, ?)", (sense_id, dtd[item.text]))
        elif item.tag == "gloss":
            c.execute("INSERT INTO sense_gloss (sense_id, gloss) VALUES (?, ?)", (sense_id, item.text))


def parse_entry(entry, dtd, database):
    c = database.cursor()
    c.execute("INSERT INTO entry DEFAULT VALUES")
    entry_id = c.lastrowid
    
    for item in entry:
        if item.tag == "ent_seq":
            c.execute("UPDATE entry SET ent_seq = ? WHERE rowid = ?", (item.text, entry_id))
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

    print("Create database...", end="\n", flush=True)
    database = create_database(args.sqlitefile.strip())

    print("Start importing JMDict data:", end="", flush=True)
    parse_jmdict(args.jmdictfile, database)

    database.close()

    
if __name__ == '__main__':
    main()
