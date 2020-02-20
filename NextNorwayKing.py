#!python
import bs4
import ijson
import mysql.connector
import os
import requests


class CNextNorwayKing:

    filename = ""
    royal_titles = ""
    ins_vikings_tbl_sql = ""
    ins_experience_tbl_sql = ""
    result_tbl = ()

    def __init__(self):

        db_cred_file = "db_credentials.txt"

        self.royal_titles = ["King", "Prince", "Queen", "Princess"]
        self.ins_vikings_tbl_sql = \
            ("INSERT INTO vikings.vikings(FName, LName, "
             "NFriends, NBattles, ImgUrl) "
             "VALUES(%s, %s, %s, %s, %s)")
        self.ins_experience_tbl_sql = \
            ("INSERT INTO vikings.experience"
             "(VID, BattleName, Title, IsCurrent, IsRoyal) "
             "VALUES(%s, %s, %s, %s, %s)")
        self.ins_enrichment_tbl_sql = \
            ("INSERT INTO vikings.enrichment(VID, Status, BirthYear, Home) "
             "VALUES(%s, %s, %s, %s)")
        self.create_db_sql = \
            ["CREATE DATABASE IF NOT EXISTS vikings",
             "CREATE TABLE IF NOT EXISTS "
             "vikings.vikings(VID INT UNSIGNED NOT NULL "
             "UNIQUE AUTO_INCREMENT PRIMARY KEY, FName VARCHAR(50), "
             "LName VARCHAR(50), NFriends SMALLINT, "
             "NBattles SMALLINT, ImgUrl VARCHAR(1024))",
             "CREATE TABLE IF NOT EXISTS vikings.experience "
             "(VID INT UNSIGNED NOT NULL, BattleName VARCHAR(50) NULL, Title "
             "VARCHAR(50) NULL, IsCurrent TINYINT NULL, IsRoyal TINYINT NULL, "
             "CONSTRAINT VID1 FOREIGN KEY (VID) REFERENCES vikings.vikings(VID)"
             " ON DELETE CASCADE ON UPDATE CASCADE)",
             "CREATE TABLE IF NOT EXISTS vikings.enrichment "
             "(VID INT UNSIGNED NOT NULL UNIQUE, Status VARCHAR(50) NULL, "
             "BirthYear VARCHAR(50) NULL, Home VARCHAR(50) NULL, "
             "CONSTRAINT VID2 FOREIGN KEY (VID) REFERENCES vikings.vikings(VID)"
             " ON DELETE CASCADE ON UPDATE CASCADE)",
             ]

        print("")
        print("")
        print("-" * 79)

        if os.stat(db_cred_file).st_size == 0:
            username = input("Enter username of DB: ")
            password = input("Enter password of DB: ")
            with open(db_cred_file, 'w') as f:
                f.write(username + '\n')
                f.write(password)
        else:
            with open(db_cred_file, 'r') as f:
                username = f.readline().strip('\n')
                password = f.readline().strip('\n')

        print("-" * 79)
        self.my_db = mysql.connector.connect(
            host="localhost", user=username, passwd=password)
        self.cursor = self.my_db.cursor()

    def create_db(self, sql_cmds):

        # Workaround... Doesn't work with self.cursor.execute(sqlCmd, multi=True)
        for sql_cmd in sql_cmds:
            self.cursor.execute(sql_cmd)
        # self.myDb.commit()

    def reset_tables(self):
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        self.cursor.execute("TRUNCATE TABLE vikings.experience;")
        self.cursor.execute("TRUNCATE TABLE vikings.enrichment;")
        self.cursor.execute("TRUNCATE TABLE vikings.vikings;")
        self.cursor.execute("ALTER TABLE vikings.vikings AUTO_INCREMENT = 1;")
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

    def set_file(self, filename):
        self.filename = filename

    def run(self):

        print("")
        print("")

        self.json_to_db()
        self.get_data()
        self.visualize_data()

    def json_to_db(self):

        with open(self.filename, 'r') as f:
            vikings = ijson.items(f, 'item')

            for viking in vikings:

                has_experience = False

                f_name = viking["firstName"]
                l_name = viking["lastName"]
                n_friends = viking["numberOfVikingFriends"]
                img_url = viking["imgUrl"]

                if ("experience" in viking):
                    n_battles = len(viking["experience"])
                    has_experience = True
                else:
                    n_battles = 0

                val = (f_name, l_name, n_friends, n_battles, img_url)

                self.store_data_to_db(self.ins_vikings_tbl_sql, val)

                self.cursor.execute("SELECT LAST_INSERT_ID()")
                vid = self.cursor.fetchone()[0]

                if has_experience:

                    for i_battle in range(len(viking["experience"])):

                        battle_name = viking["experience"][i_battle]["name"]
                        title = viking["experience"][i_battle]["jobTitle"]

                        if title in self.royal_titles:
                            is_royal = 1
                        else:
                            is_royal = 0

                        if i_battle == 0:
                            is_current = 1
                        else:
                            is_current = 0

                        val = (vid, battle_name, title, is_current, is_royal)

                        self.store_data_to_db(self.ins_experience_tbl_sql, val)

                img_url = self.scrap_data(f_name, "image")
                self.store_data_to_db(
                    "UPDATE vikings.vikings SET ImgUrl = %s WHERE VID = %s",
                    (img_url, vid))
                status = self.scrap_data(f_name, "status")
                born = self.scrap_data(f_name, "born")
                home = self.scrap_data(f_name, "abode")

                val3 = (vid, status, born, home)
                self.store_data_to_db(self.ins_enrichment_tbl_sql, val3)

    def get_data(self):
        self.cursor.execute(
            ("SELECT "
             "ImgUrl, FName, LName, Title, BirthYear, Home, NBattles, NFriends "
             "FROM vikings.vikings "
             "INNER JOIN vikings.experience USING(VID) "
             "INNER JOIN vikings.enrichment USING(VID) "
             "WHERE vikings.experience.IsCurrent = 1 AND "
             "vikings.experience.IsRoyal = 1 AND "
             "vikings.enrichment.Status = 'Alive' "
             "ORDER BY NBattles DESC"))
        self.result_tbl = self.cursor.fetchall()

    def scrap_data(self, f_name, prop):

        req = requests.get("https://vikings.fandom.com/wiki/" + f_name)
        bs = bs4.BeautifulSoup(req.text, 'html.parser')
        data = bs.find(attrs={"data-source": prop})

        if data is not None:
            if prop == "image":
                val = data.find(
                    "img", {"class": "pi-image-thumbnail"}).attrs["src"]
            else:
                div = data.find("div", {"class": "pi-data-value pi-font"})
                if len(div.contents) == 1:
                    val = div.text
                else:
                    val = div.find("a").text
        else:
            val = ""

        return val

    def visualize_data(self):
        hdr_str = ('<TH style="text-align:center">Image</TH>'
                   '<TH style="text-align:center">First Name</TH>'
                   '<TH style="text-align:center">Last Name</TH>'
                   '<TH style="text-align:center">Title</TH>'
                   '<TH style="text-align:center">Year of Birth</TH>'
                   '<TH style="text-align:center">Home</TH>'
                   '<TH style="text-align:center"># of Battles</TH>'
                   '<TH style="text-align:center"># of Friends</TH>')
        html_str = '<TABLE border="1" style="border: 1px solid #000000; '
        'border-collapse: collapse;" cellpadding="4">\n'
        html_str += ('<TR>\n' + hdr_str + '</TR>\n')

        for tbl_row in self.result_tbl:
            html_str += '<TR>\n<TD style="text-align:center">'
            '<img width="100px" height=auto src=' + \
                tbl_row[0] + '></TD>\n'
            for i_item in range(1, len(tbl_row)):
                html_str += ('<TD style="text-align:center">'
                             + str(tbl_row[i_item]) + '</TD>\n')
            html_str += '</TR>\n'

        html_str += '</TABLE>'

        with open('NextVikingsKingRecomendation.html', 'w') as f:
            f.write(html_str)

    def store_data_to_db(self, sql, val):
        self.cursor.execute(sql, val)
        self.my_db.commit()


if __name__ == "__main__":

    nextNorwayKing = CNextNorwayKing()
    nextNorwayKing.create_db(nextNorwayKing.create_db_sql)
    nextNorwayKing.reset_tables()
    nextNorwayKing.set_file("Vikings.json")
    nextNorwayKing.run()
