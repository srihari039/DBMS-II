import time
import psycopg2
import csv
from collections import OrderedDict

class set_connection:
    def establish_connection(self,dbname,user,password,host,port):
        connect_db = psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host} port={port}")
        print('Connection established')
        return connect_db

    def create_tables(self,cursor):
        start = time.time()
        table_commands = [
            """
                CREATE TABLE research_paper(
                    paper_id SERIAL PRIMARY KEY,
                    abstract TEXT NOT NULL,
                    year INT NOT NULL
                )
            """,
            """
                CREATE TABLE author(
                    author_id INT PRIMARY KEY,
                    name TEXT
                )
            """,
            """
                CREATE TABLE authored_by(
                    paper_id INT,
                    author_id INT,
                    priority INT,
                    PRIMARY KEY(paper_id,author_id)
                )
            """,
            """
                CREATE TABLE reference_papers(
                    paper_id INT,
                    ref_id INT
                )
            """,
            """
                CREATE TABLE conference(
                    conference_id INT PRIMARY KEY,
                    conference_name TEXT
                )
            """,
            """
                CREATE TABLE held_at(
                    paper_id INT,
                    conference_id INT,
                    valid_years INT,
                    title TEXT,
                    PRIMARY KEY(paper_id,conference_id)        
                )
            """
        ]

        for create_table in table_commands:
            cursor.execute(create_table)
        self.connect_db.commit()
        end = time.time()
        print(f"Time taken to create tables : {end-start}s")
        print('Tables generated successfully')


    def drop_tables(self,cursor):

        tables = [
            """
                DROP TABLE IF EXISTS research_paper
            """,
            """
                DROP TABLE IF EXISTS author
            """,
            """
                DROP TABLE IF EXISTS authored_by
            """,
             """
                DROP TABLE IF EXISTS reference_papers
            """,
             """
                DROP TABLE IF EXISTS conference
            """,
             """
                DROP TABLE IF EXISTS held_at
            """
        ]

        for drop in tables:
             cursor.execute(drop)
        self.connect_db.commit()
        print('Already Present Tables are dropped successfully')

    def handle_connection(self):
        # dbname,user,password,port = "dbms2","postgres","dbms2",5432
        dbname = input('Enter Data base name : ')
        user = input('Enter user name : ')
        password = input('Enter password : ')
        port = input('Enter port : ')
        host = input('Enter host : ')

        try:
            self.connect_db = self.establish_connection(dbname,user,password,host,port)
            db_cursor = self.connect_db.cursor()
            self.drop_tables(db_cursor)
            self.create_tables(db_cursor)
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    def __init__(self):
        self.handle_connection()

class Parser:
    def __init__(self,filename):
        self.lines = []
        self.connector = set_connection()
        self.loadfile(filename)
        self.cursor = self.connector.connect_db.cursor()
        # self.open_csv()
        self.parselines()

    def loadfile(self,filename):
        file = open(filename,"r",encoding='utf-8')
        self.lines = file.readlines()
        file.close()
        print('Loaded file successfully')

    def reset(self):
        return 0,0,'Not specified','Not specified',[],[],'Not specified','Not specified',0

    def insert_into_db(self,command):
        try:
            self.cursor.execute(command)
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        
    def open_csv(self):
        headers = [
            ['paper_id','abstract','year'],
            ['author_id','name'],
            ['paper_id',"author_id",'priority'],
            ['paper_id','ref_id'],
            ['conference_id','conference_name'],
            ['paper_id','conference_id','valid_years','title']
        ]
        filenames = ['research_paper.csv','author.csv','authored_by.csv','reference_papers.csv','conference.csv','held_at.csv']

        self.fds=[]
        itr = 0
        for filename in filenames:
            file = open(filename,'w',encoding='utf-8')
            writer = csv.writer(file)
            writer.writerow(headers[itr])
            itr += 1
            self.fds.append(writer)

    def parselines(self):
        print('Parsing started ...')
        start = time.time()
        paper_id,year,abstract,author,authors_list,references,title,conference,order = self.reset()
        conferences = {}
        authors = {}
        conference_id = 0
        author_id = 0
        for line in self.lines:
            line = line.rstrip()
            if "#*" in line:
                title = line[2:].replace("'","''")
            elif "#index" in line:
                paper_id = int(line[6:])
            elif "#t" in line:
                year = int(line[2:])
            elif "#c" in line:
                possible_conference = line[2:].replace("'","''")
                isEmpty = possible_conference == ''
                if isEmpty:
                    continue
                else:
                    conference = possible_conference
            elif "#@" in line:
                authors_list.append(line[2:].replace("'","''"))
                authors_list = list(OrderedDict.fromkeys(authors_list[0].split(',')))
            elif "#%" in line:
                references.append(int(line[2:]))
            elif "#!" in line:
                abstract = line[2:].replace("'","''")
            elif line == '':
                insert_into_paper = f'''INSERT INTO research_paper(paper_id,abstract,year) VALUES({paper_id},'{abstract}',{year})'''
                self.insert_into_db(insert_into_paper)
                if len(references) > 0:
                    for ref_id in references:
                        insert_into_references = f'''INSERT INTO reference_papers(paper_id,ref_id) VALUES({paper_id},{ref_id})'''
                        self.insert_into_db(insert_into_references)
                else:
                    insert_into_references = f'''INSERT INTO reference_papers(paper_id) VALUES({paper_id})'''
                    self.insert_into_db(insert_into_references)

                if conference not in conferences:
                    conferences[conference] = conference_id
                    insert_into_conferences = f'''INSERT INTO conference(conference_id,conference_name) VALUES({conference_id},'{conference}')'''
                    self.insert_into_db(insert_into_conferences)
                    conference_id += 1
                insert_into_held_at = f'''INSERT INTO held_at(conference_id,paper_id,title,valid_years) VALUES({conferences[conference]},{paper_id},'{title}',{-1})'''
                self.insert_into_db(insert_into_held_at)
                for author in authors_list:
                    if author!="":
                        if author not in authors:
                            authors[author] = author_id
                            insert_into_author = f'''INSERT INTO author(author_id,name) VALUES({author_id},'{author}')'''
                            self.insert_into_db(insert_into_author)
                            author_id += 1
                        insert_into_authored_by = f'''INSERT INTO authored_by(paper_id,author_id,priority) VALUES({paper_id},{authors[author]},{order})'''
                        self.insert_into_db(insert_into_authored_by)
                        order += 1
                paper_id,year,abstract,author,authors_list,references,title,conference,order = self.reset()
        print('Parsing completed')
        self.connector.connect_db.commit()
        print('Commit completed')
        end = time.time()
        print(f'Time taken to parse file and load into db : {end-start}s')

parser = Parser("source.txt")
