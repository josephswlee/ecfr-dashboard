import sqlite3
import logging

class DatabaseLoader:
    def __init__(self, db_path='data/cfr.db'):
        self.conn = sqlite3.connect(db_path)
        self.create_tables()

    def create_tables(self):
        with self.conn:
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS agencies (
                    agency_id INTEGER PRIMARY KEY,
                    name TEXT,
                    short_name TEXT,
                    display_name TEXT,
                    sortable_name TEXT,
                    slug TEXT UNIQUE,
                    parent_id INTEGER
                )
            ''')
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS cfr_sections (
                    section_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title_number TEXT,
                    title_head TEXT,
                    chapter_number TEXT,
                    chapter_head TEXT,
                    subchapter_number TEXT,
                    subchapter_head TEXT,
                    part_number TEXT,
                    part_head TEXT,
                    section_number TEXT,
                    section_title TEXT,
                    body TEXT
                )
            ''')
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS cfr_references (
                    reference_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agency_id INTEGER NOT NULL,
                    title TEXT,
                    chapter TEXT,
                    part TEXT,
                    FOREIGN KEY (agency_id) REFERENCES agencies(agency_id)
                )
            ''')

    def insert_agency(self, agency):
        self.conn.execute('''
                INSERT OR REPLACE INTO agencies 
                (agency_id, name, short_name, display_name, sortable_name, slug, parent_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                agency['agency_id'],
                agency['name'],
                agency['short_name'],
                agency['display_name'],
                agency['sortable_name'],
                agency['slug'],
                agency['parent_id']
            ))
        
    def insert_cfr_section(self, section):
        with self.conn:
            self.conn.execute('''
                INSERT INTO cfr_sections 
                (title_number, title_head, chapter_number, chapter_head, 
                subchapter_number, subchapter_head, part_number, part_head, 
                section_number, section_title, body)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                section['title_number'],
                section['title_head'],
                section['chapter_number'],
                section['chapter_head'],
                section['subchapter_number'],
                section['subchapter_head'],
                section['part_number'],
                section['part_head'],
                section['section_number'],
                section['section_title'],
                section['body']
            ))
    
    def insert_cfr_reference(self, agency_id, reference):
        with self.conn:
            self.conn.execute('''
                INSERT INTO cfr_references (agency_id, title, chapter, part)
                VALUES (?, ?, ?, ?)
            ''', (
                agency_id,
                reference.get('title'),
                reference.get('chapter'),
                reference.get('part')
            ))