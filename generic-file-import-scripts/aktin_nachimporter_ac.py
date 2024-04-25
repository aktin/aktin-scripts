# -*- coding: utf-8 -*
# Created on Fr Apr 12 13:48:00 2024
# @VERSION=1.0.0
# @VIEWNAME=AKTIN-Nachimporter-AC
# @MIMETYPE=csv
# @ID=anac
#
#      Copyright (c) 2024 AKTIN
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU Affero General Public License as
#      published by the Free Software Foundation, either version 3 of the
#      License, or (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU Affero General Public License for more details.
#
#      You should have received a copy of the GNU Affero General Public License
#      along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#
import sys
import traceback
from abc import ABCMeta, ABC, abstractmethod
from datetime import datetime
import base64
import hashlib
import os
import re
import sqlalchemy as db
from sqlalchemy.engine.base import Connection
import pandas as pd


class DiagnoseData:
    """
    This class contains diagnose data for updating the database.
    """

    def __init__(self):
        self.__pat_ide = None
        self.__enc_ide = None
        self.__diagnoses = None

    def get_pat_ide(self):
        return self.__pat_ide

    def get_enc_ide(self):
        return self.__enc_ide

    def get_diagnoses(self):
        return self.__diagnoses

    def set_pat_ide(self, pat_ide):
        self.__pat_ide = pat_ide

    def set_enc_ide(self, enc_ide):
        self.__enc_ide = enc_ide

    def set_diagnoses(self, diagnoses):
        self.__diagnoses = diagnoses


class SingletonMeta(type):
    """
     Metaclass implementing the Singleton design pattern.
     This metaclass ensures that only one instance of each class using it is created.
     """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class SingletonABCMeta(ABCMeta):
    """
       Metaclass implementing the Singleton design pattern for abstract base classes (ABCs).
       This metaclass ensures that only one instance of each ABC using it is created.
       """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonABCMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Logger(metaclass=SingletonMeta):
    def __init__(self):
        self.csv_encounters = 0
        self.successful_insert = 0

    def set_csv_encounters(self, csv_encounters):
        self.csv_encounters = csv_encounters

    def increase_db_insert(self):
        self.successful_insert += 1

    def get_csv_encounters(self):
        return self.csv_encounters

    def get_db_insert(self):
        return self.successful_insert


class CSVReader(metaclass=SingletonMeta):
    __seperator: str = ';'
    __encoding: str = 'utf-8'

    def __init__(self):
        self.__path_csv = None

    def is_csv_file(self, file_path):
        _, file_extension = os.path.splitext(file_path)
        return file_extension, file_extension.lower() == '.csv'

    def set_csv_path(self, path_csv: str):
        file_type, is_csv = self.is_csv_file(path_csv)
        if is_csv:
            self.__path_csv = path_csv
        else:
            raise Exception('Required CSV, got: '+file_type)

    def iter_rows(self):
        row_count = 0
        for row in pd.read_csv(self.__path_csv, chunksize=1, sep=self.__seperator, encoding=self.__encoding, dtype=str):
            row_count += 1
            yield row
        Logger().set_csv_encounters(row_count)


class AktinImporter:
    """This class implements the main functionality and structures the pipline for updating."""

    def __init__(self):
        self.__reader = CSVReader()
        self.__pipeline = self.__init_pipeline()
        os.environ['sourcesystem_cd'] = ('gfi_' + os.environ['script_id']
                                         + 'V' + os.environ['script_version']
                                         + '_' + Helper().hash_filename()[:50])

    @staticmethod
    def __init_pipeline():
        eid = EncounterIDHandler()
        pid = PatientIDHandler(eid)
        icd = ICDHandler(pid)
        return icd

    def import_csv(self, path_csv: str):
        conn = DatabaseConnection()
        connection = conn.connect()
        pat_map = PatientMappingEntryHandler(connection)
        enc_map = EncounterMappingEntryHandler(connection)
        obs = ObservationFactEntryHandler(connection)
        self.__reader.set_csv_path(path_csv)
        for index, r in enumerate(self.__reader.iter_rows()):
            data = DiagnoseData()
            data = self.__pipeline.update_pat_from_row(data, r, str(index + 2))
            if data:
                enc_num = enc_map.get_encounter_num_for_ide(data.get_enc_ide())
                pat_num = pat_map.get_patient_num_for_ide(data.get_pat_ide())
                obs.update_entries(enc_num, pat_num, data)
        conn.disconnect()
        print('Encounters: ' + str(Logger().get_csv_encounters()) + ', Inserted Diagnoses: ' + str(Logger().get_db_insert()))


class PatientDataColumnHandler(ABC, metaclass=SingletonABCMeta):
    """This abstract class is used to process columns from the csv. Each
    child class checks for one specific column and updates the DiagnoseData object."""
    _column_name: str

    def __init__(self, successor: 'PatientDataColumnHandler' = None):
        self.__successor = successor
        self._helper = Helper()

    def update_pat_from_row(self, data: DiagnoseData, row: pd.Series, index: str):
        try:
            data = self._process_column(data, row)
            if self.__successor is not None:
                return self.__successor.update_pat_from_row(data, row, index)
            return data
        except Exception:
            print(f'{self.__class__.__name__}: Invalid patient in row {index}, '+self.__successor._column_name)
            return None

    @abstractmethod
    def _process_column(self, pat: DiagnoseData, row: pd.Series) -> DiagnoseData:
        pass

    def _get_my_value_from_row(self, row: pd.Series) -> str:
        return row[self._column_name].values[0]


class EncounterIDHandler(PatientDataColumnHandler):
    _column_name = 'Fall'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row)
        enc_ide = self._helper.anonymize_enc(val)
        data.set_enc_ide(enc_ide)
        return data


class PatientIDHandler(PatientDataColumnHandler):
    _column_name = 'Patient'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row)
        pat_ide = self._helper.anonymize_pat(val)
        data.set_pat_ide(pat_ide)
        return data


class ICDHandler(PatientDataColumnHandler):
    _column_name = 'icd'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row).split(',')
        data.set_diagnoses(val)
        return data


class Helper(metaclass=SingletonMeta):

    def __init__(self):
        self.__path_properties = os.environ['path_aktin_properties']
        if not os.path.exists(self.__path_properties):
            raise SystemExit('file path for aktin.properties is not valid')
        self.__alg = self.__get_hash_algorithm_name()
        self.__salt = self.__get_aktin_property('pseudonym.salt')
        self.__pat_root = self.__get_aktin_property('cda.patient.root.preset')
        self.__enc_root = self.__get_aktin_property('cda.encounter.root.preset')

    def __get_hash_algorithm_name(self) -> str:
        name = self.__get_aktin_property('pseudonym.algorithm') or 'sha1'
        return str.lower(name.replace('-', '', ).replace('/', '_'))

    def __get_aktin_property(self, prop: str) -> str:
        with open(self.__path_properties) as properties:
            for line in properties:
                if '=' in line:
                    key, value = line.split('=', 1)
                    if key == prop:
                        return value.strip()
            return ''

    def hash_filename(self):
        filename = os.path.basename(__file__)
        return self.__anonymize_composite(filename)

    @staticmethod
    def convert_date_to_i2b2_format(date: str) -> str:
        if len(date) == 8:
            return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        elif len(date) == 12:
            if date[8:10] == '24':
                date = ''.join([date[:8], '2359'])
            return datetime.strptime(date, '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M')
        elif len(date) == 14:
            return datetime.strptime(date, '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        else:
            raise ValueError()

    def anonymize_enc(self, ext) -> str:
        return self.__anonymize(self.__enc_root, ext)

    def anonymize_pat(self, ext) -> str:
        return self.__anonymize(self.__pat_root, ext)

    def __anonymize_composite(self, composite: str):
        buffer = composite.encode('UTF-8')
        alg = getattr(hashlib, self.__alg)()
        alg.update(buffer)
        return base64.urlsafe_b64encode(alg.digest()).decode('UTF-8')

    def __anonymize(self, root, ext) -> str:
        composite = '/'.join([str(root), str(ext)])
        composite = self.__salt + composite if self.__salt else composite
        return self.__anonymize_composite(composite)


class DatabaseConnection(metaclass=SingletonMeta):

    def __init__(self):
        self.__username = os.environ['username']
        self.__password = os.environ['password']
        self.__i2b2_url = os.environ['connection-url']
        self.__engine = None
        self.__connection = None

    def connect(self) -> Connection:
        pattern = 'jdbc:postgresql://(.*?)(\?searchPath=..*)?$'
        url = re.search(pattern, self.__i2b2_url).group(1)

        if self.__engine is None:
            self.__engine = db.create_engine(f'postgresql+psycopg2://{self.__username}:{self.__password}@{url}')
        if self.__connection is None:
            self.__connection = self.__engine.connect()
        return self.__connection

    def disconnect(self):
        if self.__connection is not None:
            self.__connection.close()
            self.__connection = None


class TableEntryHandler(metaclass=SingletonABCMeta):
    """This abstract class implements the basic form of interaction with a table that is defined."""
    _table_name: str

    def __init__(self, conn: Connection):
        self._conn = conn
        self._table = self.__reflect_table()

    def __reflect_table(self) -> db.schema.Table:
        return db.Table(self._table_name, db.MetaData(), autoload_with=self._conn)


class ObservationFactEntryHandler(TableEntryHandler):
    _table_name = 'observation_fact'

    def __init__(self, conn: Connection):
        super().__init__(conn)
        self.__script_id = os.environ['script_id']

    def update_entries(self, enc_num: int, pat_num: int, data: DiagnoseData):
        if self.__check_if_observation_exists(enc_num):
            self._update_table_entry(enc_num, pat_num, data)

    def __check_if_observation_exists(self, enc_num: int) -> bool:
        query = db.select(self._table.c.encounter_num).where(
            (self._table.c.encounter_num == enc_num))
        result = self._conn.execute(query).fetchone()
        return result is not None

    def _update_table_entry(self, enc_num: int, pat_num: int, entry: DiagnoseData):
        self._remove_observation_entry(enc_num)
        self._insert_observation_entry(enc_num, pat_num, entry)

    def _remove_observation_entry(self, enc_num: int):
        query = (
            db.delete(self._table)
            .where(
                (self._table.c.encounter_num == enc_num) &
                (self._table.c.sourcesystem_cd != os.environ['sourcesystem_cd'])
            )
        )
        try:
            self._conn.execute(query)
        except db.exc.SQLAlchemyError:
            self._conn.rollback()
            print(f'Update operation to {self._table} failed: {traceback.format_exc()}')

    def _insert_observation_entry(self, enc_num: int, pat_num: int, entry: DiagnoseData):
        import_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        sourcesystem_cd = os.environ['sourcesystem_cd']
        for diagnose in entry.get_diagnoses():
            query = (
                db.insert(self._table)
                .values(
                    encounter_num=enc_num,
                    patient_num=pat_num,
                    concept_cd=diagnose,
                    sourcesystem_cd=sourcesystem_cd,
                    import_date=import_date,
                    start_date=import_date,
                    provider_id='@',
                    modifier_cd='localisation'
                )
            )
            try:
                self._conn.execute(query)
                Logger().increase_db_insert()
            except db.exc.SQLAlchemyError:
                self._conn.rollback()
                print(f'Update operation to {self._table} failed: {traceback.format_exc()}')


class EncounterMappingEntryHandler(TableEntryHandler):
    _table_name = 'encounter_mapping'

    def __init__(self, conn: Connection):
        super().__init__(conn)

    def get_encounter_num_for_ide(self, enc_ide: str):
        query = db.select(self._table.c.encounter_num, self._table.c.encounter_ide).where(
            self._table.c.encounter_ide == enc_ide)
        result = self._conn.execute(query).fetchone()
        return result[0] if result else None


class PatientMappingEntryHandler(TableEntryHandler):
    _table_name = 'patient_mapping'

    def __init__(self, conn: Connection):
        super().__init__(conn)

    def get_patient_num_for_ide(self, pat_ide: str):
        query = db.select(self._table.c.patient_num).where(self._table.c.patient_ide == pat_ide)
        result = self._conn.execute(query).fetchone()
        return result[0] if result else None


if __name__ == '__main__':
    importer = AktinImporter()
    importer.import_csv(sys.argv[2])
