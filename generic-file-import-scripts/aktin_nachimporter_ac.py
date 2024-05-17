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
    This class contains diagnostic data for updating the database.
    """

    def __init__(self):
        self.__pat_ide = None
        self.__enc_ide = None
        self.__diagnoses = None
        self.__startdatetime = None

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
        self.__diagnoses = diagnoses.split('; ')

    def set_startdatetime(self, __startdatetime):
        self.__startdatetime = __startdatetime

    def get_startdate(self):
        return self.__startdatetime


class SingletonMeta(type):
    """
    Metaclass that implements the Singleton design pattern.
    This metaclass ensures that only one instance of any class that uses it is created.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class SingletonABCMeta(ABCMeta):
    """
    Metaclass that implements the Singleton design pattern for Abstract Base Classes (ABCs).
    This metaclass ensures that only one instance of each ABC that uses it is created.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonABCMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Logger(metaclass=SingletonMeta):
    """
    This class provides logging for displaying the status of the update process. It displays the number of
    encounters in the csv file, counts the number of updated encounter diagnoses, and counts the number of non-updatable
    diagnoses because the encounter number doesn't exist in the database.
    """
    def __init__(self):
        self.csv_encounters = 0
        self.invalid_cases = 0
        self.successful_insert = 0
        self.new_imported = 0
        self.updated = 0

    def set_csv_encounters(self, csv_encounters):
        self.csv_encounters = csv_encounters

    def increase_db_insert(self):
        self.successful_insert += 1

    def get_csv_encounters(self):
        return self.csv_encounters

    def get_db_insert(self):
        return self.successful_insert

    def increase_invalid(self):
        self.invalid_cases += 1

    def get_invalid_cases(self):
        return self.invalid_cases

    def increase_new_imported(self, num=1):
        self.new_imported += num

    def decrease_new_imported(self):
        self.new_imported -= 1

    def get_new_imported(self):
        return self.new_imported

    def increase_updated(self, num=1):
        self.updated += num

    def get_updated(self):
        return self.updated


class CSVReader(metaclass=SingletonMeta):

    __seperator: str = ';'
    __encoding: str = 'latin_1'

    def __init__(self):
        self.__path_csv = None
        self.__logger = Logger()

    def is_csv_file(self, file_path):
        _, file_extension = os.path.splitext(file_path)
        return file_extension, file_extension.lower() == '.csv'

    def set_csv_path(self, path_csv: str):
        file_type, is_csv = self.is_csv_file(path_csv)
        if is_csv:
            self.__path_csv = path_csv
        else:
            raise Exception('Required CSV, got: ' + file_type)

    def iter_rows(self):
        row_count = 0
        for row in pd.read_csv(self.__path_csv, chunksize=1, sep=self.__seperator, encoding=self.__encoding, dtype=str):
            row_count += 1
            yield row
        self.__logger.set_csv_encounters(row_count)


class AktinImporter:
    """
    This class implements the pipline for updating encounter data in the AKTIN Data Warehouse.
    The update result will be Logged at the end by :class:'Logger'.
    """

    def __init__(self):
        self.__reader = CSVReader()
        self.__logger = Logger()
        self.__pipeline = self.__init_pipeline()
        os.environ['sourcesystem_cd'] = ('gfi_' + os.environ['script_id']
                                         + 'V' + os.environ['script_version']
                                         + '_' + Helper().hash_filename()[:50])

    @staticmethod
    def __init_pipeline():
        """
        This method updates the data class :class:'DiagnoseData' by using the specified column handlers.
        @:return
        """
        eid = EncounterIDHandler()
        pid = PatientIDHandler(eid)
        sdtid = StartDateTimeHandler(pid)
        icd = ICDHandler(sdtid)
        sicd = ICDStartHandler(icd)
        return sicd

    def import_csv(self, path_csv: str):
        """
        This method reads data from a CSV file located at the specified path and updates the database on that basis.
        It utilizes various handlers to interact with the tables of the database.
        @:param path_csv: Path to the CSV file
        """
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
            else:
                self.__logger.increase_invalid()

        conn.disconnect()
        print('Encounters: ' + str(self.__logger.get_csv_encounters()) + ', ' +
              'Valid: ' + str(self.__logger.get_csv_encounters() - self.__logger.get_invalid_cases()) + ', ' +
              'Inserted Diagnoses: ' + str(self.__logger.get_db_insert()) + ', ' +
              'Updated: ' + str(self.__logger.get_updated()) + ', ' +
              'New imported: ' + str(self.__logger.get_new_imported())
              )


class PatientDataColumnHandler(ABC, metaclass=SingletonABCMeta):
    """
    This abstract class is used to process columns from the csv. Each
    child class handles its specified column and updates the DiagnoseData object accordingly.
    """
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
            print(f'{self.__class__.__name__}: Invalid patient in row {index}, ' + self.__successor._column_name)
            return None

    @abstractmethod
    def _process_column(self, pat: DiagnoseData, row: pd.Series) -> DiagnoseData:
        pass

    def _get_my_value_from_row(self, row: pd.Series) -> str:
        val = row[self._column_name].values[0]
        if pd.isna(val):
            val = None
        return val


class EncounterIDHandler(PatientDataColumnHandler):
    _column_name = 'Aufnahmenummer'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row)
        if val is None:
            raise ValueError(f'{self.__class__.__name__}')
        enc_ide = self._helper.anonymize_enc(val)
        data.set_enc_ide(enc_ide)
        return data


class PatientIDHandler(PatientDataColumnHandler):
    _column_name = 'Patientennummer'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row)
        if val is None:
            raise ValueError(f'{self.__class__.__name__}')
        pat_ide = self._helper.anonymize_pat(val)
        data.set_pat_ide(pat_ide)
        return data


class ICDHandler(PatientDataColumnHandler):
    _column_name = 'Entlassdiagnosen'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row)
        if val is None:
            raise ValueError(f'{self.__class__.__name__}')
        data.set_diagnoses(val)
        return data


class ICDStartHandler(PatientDataColumnHandler):
    _column_name = 'Aufnahmediagnosen'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row)
        if val is not None:
            data.set_diagnoses(val)
        return data


class StartDateTimeHandler(PatientDataColumnHandler):
    _column_name = 'Aufnahmedatumuhrzeit'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row)
        if val is None:
            raise ValueError(f'{self.__class__.__name__}')
        data.set_startdatetime(val)
        return data


class Helper(metaclass=SingletonMeta):
    """
    A utility class for various helper functions.
    This class provides methods to hash filenames, convert dates to i2b2 format and
    anonymizing patient and encounter identifiers.
    """

    def __init__(self):
        self.__path_properties = os.environ['path_aktin_properties']
        if not os.path.exists(self.__path_properties):
            raise SystemExit('file path for aktin.properties is not valid')
        self.__alg = self.__get_hash_algorithm_name()
        self.__salt = self.__get_aktin_property('pseudonym.salt')
        self.__pat_root = self.__get_aktin_property('cda.patient.root.preset')
        self.__enc_root = self.__get_aktin_property('cda.encounter.root.preset')

    def __get_hash_algorithm_name(self) -> str:
        """
        Retrieves the hash algorithm name from the aktin properties file.
        Uses SHA1 if no algorith is found at 'pseudonym.algorithm'.
        """
        name = self.__get_aktin_property('pseudonym.algorithm') or 'sha1'
        return str.lower(name.replace('-', '', ).replace('/', '_'))

    def __get_aktin_property(self, prop: str) -> str:
        """
        Retrieves a property value from aktin.properties.
        :param prop:  The property key
        :return: The property value
        """
        with open(self.__path_properties) as properties:
            for line in properties:
                if '=' in line:
                    key, value = line.split('=', 1)
                    if key == prop:
                        return value.strip()
            return ''

    def hash_filename(self):
        filename = os.path.basename(__file__)
        return self.__hash_composite(filename)

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
        return self.__anonymize_id(self.__enc_root, ext)

    def anonymize_pat(self, ext) -> str:
        return self.__anonymize_id(self.__pat_root, ext)

    def __hash_composite(self, composite: str):
        """
        Anonymizes a composite string.

        @:param composite: The composite string to be anonymized.
        @:return The anonymized composite string.
        """
        buffer = composite.encode('UTF-8')
        alg = getattr(hashlib, self.__alg)()
        alg.update(buffer)
        return base64.urlsafe_b64encode(alg.digest()).decode('UTF-8')

    def __anonymize_id(self, root, ext) -> str:
        composite = '/'.join([str(root), str(ext)])
        composite = self.__salt + composite if self.__salt else composite
        return self.__hash_composite(composite)


class DatabaseConnection(metaclass=SingletonMeta):
    """
    A singleton class for managing database connections.

    This class handles the connection to the PostgreSQL database using the provided
    environment variables for username, password, and connection URL. It stores a connection and keeps
    it open until it is closed manually.
    """
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
    """
    This abstract class manages the necessary functionality to enable interaction with a table in the database.
    """
    _table_name: str

    def __init__(self, conn: Connection):
        self._conn = conn
        self._table = self.__reflect_table()
        self.__logger = Logger()

    def __reflect_table(self) -> db.schema.Table:
        return db.Table(self._table_name, db.MetaData(), autoload_with=self._conn)


class ObservationFactEntryHandler(TableEntryHandler):
    """
    This class inherits from TableEntryHandler and provides methods to update
    and insert observation fact entries in the database table.
    """
    _table_name = 'observation_fact'

    def __init__(self, conn: Connection):
        super().__init__(conn)
        self.__logger = Logger()
        self.__script_id = os.environ['script_id']
        os.environ['sourcesystem_cd'] = ('gfi_' + os.environ['script_id']
                           + 'V' + os.environ['script_version']
                           + '_' + Helper().hash_filename()[:50])

    def update_entries(self, enc_num: int, pat_num: int, data: DiagnoseData):
        """
        Updates the database if the specified encounter in :param:'data' exists in the database
        :param enc_num
        :param pat_num
        :param data
        """
        if self.__check_if_observation_exists(enc_num):
            self._update_table_entry(enc_num, pat_num, data)

    def __check_if_observation_exists(self, enc_num: int) -> bool:
        query = db.select(self._table.c.encounter_num).where(
            (self._table.c.encounter_num == enc_num))
        result = self._conn.execute(query).fetchone()
        return result is not None

    def _update_table_entry(self, enc_num: int, pat_num: int, entry: DiagnoseData):
        removed_num = self._remove_observation_entry(enc_num)
        inserted_num = self._insert_observation_entry(enc_num, pat_num, entry)
        self.__logger.increase_updated(removed_num)
        if removed_num < inserted_num:
            self.__logger.increase_new_imported(inserted_num-removed_num)

    def _remove_observation_entry(self, enc_num: int):
        """
        Removes the observation fact entries from the database with matching
        encounter numbers :param:'enc_num' and different sourcesystem code.
        :param enc_num:
        """
        query = (
            db.delete(self._table)
            .where(
                (self._table.c.encounter_num == enc_num) &
                (self._table.c.sourcesystem_cd != os.environ['sourcesystem_cd'])
            ).returning(self._table.c.encounter_num)
        )

        try:
            result = self._conn.execute(query)
            return len(result.fetchall())
        except db.exc.SQLAlchemyError:
            self._conn.rollback()
            print(f'Update operation to {self._table} failed: {traceback.format_exc()}')

    def _insert_observation_entry(self, enc_num: int, pat_num: int, entry: DiagnoseData):
        import_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        start_date = datetime.strptime(entry.get_startdate(), '%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S.%f')
        _imported_num = 0
        for diagnose in entry.get_diagnoses():
            query = (
                db.insert(self._table)
                .values(
                    encounter_num=enc_num,
                    patient_num=pat_num,
                    concept_cd=diagnose,
                    sourcesystem_cd=os.environ['sourcesystem_cd'],
                    import_date=import_date,
                    start_date=start_date,
                    provider_id='@',
                    modifier_cd='localisation'
                )
            )
            try:
                self._conn.execute(query)
                self.__logger.increase_db_insert()
                _imported_num += 1
            except db.exc.SQLAlchemyError:
                # self._conn.rollback()
                print(f'Update operation to {self._table} failed: {traceback.format_exc()}')
        return _imported_num


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
