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
    This class contains diagnostic data of a patients treatment encounter in an emergency department.
    """

    def __init__(self):
        # The two IDE values identify updatable records and insert information from the other attributes into them.
        self.__pat_ide = None  # patient id from the csv encoded with sha1
        self.__enc_ide = None  # encounter id from the csv encoded with sha1
        self.__diagnoses = None  # string that contains a ';' seperated list of diagnoses
        self.__start_date_time = None  # date and timestamp of encounter start (admission)

    def is_valid(self):
        """
        Checks if this object is valid for updating the database. The object is valid if no attribute is 'None'.
        :return:
        """
        for attr_name, attr_value in self.__dict__.items():
            if attr_value is None:
                return False
        return True

    def __str__(self):
        summary = ''
        for attr_name, attr_value in self.__dict__.items():
            summary += f'{attr_name}: {attr_value}\n'
        return summary

    def get_pat_ide(self) -> str:
        return self.__pat_ide

    def get_enc_ide(self) -> str:
        return self.__enc_ide

    def get_diagnoses(self) -> str:
        return self.__diagnoses

    def set_pat_ide(self, pat_ide: str):
        self.__pat_ide = pat_ide

    def set_enc_ide(self, enc_ide: str):
        self.__enc_ide = enc_ide

    def set_diagnoses(self, diagnoses_raw: str):
        self.__diagnoses = diagnoses_raw.split('; ')

    def set_start_datetime(self, start_datetime: str):
        self.__start_date_time = start_datetime

    def get_start_datetime(self) -> datetime:
        return self.__start_date_time


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
    encounters in the csv file, -valid encounters, -encounters able to connect to the database, -imported diagnoses,
    -how many of which are updated and additional (number of diagnoses exceeding the number of diagnoses prior to update)
    """

    def __init__(self):
        self.__csv_encounters = 0
        self.__invalid_encounters = 0
        self.__successful_insert = 0
        self.__new_imported = 0
        self.__updated = 0

    def set_csv_encounters(self, csv_encounters: int):
        self.__csv_encounters = csv_encounters

    def increase_connected_to_db(self):
        self.__successful_insert += 1

    def get_num_of_csv_encounters(self) -> int:
        return self.__csv_encounters

    def get_successfully_inserted_encounters(self) -> int:
        return self.__successful_insert

    def increase_invalid_csv_encounter_count(self):
        self.__invalid_encounters += 1

    def get_invalid_csv_encounter_count(self) -> int:
        return self.__invalid_encounters

    def increase_new_imported_diagnoses(self, num: int = 1):
        self.__new_imported += num

    def get_new_imported(self):
        return self.__new_imported

    def increase_removed_diagnoses(self, num=1):
        self.__updated += num

    def get_updated(self):
        return self.__updated


class CSVReader(metaclass=SingletonMeta):
    """
    This class reads a csv file and functions as a python generator where each row can be yielded by the using method.
    """
    __seperator: str = ';'
    __encoding: str = 'latin_1'

    def __init__(self):
        self.__path_csv = None
        self.logger = Logger()

    def is_csv_file(self, file_path: str) -> tuple[str, bool]:
        _, file_extension = os.path.splitext(file_path)
        return file_extension, file_extension.lower() == '.csv'

    def set_csv_path(self, path_csv: str):
        file_type, is_csv = self.is_csv_file(path_csv)
        if is_csv:
            self.__path_csv = path_csv
        else:
            raise InvalidFileTypeError('Required CSV, got: ' + file_type)

    def iter_rows(self):
        row_count = 0
        for row in pd.read_csv(self.__path_csv, chunksize=1, sep=self.__seperator, encoding=self.__encoding, dtype=str):
            row_count += 1
            yield row
        self.logger.set_csv_encounters(row_count)


class InvalidFileTypeError(Exception):
    """
    Exception raised for files of a different type as requested/needed.
    """
    def __init__(self, message="Invalid file type provided"):
        self.message = message
        super().__init__(self.message)


class AktinImporter:
    """
    This class implements the pipeline for updating encounter data in the AKTIN Data Warehouse.
    The update result will be Logged at the end by :class:'Logger'.
    """
    def __init__(self):
        self.__reader = CSVReader()
        self.logger = Logger()
        self.__pipeline = self.__init_pipeline()

    @staticmethod
    def __init_pipeline():
        """
        This method updates the data class :class:'DiagnoseData' by using the specified column handlers.
        @:return _start_diagnoses: returns a DiagnoseData object that has been updated with diagnose data for one encounter.
        @:return None: returns None if one of the attributes of DiagnoseData were not found in the CSV file
        """
        _enc_id = EncounterIDHandler()
        _pat_id = PatientIDHandler(_enc_id)
        _date_time = StartDateTimeHandler(_pat_id)
        _diagnoses = EndICDHandler(_date_time)
        _start_diagnoses = StartICDHandler(_diagnoses)
        return _start_diagnoses

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
            if data.is_valid():
                enc_num = enc_map.get_encounter_num_for_ide(data.get_enc_ide())
                pat_num = pat_map.get_patient_num_for_ide(data.get_pat_ide())
                obs.update_entries_if_exist(enc_num, pat_num, data)
            else:
                self.logger.increase_invalid_csv_encounter_count()

        print('Fälle in Datei: ' + str(self.logger.get_num_of_csv_encounters()) + ',\n' +
              'Valide Fälle: ' + str(self.logger.get_num_of_csv_encounters() - self.logger.get_invalid_csv_encounter_count()) + ',\n' +
              'Verknüpfte Fälle: ' + str(self.logger.get_successfully_inserted_encounters()) + ',\n' +
              'Importierte Diagnosen: ' + str(self.logger.get_new_imported()) + ',\n' +
              'davon update: ' + str(self.logger.get_updated()) + ',\n' +
              'davon neu: ' + str(self.logger.get_new_imported() - self.logger.get_updated())
              )
        conn.disconnect()


class PatientDataColumnHandler(ABC, metaclass=SingletonABCMeta):
    """
    This abstract class is used to process columns from the csv. Each
    child class handles its specified column and updates the DiagnoseData object accordingly.
    """
    _column_name: str

    def __init__(self, successor: 'PatientDataColumnHandler' = None):
        self.__successor = successor
        self.helper = Helper()

    def update_pat_from_row(self, data: DiagnoseData, row: pd.Series, index: str) -> DiagnoseData:
        data = self._process_column(data, row)
        if self.__successor is not None:
            return self.__successor.update_pat_from_row(data, row, index)
        return data

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
        enc_ide = self.helper.anonymize_enc(val) if val is not None else None

        data.set_enc_ide(enc_ide)
        return data


class PatientIDHandler(PatientDataColumnHandler):
    _column_name = 'Patientennummer'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row)
        pat_ide = self.helper.anonymize_pat(val) if val is not None else None
        data.set_pat_ide(pat_ide)
        return data


class EndICDHandler(PatientDataColumnHandler):
    """
    This class supplements a DiagnoseData object with end diagnose from the row of the csv.
    This diagnose represents diagnose information given at the end of a treatment.
    Is necessary to differ between acute and stationary encounters in the csv. If the given row contains
    a start diagnose, the StartICDHandler class will overwrite the end diagnose with the start diagnose.
    """
    _column_name = 'Entlassdiagnosen'

    def _process_column(self, data: DiagnoseData, row: pd.Series) -> DiagnoseData:
        val = self._get_my_value_from_row(row)
        if val is not None:
            data.set_diagnoses(val)
        return data


class StartICDHandler(PatientDataColumnHandler):
    """
        This class supplements a DiagnoseData object with start diagnose from the row of the csv.
        This diagnose represents diagnose information given at the start of a treatment.
        Is necessary to differ between acute and stationary encounters in the csv.
        If there exists a start diagnose in the given row, this entry represents a acute case and this
        diagnose will overwrite the end diagnose in the DiagnoseData object.
        """
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
        try:
            _datetime = None if val is None else self.helper.convert_date_to_i2b2_format(val)
        except ValueError:
            _datetime = None

        if _datetime is not None:
            data.set_start_datetime(_datetime)
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

    def hash_filename(self) -> str:
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
        elif len(date) == 19:
            # Try the german date format and if not try the english format
            try:
                return datetime.strptime(date, '%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                return datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        else:
            raise ValueError()

    def anonymize_enc(self, ext: str) -> str:
        return self.__anonymize_id(self.__enc_root, ext)

    def anonymize_pat(self, ext: str) -> str:
        return self.__anonymize_id(self.__pat_root, ext)

    def __hash_composite(self, composite: str) -> str:
        """
        Anonymizes a composite string.

        @:param composite: The composite string to be anonymized.
        @:return The anonymized composite string.
        """
        buffer = composite.encode('UTF-8')
        alg = getattr(hashlib, self.__alg)()
        alg.update(buffer)
        return base64.urlsafe_b64encode(alg.digest()).decode('UTF-8')

    def __anonymize_id(self, root: str, ext: str) -> str:
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
        self.helper = Helper()
        self._table = self.__reflect_table()
        self.logger = Logger()

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
        self._sourcesystem_cd = ('gfi_' + os.environ['script_id']
                                 + 'V' + os.environ['script_version']
                                 + '_' + Helper().hash_filename()[:50])
        self._sourcesystem_sub = self._sourcesystem_cd.split('V')[0]

    def update_entries_if_exist(self, enc_num: int, pat_num: int, data: DiagnoseData):
        """
        Updates the database if the specified encounter in :param:'data' exists in the database
        :param enc_num
        :param pat_num
        :param data
        """

        if (enc_num is not None
                and pat_num is not None
                and self.__check_if_observation_exists(enc_num)):
            self.logger.increase_connected_to_db()
            self._update_table_entry(enc_num, pat_num, data)

    def __check_if_observation_exists(self, enc_num: int) -> bool:
        query = db.select(self._table.c.encounter_num).where(
            (self._table.c.encounter_num == enc_num))
        result = self._conn.execute(query).fetchone()
        return result is not None

    def _update_table_entry(self, enc_num: int, pat_num: int, entry: DiagnoseData):
        count_removed_diagnoses = self._remove_observation_entry(enc_num)
        self.logger.increase_removed_diagnoses(count_removed_diagnoses)

        count_inserted_diagnoses = self._insert_observation_entry(enc_num, pat_num, entry)
        self.logger.increase_new_imported_diagnoses(count_inserted_diagnoses)

    def _remove_observation_entry(self, enc_num: int) -> int:
        """
        Removes the observation fact entries from the database with matching
        encounter numbers :param:'enc_num' and different sourcesystem code.
        :param enc_num:
        """
        query = (
            db.delete(self._table)
            .where(
                (self._table.c.encounter_num == enc_num) &
                (self._table.c.sourcesystem_cd.like(f'{self._sourcesystem_sub}%'))
            ).returning(self._table.c.encounter_num)
        )

        try:
            result = self._conn.execute(query)
            deleted = len(result.fetchall())
            return deleted
        except db.exc.SQLAlchemyError:
            self._conn.rollback()
            print(f'Update operation to {self._table} failed: {traceback.format_exc()}')

    def _insert_observation_entry(self, enc_num: int, pat_num: int, entry: DiagnoseData) -> int:
        import_date = self.helper.convert_date_to_i2b2_format(str(datetime.now())[0:19])
        _imported_num = 0
        for diagnose in entry.get_diagnoses():
            query = (
                db.insert(self._table)
                .values(
                    encounter_num=enc_num,
                    patient_num=pat_num,
                    concept_cd=f'ICD10GM:{diagnose}',
                    sourcesystem_cd=self._sourcesystem_cd,
                    import_date=import_date,
                    start_date=entry.get_start_datetime(),
                    provider_id='@',
                    modifier_cd='@'
                )
            )
            try:
                self._conn.execute(query)
                _imported_num += 1

            except db.exc.SQLAlchemyError:
                self._conn.rollback()
                duplicate_entry_error_msg = 'psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint'
                if not (traceback.format_exc().__contains__(duplicate_entry_error_msg)):
                    print(f'Update operation to {self._table} failed: {traceback.format_exc()}')

        return _imported_num


class EncounterMappingEntryHandler(TableEntryHandler):
    _table_name = 'encounter_mapping'

    def __init__(self, conn: Connection):
        super().__init__(conn)

    def get_encounter_num_for_ide(self, enc_ide: str) -> int:
        query = db.select(self._table.c.encounter_num, self._table.c.encounter_ide).where(
            self._table.c.encounter_ide == enc_ide)
        result = self._conn.execute(query).fetchone()
        return result[0] if result else None


class PatientMappingEntryHandler(TableEntryHandler):
    _table_name = 'patient_mapping'

    def __init__(self, conn: Connection):
        super().__init__(conn)

    def get_patient_num_for_ide(self, pat_ide: str) -> int:
        query = db.select(self._table.c.patient_num).where(self._table.c.patient_ide == pat_ide)
        result = self._conn.execute(query).fetchone()
        return result[0] if result else None


if __name__ == '__main__':
    importer = AktinImporter()
    importer.import_csv(sys.argv[2])
