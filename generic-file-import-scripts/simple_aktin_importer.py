# -*- coding: utf-8 -*
# Created on Thu Dec 07 08:58:02 2023
# @VERSION=1.0.0
# @VIEWNAME=Simple-AKTIN-Importer
# @MIMETYPE=csv
# @ID=sai

#
#      Copyright (c) 2023 AKTIN
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

import base64
import hashlib
import os
import re
import sys
import traceback
from abc import ABC, ABCMeta, abstractmethod
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy as db


class PatientData:
    """
    Data structure representing a single patient record.
    """

    def __init__(self, index: str):
        self.__index = index
        self.__admission_date = None
        self.__pat_ide = None
        self.__enc_ide = None
        self.__birth_date = None
        self.__age = None
        self.__sex = None
        self.__assessment = None
        self.__assessment_date = None
        self.__first_contact = None

    def get_index(self):
        return self.__index

    def set_admission_date(self, admission_date):
        self.__admission_date = admission_date

    def get_admission_date(self):
        return self.__admission_date

    def set_pat_ide(self, pat_ide):
        self.__pat_ide = pat_ide

    def get_pat_ide(self):
        return self.__pat_ide

    def set_enc_ide(self, enc_ide):
        self.__enc_ide = enc_ide

    def get_enc_ide(self):
        return self.__enc_ide

    def set_birth_date(self, birth_date):
        self.__birth_date = birth_date

    def get_birth_date(self):
        return self.__birth_date

    def set_age(self, age):
        self.__age = age

    def get_age(self):
        return self.__age

    def set_sex(self, sex):
        self.__sex = sex

    def get_sex(self):
        return self.__sex

    def set_assessment(self, assessment):
        self.__assessment = assessment

    def get_assessment(self):
        return self.__assessment

    def set_assessment_date(self, assessment_date):
        self.__assessment_date = assessment_date

    def get_assessment_date(self):
        return self.__assessment_date

    def set_first_contact(self, first_contact):
        self.__first_contact = first_contact

    def get_first_contact(self):
        return self.__first_contact


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class SingletonABCMeta(ABCMeta):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonABCMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AktinImporter:
    """
    Orchestrates the patient data import process from CSV to i2b2 database.
    """

    def __init__(self):
        self.__reader = CSVReader()
        self.__pipeline = self.__init_pipeline()

    @staticmethod
    def __init_pipeline():
        """
        Constructs the processing pipeline of data handlers.

        Returns:
           PatientDataColumnHandler: The first handler in the pipeline chain.
        """
        first = FirstContactHandler()
        assess_date = PatientAssessmentDateHandler(first)
        assess = PatientAssessmentHandler(assess_date)
        sex = PatientSexHandler(assess)
        age = PatientAgeHandler(sex)
        birth = BirthDateHandler(age)
        pat = PatientIdHandler(birth)
        enc = EncounterIdHandler(pat)
        adm = AdmissionDateHandler(enc)
        return adm

    def import_csv(self, path_csv: str):
        """
        Imports patient data from CSV and inserts it into the i2b2 database.

        Args:
            path_csv (str): The path to the CSV file.
        """
        conn = DatabaseConnection()
        pat_map = PatientMappingEntryHandler(conn)
        pat_dim = PatientDimensionEntryHandler(conn)
        enc_map = EncounterMappingEntryHandler(conn)
        vis_dim = VisitDimensionEntryHandler(conn)
        obs = ObservationFactEntryHandler(conn)
        self.__reader.set_csv_path(path_csv)
        for index, r in enumerate(self.__reader.iter_rows()):
            data = PatientData(str(index + 2))  # skip header + 0 index
            data = self.__pipeline.update_pat_from_row(data, r)
            if data:
                pat_num, pat_ide = pat_map.create_new_entry(data)
                pat_dim.create_new_entry(pat_num, data)
                enc_num, enc_ide = enc_map.create_new_entry(pat_ide, data)
                vis_dim.create_new_entry(enc_num, pat_num, data)
                obs.create_new_entries(enc_num, pat_num, data)


class CSVReader(metaclass=SingletonMeta):
    """
    Handles reading and iterating through the CSV data.
    """
    __seperator: str = ','
    __encoding: str = 'utf-8'

    def __init__(self):
        self.__path_csv = None

    def set_csv_path(self, path_csv: str):
        self.__path_csv = path_csv

    def iter_rows(self):
        """
        Iterates through the CSV file, yielding each row as a pandas Series.

        Yields:
            pandas.Series: A single row of data from the CSV file.
        """
        for r in pd.read_csv(self.__path_csv, chunksize=1, sep=self.__seperator, encoding=self.__encoding, dtype=str):
            yield r


# if any value but ID is invalid -> drop complete patient
class PatientDataColumnHandler(ABC, metaclass=SingletonABCMeta):
    """
    Abstract base class for handling specific patient data columns.

    Attributes:
        _column_name (str): Name of the column this handler is responsible for.
    """
    _column_name: str

    def __init__(self, successor: 'PatientDataColumnHandler' = None):
        """
        Initializes the handler.

        Args:
            successor (PatientDataColumnHandler, optional): The next handler in the pipeline. Defaults to None.
        """
        self.__successor = successor
        self._helper = Helper()

    def update_pat_from_row(self, data: PatientData, row: pd.Series):
        """
        Processes the column for a patient record and updates the PatientData object.

        Args:
            data (PatientData): The PatientData object being updated.
            row (pd.Series): The row of data from the CSV.

        Returns:
            PatientData: The updated PatientData object, or None if the patient record is invalid.
        """
        try:
            data = self._process_column(data, row)
            if self.__successor is not None:
                return self.__successor.update_pat_from_row(data, row)
            return data
        except Exception:
            print(f'{self.__class__.__name__}: Invalid patient in row {data.get_index()}')
            return None

    @abstractmethod
    def _process_column(self, pat: PatientData, row: pd.Series) -> PatientData:
        """
        Abstract method to be implemented by subclasses. Handles the specific processing logic for a single data column.
        """
        pass

    def _get_my_value_from_row(self, row: pd.Series) -> str:
        """
        Helper method to retrieve the value from the current column.
        """
        return row[self._column_name].values[0]


class AdmissionDateHandler(PatientDataColumnHandler):
    _column_name = 'AUFNAHME_DATUM'

    def _process_column(self, data: PatientData, row: pd.Series) -> PatientData:
        val = self._get_my_value_from_row(row)
        date = self._helper.convert_date_to_i2b2_format(val)
        data.set_admission_date(date)
        return data


class EncounterIdHandler(PatientDataColumnHandler):
    _column_name = 'Fall'

    def _process_column(self, data: PatientData, row: pd.Series) -> PatientData:
        val = self._get_my_value_from_row(row)
        enc_ide = self._helper.anonymize_enc(val)
        data.set_enc_ide(enc_ide)
        return data


class PatientIdHandler(PatientDataColumnHandler):
    _column_name = 'Patient'

    def _process_column(self, data: PatientData, row: pd.Series) -> PatientData:
        val = self._get_my_value_from_row(row)
        pat_ide = self._helper.anonymize_pat(val)
        data.set_pat_ide(pat_ide)
        return data


class BirthDateHandler(PatientDataColumnHandler):
    _column_name = 'Geburtsdatum'

    def _process_column(self, data: PatientData, row: pd.Series) -> PatientData:
        val = self._get_my_value_from_row(row)
        birth_date = self._helper.convert_date_to_i2b2_format(val)
        data.set_birth_date(birth_date)
        return data


class PatientAgeHandler(PatientDataColumnHandler):
    _column_name = 'Alter des Patienten'

    def _process_column(self, data: PatientData, row: pd.Series) -> PatientData:
        """
        Extracts the patient's age, validates it, and updates the PatientData object.
        """
        val = self._get_my_value_from_row(row)
        age = int(val)
        if age > 0:
            data.set_age(age)
        else:
            raise ValueError()
        return data


class PatientSexHandler(PatientDataColumnHandler):
    _column_name = 'Geschlecht'
    __sex_map = {'M': 'M', 'W': 'F', 'U': 'X'}

    def _process_column(self, data: PatientData, row: pd.Series) -> PatientData:
        """
        Extracts the patient's sex, maps it to an i2b2 compatible value, and updates the PatientData object.
        """
        val = self._get_my_value_from_row(row)
        sex = self.__sex_map[val]
        data.set_sex(sex)
        return data


class PatientAssessmentHandler(PatientDataColumnHandler):
    _column_name = 'PTS'

    def _process_column(self, data: PatientData, row: pd.Series) -> PatientData:
        """
        Extracts the patient's assessment score, validates it, and updates the PatientData object.
        """
        val = self._get_my_value_from_row(row)
        if not val.isdigit() or not 1 <= int(val) <= 5:
            raise ValueError()
        data.set_assessment(val)
        return data


class PatientAssessmentDateHandler(PatientDataColumnHandler):
    _column_name = 'TIMES_PRIO'

    def _process_column(self, data: PatientData, row: pd.Series) -> PatientData:
        val = self._get_my_value_from_row(row)
        assessment_date = self._helper.convert_date_to_i2b2_format(val)
        data.set_assessment_date(assessment_date)
        return data


class FirstContactHandler(PatientDataColumnHandler):
    _column_name = 'ErstkontaktArzt'

    def _process_column(self, data: PatientData, row: pd.Series) -> PatientData:
        """
        Adds value of column as MM:SS to stored addmission timestamp of PatientData object as first contact timestamp, and updates the PatientData object.
        """
        waiting_time = self._get_my_value_from_row(row)
        minutes, seconds = map(int, waiting_time.split(':'))
        time_delta = timedelta(minutes=minutes, seconds=seconds)
        assessment_date = data.get_assessment_date()
        assessment_date = datetime.strptime(assessment_date, '%Y-%m-%d %H:%M')
        data.set_first_contact(assessment_date + time_delta)
        return data


class Helper(metaclass=SingletonMeta):
    """
    Provides utility functions for the data import process.
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
        Gets the configured hash algorithm name from aktin.properties (defaults to 'sha1') and converts it to python demanted format.

        Returns:
            str: The name of the hashing algorithm (e.g., 'sha1', 'md5').
        """
        name = self.__get_aktin_property('pseudonym.algorithm') or 'sha1'
        return str.lower(name.replace('-', '', ).replace('/', '_'))

    def __get_aktin_property(self, prop: str) -> str:
        """
        Retrieves a property value from the aktin.properties file.

        Args:
            prop (str): The name of the property to retrieve.

        Returns:
            str: The value of the property, or an empty string if not found.
        """
        with open(self.__path_properties) as properties:
            for line in properties:
                if '=' in line:
                    key, value = line.split('=', 1)
                    if key == prop:
                        return value.strip()
            return ''

    @staticmethod
    def convert_date_to_i2b2_format(date: str) -> str:
        """
        Converts a date string to i2b2 compatible format ('YYYY-MM-DD' or 'YYYY-MM-DD HH:MM').

        Args:
            date (str): The input date string.

        Returns:
            str: The date in i2b2 format.

        Raises:
            ValueError: If the input date format is not recognized.
        """
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
        """
        Anonymizes an encounter ID using a configured hashing algorithm and optional salt.
        """
        return self.__anonymize(self.__enc_root, ext)

    def anonymize_pat(self, ext) -> str:
        """
        Anonymizes a patient ID using a configured hashing algorithm and optional salt.
        """
        return self.__anonymize(self.__pat_root, ext)

    def __anonymize(self, root, ext) -> str:
        """
        Creates an anonymized identifier using a root value, extension, optional salt, and a configured hashing algorithm.

        Args:
            root (str): The base or preset value to use in the anonymization.
            ext (str): The extension to be combined with the root.

        Returns:
            str: The anonymized identifier, base64 encoded for URL safety.
        """
        composite = '/'.join([str(root), str(ext)])
        composite = self.__salt + composite if self.__salt else composite
        buffer = composite.encode('UTF-8')
        alg = getattr(hashlib, self.__alg)()
        alg.update(buffer)
        return base64.urlsafe_b64encode(alg.digest()).decode('UTF-8')


class DatabaseConnection(metaclass=SingletonMeta):
    """
    Manages connections to the i2b2 database.
    """

    def __init__(self):
        """
        Initializes the database connection using environment variables for credentials and connection URL.
        Parses the connection URL to isolate the database host. Creates a SQLAlchemy engine for interaction.
        """
        self.__username = os.environ['username']
        self.__password = os.environ['password']
        self.__i2b2_url = os.environ['connection-url']
        pattern = 'jdbc:postgresql://(.*?)(\?searchPath=..*)?$'
        url = re.search(pattern, self.__i2b2_url).group(1)
        self.__engine = db.create_engine(f'postgresql+psycopg2://{self.__username}:{self.__password}@{url}')

    def get_engine(self):
        """
        Returns the SQLAlchemy engine object.

        Returns:
            sqlalchemy.engine.Engine: The engine used for database interactions.
        """
        return self.__engine

    def get_connection(self):
        """
        Returns an active database connection.

        Returns:
            sqlalchemy.engine.Connection:  A connection to the database.
        """
        return self.__engine.connect()


class TableEntryHandler(metaclass=SingletonABCMeta):
    """
    Abstract base class for handling the insertion of data into specific i2b2 database tables.

    Attributes:
        _table_name (str): The name of the database table this handler manages.
        _conn (DatabaseConnection): A connection to the i2b2 database.
        __uuid (str): A unique identifier for the data source.
    """

    def __init__(self, conn: DatabaseConnection):
        self._conn = conn
        self._table = self.__reflect_table()
        self.__uuid = os.environ['uuid']

    def __reflect_table(self) -> db.schema.Table:
        """
        Loads the table schema from the database using SQLAlchemy.

        Returns:
            sqlalchemy.schema.Table: A SQLAlchemy Table object representing the database table.
        """
        return db.Table(self._table_name, db.MetaData(), autoload_with=self._conn.get_engine())

    def _add_meta_to_table_entry(self, entry: dict) -> dict:
        """
        Adds metadata fields to a table entry dictionary.

        Args:
            entry (dict): The dictionary representing a row of data to be inserted.

        Returns:
           dict: The updated entry dictionary with 'import_date' (current date) and 'sourcesystem_cd' (the uuid) fields.
        """
        import_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        entry.update({
            'import_date': import_date,
            'sourcesystem_cd': self.__uuid
        })
        return entry

    def _upload_table_entry(self, entry: dict):
        """
        Inserts a single table entry into the i2b2 database.

        Args:
            entry (dict): The dictionary representing a single row to be inserted.
        """
        with self._conn.get_connection() as conn:
            with conn.begin() as transaction:
                try:
                    conn.execute(self._table.insert(), [entry])
                except db.exc.SQLAlchemyError:
                    transaction.rollback()
                    print(f'Upload operation to {self._table} failed: {traceback.format_exc()}')


class PatientMappingEntryHandler(TableEntryHandler):
    """
    Manages the 'patient_mapping' table, linking pseudonymized patient IDs (pat_ide) to internal database IDs (patient_num).
    """
    _table_name = 'patient_mapping'

    def __init__(self, conn: DatabaseConnection):
        super().__init__(conn)
        self.__pat_num = self.__get_max_pat_num()

    def __get_max_pat_num(self):
        """
        Queries the database to find the highest existing 'patient_num' value.

        Returns:
            int: The maximum patient_num found, or 0 if the table is empty.
        """
        max_query = db.select(db.func.max(self._table.c.patient_num))
        with self._conn.get_connection() as conn:
            result = conn.execute(max_query).scalar()
        return result or 0

    def create_new_entry(self, data: PatientData) -> (int, str):
        """
        Creates a new patient mapping entry if necessary.

        Args:
            data (PatientData): The PatientData object containing patient information.

        Returns:
            tuple (int, str):  A tuple containing the assigned 'patient_num' and original 'pat_ide'.
        """
        pat_ide = data.get_pat_ide()
        pat_num = self.__get_patient_num_for_ide(pat_ide)
        if pat_num is None:
            self.__pat_num += 1
            pat_num = self.__pat_num
            self.__upload_patient_mapping_entry(pat_ide, pat_num)
        return pat_num, pat_ide

    def __get_patient_num_for_ide(self, pat_ide: str):
        """
        Checks if a 'patient_num' is already mapped to a given 'pat_ide'.

        Args:
            pat_ide (str): The pseudonymized patient identifier.

        Returns:
            int: The existing 'patient_num' if found, otherwise None.
        """
        query = db.select(self._table.c.patient_num).where(self._table.c.patient_ide == pat_ide)
        with self._conn.get_connection() as conn:
            result = conn.execute(query).fetchone()
        return result[0] if result else None

    def __upload_patient_mapping_entry(self, pat_ide: str, pat_num: int):
        """
        Inserts a new patient mapping record into the database.

        Args:
            pat_ide (str): The pseudonymized patient identifier.
            pat_num (int): The assigned internal patient number.
        """
        entry = {
            'patient_ide': pat_ide,
            'patient_ide_source': 'HIVE',
            'patient_num': pat_num,
            'project_id': 'AKTIN'
        }
        entry = self._add_meta_to_table_entry(entry)
        self._upload_table_entry(entry)


class PatientDimensionEntryHandler(TableEntryHandler):
    """
    Manages the 'patient_dimension' table, storing core patient demographic information.
    """
    _table_name = 'patient_dimension'

    def create_new_entry(self, pat_num: int, data: PatientData):
        """
        Inserts a new patient dimension record, but only if the patient_num doesn't already exist.

        Args:
            pat_num (int): The internal patient number.
            data (PatientData): The PatientData object containing patient demographics.
        """
        if not self.__check_if_pat_num_exists(pat_num):
            self.__upload_patient_dimension_entry(pat_num, data)

    def __check_if_pat_num_exists(self, pat_num: int) -> bool:
        """
        Checks if a record with the given patient_num exists in the 'patient_dimension' table.

        Args:
            pat_num (int): The internal patient number.

        Returns:
            bool: True if a record exists, False otherwise.
        """
        query = db.select(self._table.c.patient_num).where(self._table.c.patient_num == pat_num)
        with self._conn.get_connection() as conn:
            result = conn.execute(query).fetchone()
        return result is not None

    def __upload_patient_dimension_entry(self, pat_num: int, data: PatientData):
        """
        Creates a patient dimension entry and inserts it into the database.

        Args:
            pat_num (int): The internal patient number.
            data (PatientData):  The PatientData object containing patient demographics.
        """
        entry = {
            'patient_num': pat_num,
            'birth_date': data.get_birth_date(),
            'sex_cd': data.get_sex(),
            'age_in_years_num': data.get_age()
        }
        entry = self._add_meta_to_table_entry(entry)
        self._upload_table_entry(entry)


class EncounterMappingEntryHandler(TableEntryHandler):
    """
    Manages the 'encounter_mapping' table, linking pseudonymized encounter IDs (enc_ide) to internal database IDs (encounter_num).
    """
    _table_name = 'encounter_mapping'

    def __init__(self, conn: DatabaseConnection):
        super().__init__(conn)
        self.__enc_num = self.__get_max_enc_num()

    def __get_max_enc_num(self):
        """
        Queries the database to find the highest existing 'encounter_num' value.

        Returns:
            int: The maximum encounter_num found, or 0 if the table is empty.
        """
        max_query = db.select(db.func.max(self._table.c.encounter_num))
        with self._conn.get_connection() as conn:
            result = conn.execute(max_query).scalar()
        return result or 0

    def create_new_entry(self, pat_ide: str, data: PatientData) -> (int, str):
        """
        Creates a new encounter mapping entry if necessary.

        Args:
            pat_ide (str): The pseudonymized patient identifier.
            data (PatientData): The PatientData object containing encounter information.

        Returns:
            tuple (int, str): A tuple containing the assigned 'encounter_num' and original 'enc_ide'.
        """
        enc_ide = data.get_enc_ide()
        enc_num = self.__get_encounter_num_for_ide(enc_ide)
        if enc_num is None:
            self.__enc_num += 1
            enc_num = self.__enc_num
            self.__upload_patient_mapping_entry(enc_ide, enc_num, pat_ide)
        return enc_num, enc_ide

    def __get_encounter_num_for_ide(self, enc_ide: str):
        """
        Checks if an 'encounter_num' is already mapped to a given 'enc_ide'.

        Args:
            enc_ide (str): The pseudonymized encounter identifier.

        Returns:
            int: The existing 'encounter_num' if found, otherwise None.
        """
        query = db.select(self._table.c.encounter_num).where(self._table.c.encounter_ide == enc_ide)
        with self._conn.get_connection() as conn:
            result = conn.execute(query).fetchone()
        return result[0] if result else None

    def __upload_patient_mapping_entry(self, enc_ide: str, enc_num: int, pat_ide: str):
        """
        Inserts a new encounter mapping record into the database.

        Args:
            enc_ide (str): The pseudonymized encounter identifier.
            enc_num (int):  The assigned internal encounter number.
            pat_ide (str): The pseudonymized patient identifier.
        """
        entry = {
            'encounter_ide': enc_ide,
            'encounter_ide_source': 'HIVE',
            'encounter_num': enc_num,
            'patient_ide': pat_ide,
            'patient_ide_source': 'HIVE',
            'project_id': 'AKTIN'
        }
        entry = self._add_meta_to_table_entry(entry)
        self._upload_table_entry(entry)


class VisitDimensionEntryHandler(TableEntryHandler):
    """
    Manages the 'visit_dimension' table, storing core information about patient visits/encounters.
    """
    _table_name = 'visit_dimension'

    def create_new_entry(self, enc_num: int, pat_num: int, data: PatientData):
        """
        Inserts a new visit dimension record, but only if the encounter_num doesn't already exist.

        Args:
            enc_num (int): The internal encounter number.
            pat_num (int): The internal patient number.
            data (PatientData): The PatientData object containing visit information.
        """
        if not self.__check_if_enc_num_exists(enc_num):
            self.__upload_patient_dimension_entry(enc_num, pat_num, data)

    def __check_if_enc_num_exists(self, enc_num: int) -> bool:
        """
        Checks if a record with the given encounter_num exists in the 'visit_dimension' table.

        Args:
            enc_num (int): The internal encounter number.

        Returns:
            bool: True if a record exists, False otherwise.
        """
        query = db.select(self._table.c.encounter_num).where(self._table.c.encounter_num == enc_num)
        with self._conn.get_connection() as conn:
            result = conn.execute(query).fetchone()
        return result is not None

    def __upload_patient_dimension_entry(self, enc_num: int, pat_num: int, data: PatientData):
        """
        Creates a visit dimension entry and inserts it into the database.

        Args:
            enc_num (int): The internal encounter number.
            pat_num (int): The internal patient number.
            data (PatientData): The PatientData object containing visit information.
        """
        entry = {
            'encounter_num': enc_num,
            'patient_num': pat_num,
            'start_date': data.get_admission_date()
        }
        entry = self._add_meta_to_table_entry(entry)
        self._upload_table_entry(entry)


class ObservationFactEntryHandler(TableEntryHandler):
    """
    Manages the 'observation_fact' table, storing patient observations and assessments.
    """
    _table_name = 'observation_fact'

    def __init__(self, conn: DatabaseConnection):
        """
        Initializes the handler and gets the script ID from environment variables.

        Args:
            conn (DatabaseConnection): A database connection object.
        """
        super().__init__(conn)
        self.__script_id = os.environ['script_id']

    def create_new_entries(self, enc_num: int, pat_num: int, data: PatientData):
        """
        Creates assessment and physical encounter entries in the 'observation_fact' table (if the encounter doesn't exist yet).

        Args:
            enc_num (int): The internal encounter number.
            pat_num (int): The internal patient number.
            data (PatientData): The PatientData object.
        """
        if not self.__check_if_enc_num_exists(enc_num):
            self.__upload_assessment_entry(enc_num, pat_num, data)
            self.__upload_physencounter_entry(enc_num, pat_num, data)

    def __check_if_enc_num_exists(self, enc_num: int) -> bool:
        """
        Checks if an 'observation_fact' record exists for the given encounter number.

        Args:
            enc_num (int): The internal encounter number.

        Returns:
            bool: True if a record exists, False otherwise.
        """
        query = db.select(self._table.c.encounter_num).where(self._table.c.encounter_num == enc_num)
        with self._conn.get_connection() as conn:
            result = conn.execute(query).fetchone()
        return result is not None

    def __upload_assessment_entry(self, enc_num: int, pat_num: int, data: PatientData):
        """
        Creates an assessment observation entry and inserts it into the database.

        Args:
            enc_num (int): The internal encounter number.
            pat_num (int): The internal patient number.
            data (PatientData): The PatientData object.
        """
        entry = {
            'encounter_num': enc_num,
            'patient_num': pat_num,
            'concept_cd': ':'.join(['ESI', data.get_assessment()]),
            'start_date': data.get_assessment_date()
        }
        entry = self.__add_obs_meta_to_table_entry(entry)
        entry = self._add_meta_to_table_entry(entry)
        self._upload_table_entry(entry)

    def __upload_physencounter_entry(self, enc_num: int, pat_num: int, data: PatientData):
        """
        Creates a physical encounter observation entry and inserts it into the database.

        Args:
            enc_num (int): The internal encounter number.
            pat_num (int): The internal patient number.
            data (PatientData): The PatientData object.
        """
        entry = {
            'encounter_num': enc_num,
            'patient_num': pat_num,
            'concept_cd': 'AKTIN:PHYSENCOUNTER',
            'start_date': data.get_first_contact()
        }
        entry = self.__add_obs_meta_to_table_entry(entry)
        entry = self._add_meta_to_table_entry(entry)
        self._upload_table_entry(entry)

    def __add_obs_meta_to_table_entry(self, entry: dict) -> dict:
        """Adds metadata fields required for 'observation_fact' entries.

        Args:
            entry (dict): The observation fact entry dictionary.

        Returns:
            dict: The updated entry dictionary with metadata fields.
        """
        entry.update({
            'provider_id': self.__script_id,
            'modifier_cd': '@',
            'instance_num': 1
        })
        return entry


if __name__ == '__main__':
    importer = AktinImporter()
    importer.import_csv(sys.argv[2])
