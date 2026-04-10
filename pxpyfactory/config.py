# Code partly AI generated
# Many not in use, but kept for potential future use
"""
Configuration settings for pxPyFactory
Centralizes all configuration values, paths, and constants
"""
from dataclasses import dataclass
from typing import List


# ============================================================================
# CLOUD & EXTERNAL SERVICES
# ============================================================================

@dataclass
class GoogleCloudStorage:
    """Google Cloud Storage configuration"""
    # Input bucket - CSV files, logs, work files (private)
    BUCKET_INPUT = "pxpyfactory-input" #in_use#
    
    # Output bucket - Generated PX files (shared with API)
    BUCKET_OUTPUT = "pxpyfactory-output" # "pxweb-api-input" #in_use#


@dataclass
class GitHubConfig:
    """GitHub deployment configuration"""
    OWNER = "navikt" #in_use#
    REPO = "pxweb-api" #in_use#
    WORKFLOW_FILE = "deploy.yml" #in_use#
    DEFAULT_ENVIRONMENT = "dev" #in_use#
    DEFAULT_BRANCH = "main" #in_use#
    API_VERSION = "2022-11-28" #in_use#
    ENV_VAR_TOKEN = "GITHUB_TOKEN_PX" #in_use#
    ACCEPT_HEADER = "application/vnd.github+json" #in_use#
    SUCCESS_STATUS_CODE = 204 #in_use#


# ============================================================================
# PATHS & FILE STRUCTURE
# ============================================================================

@dataclass
class Paths: #in_use#
    """File and directory paths"""
    INPUT = "stats" #in_use#
    OUTPUT = "px" #in_use#
    SAVED_QUERY_OUTPUT = "sq" #in_use#
    COMMON_METADATA_FILE = "common_meta.xlsx" #in_use#
    PRODUCTION_LOG_FILE = "log/production_log.jsonl" #in_use#


@dataclass
class FileFormats:
    """File extensions and formats"""
    PX = ".px"
    CSV = ".csv"
    PARQUET = ".parquet"
    XLSX = ".xlsx"
    SQA = ".sqa"
    SQS = ".sqs"
    JSONL = ".jsonl"
    TXT = ".txt"


# ============================================================================
# INPUT DATA STRUCTURE
# ============================================================================

@dataclass
class ExcelSheetNames:
    """Sheet names in the common metadata Excel file"""
    DATA_PRODUCTS = "dataprodukter"
    METADATA_DEFAULT = "metadata-default"
    METADATA_MANUAL = "metadata-manual"
    FOLDER_ALIAS = "folder-alias"


@dataclass
class DataProductColumns:
    """Column names in the data products sheet"""
    BUILD = "BUILD"
    SUBJECT_CODE = "SUBJECT-CODE"
    SUBJECT_AREA = "SUBJECT-AREA"
    SUBJECT = "SUBJECT"
    TABLEID = "TABLEID"
    TABLEID_RAW = "TABLEID_RAW"
    TITLE = "TITLE"
    CONTENTS = "CONTENTS"
    STUB = "STUB"
    HEADING = "HEADING"
    DATA = "DATA"
    UNITS = "UNITS"
    TIMEVAL = "TIMEVAL"
    FORCE_BUILD = "FORCE_BUILD"


@dataclass
class MetadataColumns:
    """Column names in metadata dataframes"""
    TYPE = "TYPE"
    KEYWORD = "KEYWORD"
    VALUE = "VALUE"
    ORDER = "ORDER"
    MANDATORY = "MANDATORY"
    DEFAULT_VALUE = "DEFAULT_VALUE"
    MANUAL_VALUE = "MANUAL_VALUE"
    SPESIFIC_VALUE = "SPESIFIC_VALUE"


@dataclass
class MetadataTypes:
    """Metadata type identifiers"""
    PX = "PX"
    SQ = "SQ"
    RENAME = "RENAME"
    TEXT = "text"
    INTEGER = "integer"
    DATA = "data"


# ============================================================================
# LOGGING
# ============================================================================

@dataclass
class LogColumns:
    """Column names in log entries"""
    TIMESTAMP = "timestamp"
    TYPE = "type"
    TABLEID = "tableid"
    HASHED_PARAMS = "hashed_params"
    SIZE = "size"
    TIME = "time"
    META_SIZE = "meta_size"
    META_TIME = "meta_time"


@dataclass
class LogTypes:
    """Log entry type identifiers used in production log"""
    SUMMARY = "summary" #in_use#
    TABLE = "table" #in_use#


# ============================================================================
# DEFAULTS & CONSTANTS
# ============================================================================

@dataclass
class Defaults:
    """Default values"""
    CSV_SEPARATOR = ";"
    EXCEL_SHEET_NAME = "Ark1"
    HEADER_ROW = 0
    CONTVARIABLE_NAME = "STAT_VAR"
    TABLEID_MAX_LENGTH = 20
    MAX_SQ_CELLS = 500000  # Maximum cells viewable in pxWeb2 #in_use#


@dataclass
class TimeFormats:
    """Time and date formats"""
    TIMESTAMP_FORMAT = "%Y%m%d %H:%M"
    TIMESTAMP_WITH_SECONDS = "%Y-%m-%d %H:%M:%S"
    YEAR_MIN = 2020
    YEAR_MAX = 2099


@dataclass
class TimeListCodes:
    """TIMEVAL TLIST codes for different time periods"""
    YEARLY = "A1"
    QUARTERLY = "Q1"
    MONTHLY = "M1"
    WEEKLY = "W1"


@dataclass
class AliasConfig:
    """Alias file configuration"""
    LANGUAGES = ["no", "en"]
    PREFIX = "alias_"
    COLUMNS = ["CODE", "NO", "EN"]


@dataclass
class ValidationStrings:
    """Strings considered as invalid/empty values"""
    NONE_STRINGS = ["none", "null", "nan", "nat"]
    PLACEHOLDER_CHARS = ["-", ".", ".."]


# ============================================================================
# PX FILE CONFIGURATION
# ============================================================================

@dataclass
class PXKeywords:
    """Common PX file keywords"""
    TABLEID = "TABLEID"
    MATRIX = "MATRIX"
    TITLE = "TITLE"
    STUB = "STUB"
    HEADING = "HEADING"
    CONTVARIABLE = "CONTVARIABLE"
    UNITS = "UNITS"
    SUBJECT_CODE = "SUBJECT-CODE"
    SUBJECT_AREA = "SUBJECT-AREA"
    CONTENTS = "CONTENTS"
    VALUES = "VALUES"
    TIMEVAL = "TIMEVAL"
    TLIST = "TLIST"
    LAST_UPDATED = "LAST-UPDATED"
    DATASYMBOL2 = "DATASYMBOL2"
    CONTACT = "CONTACT"
    PRECISION = "PRECISION"

# ============================================================================
# SINGLETON INSTANCES
# ============================================================================

# Cloud & External
gcs = GoogleCloudStorage()
github = GitHubConfig()

# Paths & Files
paths = Paths()
formats = FileFormats()

# Input Data
sheets = ExcelSheetNames()
dp_cols = DataProductColumns()
meta_cols = MetadataColumns()
meta_types = MetadataTypes()

# Logging
log_cols = LogColumns()
log_types = LogTypes()

# Defaults & Constants
defaults = Defaults()
time_formats = TimeFormats()
tlist = TimeListCodes()
alias = AliasConfig()
validation = ValidationStrings()

# PX Configuration
px_keywords = PXKeywords()
