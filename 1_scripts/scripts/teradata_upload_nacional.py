"""
_summary_

"""

import pathlib
import os
import sys
import logging
import re
from typing import List
from datetime import datetime
from collections import OrderedDict
from dotenv import load_dotenv  # type: ignore
import duckdb  # type: ignore
import pandas  # type: ignore
from teradataml import (  # type: ignore
    create_context,
    fastload,
    execute_sql,
    TIMESTAMP,
    VARCHAR,
    NUMBER,
    INTEGER,
    CHAR,
    TeradataMlException,
)


class ColorFormatter(logging.Formatter):
    """Formatador de log que adiciona cores apenas para saídas no console."""

    # Cores ANSI
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    blue = "\x1b[34;20m"
    reset = "\x1b[0m"

    fmt = "%(asctime)s - %(levelname)s - %(message)s"

    FORMATS = {
        logging.DEBUG: grey + fmt + reset,
        logging.INFO: blue + fmt + reset,
        logging.WARNING: yellow + fmt + reset,
        logging.ERROR: red + fmt + reset,
        logging.CRITICAL: bold_red + fmt + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


def setup_logging(job_name: str) -> logging.Logger:
    log_path = pathlib.Path("logs")
    log_path.mkdir(exist_ok=True)
    log_filename = (
        log_path / f"log_{job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColorFormatter())
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def add_audit_columns(
    direction: str,
    source_system: str,
    job_name: str,
    job_name_abbr: str,
    teradata_user: str,
    dataframe: pandas.DataFrame,
    file: str,
    logger: logging.Logger,
) -> pandas.DataFrame:
    """_summary_

    Args:
        dataframe (pandas.DataFrame): _description_
        file (str): _description_

    Returns:
        pandas.DataFrame: _description_
    """
    logger.info("Adicionando colunas de auditoria ao arquivo")
    dataframe["DIRECTION"] = direction
    dataframe["SOURCE_FILE_NAME"] = file[file.rfind("\\") + 1 :]
    dataframe["SOURCE_SYSTEM"] = source_system
    dataframe["JOB_NAME"] = job_name
    dataframe["ETL_BATCH_ID"] = f"{datetime.today():%d_%m_%Y}_{job_name_abbr}"
    dataframe["ROW_CREATE_TS"] = datetime.today()
    dataframe["ROW_CREATE_USER"] = teradata_user.capitalize()
    return dataframe


def is_valid_table_name(name: str) -> bool:
    # Define o padrão: Começa com letra, seguido por letras, números ou underscores
    # Comprimento entre 1 e 128 caracteres
    pattern = r"^[a-zA-Z][a-zA-Z0-9_]{0,127}$"
    return bool(re.match(pattern, name))


def sanitize_numeric_col(
    df: pandas.DataFrame, columns: List[str], is_integer: bool = False
) -> pandas.DataFrame:
    """
    Sanitiza colunas numéricas removendo ruídos de formatação e tratando NaNs.
    """
    for col in columns:
        if col not in df.columns:
            continue

        # Converte para string para tratar formatação de milhar/decimal brasileira
        df[col] = df[col].astype(str).str.replace(".", "").str.replace(",", ".")
        df[col] = pandas.to_numeric(df[col], errors="coerce").fillna(0)

        if is_integer:
            df[col] = df[col].astype(int)
        else:
            df[col] = df[col].astype(float).round(5)

    return df


def sanitize_str_col(df: pandas.DataFrame, columns: List[str]) -> pandas.DataFrame:
    """_summary_

    Args:
        df (pandas.DataFrame): _description_
        columns (List[str]): _description_

    Returns:
        pandas.DataFrame: _description_
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("NA")
            df[col] = df[col].astype("object")
    return df


def sanitize_date_col(df: pandas.DataFrame, date_cols: List[str]) -> pandas.DataFrame:
    for col in date_cols:
        if col in df.columns:
            df[col] = pandas.to_datetime(df[col])
    return df


def ensure_staging_table(table_name: str, logger: logging.Logger) -> None:
    if not is_valid_table_name(table_name):
        raise ValueError(f"Nome de tabela ilegal detectado: {table_name}")

    logger.info("Criando tabela %s se não existir", table_name)
    execute_sql("SET QUERY_BAND = 'name=INT_ROAMING;' FOR SESSION;")

    try:
        execute_sql(
            f"""
                    CREATE MULTISET TABLE U_INT_ATACADO.{table_name}(
                        -- COLUNAS CARREGADAS DA DCH
                        TAP_FILE_CURRENT_PROCESSING_DATE TIMESTAMP,
                        DATE_CALL TIMESTAMP,
                        PMN_SETTLEMENT_TADIG_CODE VARCHAR(8000),
                        CALL_TYPE VARCHAR(8000),
                        IMSI VARCHAR(8000),
                        MSISDN VARCHAR(8000),
                        APN_NETWORK VARCHAR(8000),
                        DEVICE_TAC_CODE VARCHAR(8000),
                        NUMBER_OF_CALLS INTEGER,
                        CHARGED_SMS INTEGER,
                        CHARGED_MINUTES NUMBER(15,5),
                        CHARGED_MB NUMBER(15,5),
                        SETTLEMENT_GROSS_CHARGE_BRL NUMBER(15,5),
                        
                        -- COLUNAS PREENCHIDAS PELO LOADER
                        DIRECTION CHAR(3),
                        SOURCE_FILE_NAME VARCHAR(255), -- Arquivo de origem da informação
                        SOURCE_SYSTEM VARCHAR(255), -- Sistema de Origem (DCH, BO, etc)
                        JOB_NAME VARCHAR(255), -- Job que realizou a carga
                        ETL_BATCH_ID VARCHAR(50), -- ID de Batch
                        
                        -- COLUNAS PREENCHIDAS AUTOMATICAMENTE
                        ROW_CREATE_TS TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP, -- TIMESTAMP em que a informação foi carregada no Teradata
                        ROW_CREATE_USER VARCHAR(128) DEFAULT USER -- Usuário que carregou a informação
                    );
                """
        )
    except Exception as e:
        raise e


def move_data_from_stg_to_final_table(
    db_name: str, table_name: str, logger: logging.Logger
) -> None:
    logger.info("Enviando dados para tabela final...")
    query = f"""
    INSERT INTO {db_name}.FCT_RMNG_NAT
        SELECT
            TAP_FILE_CURRENT_PROCESSING_DATE,
            DATE_CALL,
            DIRECTION,
            PMN_SETTLEMENT_TADIG_CODE,
            CALL_TYPE,
            IMSI,
            MSISDN,
            DEVICE_TAC_CODE,
            APN_NETWORK,
            NUMBER_OF_CALLS,
            CHARGED_SMS,
            CHARGED_MINUTES,
            CHARGED_MB,
            SETTLEMENT_GROSS_CHARGE_BRL,
            SOURCE_FILE_NAME,
            SOURCE_SYSTEM,
            JOB_NAME,
            ETL_BATCH_ID,
            ROW_CREATE_TS,
            ROW_CREATE_USER
        FROM {db_name}.{table_name};
    """
    try:
        execute_sql(query)
        logger.info("Dados enviados com sucesso!")
        execute_sql(f"DROP TABLE {db_name}.{table_name};")
        logger.info("Staging excluída!")
    except Exception as e:
        logger.error("Erro ao transferir dados da staging: %s", e)


def list_dates_in_file(file: str, logger: logging.Logger) -> List:
    query = """
                SELECT
                    "TAP File (Current) Processing Date" AS TAP_FILE_CURRENT_PROCESSING_DATE,
                    COUNT("Date (Call)") AS ROW_COUNT_CALL
                FROM read_csv_auto(
                        ?,
                        delim=';',
                        encoding = 'utf-16',
                        quote='"')
                WHERE 1=1
                GROUP BY TAP_FILE_CURRENT_PROCESSING_DATE;
            """
    try:
        df = duckdb.execute(query, [file]).df()
    except duckdb.BinderException as e:
        logger.error("Coluna não encontrada: %s", e)
        return []
    return df["TAP_FILE_CURRENT_PROCESSING_DATE"].dt.date.unique().tolist()


def load_file(file: str) -> pandas.DataFrame:

    try:
        query = """
                    SELECT
                        "TAP File (Current) Processing Date" AS TAP_FILE_CURRENT_PROCESSING_DATE,
                        "Date (Call) YYYYMMDD" AS DATE_CALL,
                        "PMN (Settlement) TADIG Code" AS PMN_SETTLEMENT_TADIG_CODE,
                        "Call Type" AS CALL_TYPE,
                        "IMSI" AS IMSI,
                        "MSISDN" AS MSISDN,
                        "Device TAC Code" AS DEVICE_TAC_CODE,
                        "APN Network" AS APN_NETWORK,
                        "Number of Calls (incl. combined partials)" AS NUMBER_OF_CALLS,
                        "Charged SMS" AS CHARGED_SMS,
                        "Charged Minutes" AS CHARGED_MINUTES,
                        "Charged MB" AS CHARGED_MB,
                        "Settlement Gross Charge - TAP Local Currency" AS SETTLEMENT_GROSS_CHARGE_BRL
                    FROM read_csv_auto(
                        ?,
                        delim=';',
                        encoding = 'utf-16',
                        quote='"')
                    WHERE 1=1
                """
        df = duckdb.execute(query, [file]).df()
    except Exception:
        query = """
                    SELECT
                        "TAP File (Current) Processing Date" AS TAP_FILE_CURRENT_PROCESSING_DATE,
                        "Date (Call)" AS DATE_CALL,
                        "PMN (Settlement) TADIG Code" AS PMN_SETTLEMENT_TADIG_CODE,
                        "Call Type" AS CALL_TYPE,
                        "IMSI" AS IMSI,
                        "MSISDN" AS MSISDN,
                        "Device TAC" AS DEVICE_TAC_CODE,
                        "APN Network" AS APN_NETWORK,
                        "Number of Events" AS NUMBER_OF_CALLS,
                        "Charged Events" AS CHARGED_SMS,
                        "Charged Minutes" AS CHARGED_MINUTES,
                        "Charged MB" AS CHARGED_MB,
                        "Settlement Gross Charge - TAP Local Currency" AS SETTLEMENT_GROSS_CHARGE_BRL
                    FROM read_csv_auto(
                        ?,
                        delim=';',
                        encoding = 'utf-16',
                        quote='"')
                    WHERE 1=1
                """
        df = duckdb.execute(query, [file]).df()
    return df


def move_file(base_tap: pathlib.Path, file: str) -> None:
    """_summary_

    Args:
        file (str): _description_
    """
    os.rename(file, str(base_tap) + "\\processed\\" + file[file.rfind("\\") + 1 :])


def create_processed_folder(base_tap: pathlib.Path) -> None:
    try:
        os.mkdir(str(base_tap) + "\\processed\\")
    except Exception:
        ...


def main() -> None:
    """
    _summary_
    """
    load_dotenv()
    logger = setup_logging(str(os.getenv("JOB_NAME")))
    logger.info("Carregando configurações do script...")

    DIRECTION = str(os.getenv("DIRECTION"))
    SOURCE_SYSTEM = str(os.getenv("SOURCE_SYSTEM"))
    JOB_NAME = str(os.getenv("JOB_NAME"))
    JOB_NAME_ABBR = str(os.getenv("JOB_NAME_ABBR"))

    TERADATA_HOST = str(os.getenv("TERADATA_HOST"))
    TERADATA_USER = str(os.getenv("TERADATA_USER"))
    TERADATA_PASSWORD = str(os.getenv("TERADATA_PASSWORD"))
    TERADATA_DB = str(os.getenv("TERADATA_DB"))
    TABLE_NAME = str(os.getenv("TABLE_NAME")) + DIRECTION
    IF_EXISTS = str(os.getenv("IF_EXISTS"))

    if not is_valid_table_name(TABLE_NAME):
        logger.error(
            "Nome de tabela '%s' contém caracteres não permitidos ou formato inválido.",
            TABLE_NAME,
        )
        sys.exit(1)

    try:
        BATCH_SIZE_TERADATA = int(os.getenv("BATCH_SIZE_TERADATA"))
    except TypeError:
        logger.error(
            "O valor de 'BATCH_SIZE_TERADATA' não é um número inteiro válido: %s",
            os.getenv("BATCH_SIZE_TERADATA"),
        )
        sys.exit()

    TABLE_COLUMNS = OrderedDict(
        TAP_FILE_CURRENT_PROCESSING_DATE=TIMESTAMP,
        DATE_CALL=TIMESTAMP,
        PMN_SETTLEMENT_TADIG_CODE=VARCHAR(8000),
        CALL_TYPE=VARCHAR(8000),
        IMSI=VARCHAR(8000),
        MSISDN=VARCHAR(8000),
        APN_NETWORK=VARCHAR(8000),
        DEVICE_TAC_CODE=VARCHAR(8000),
        NUMBER_OF_CALLS=INTEGER(),
        CHARGED_SMS=INTEGER(),
        CHARGED_MINUTES=NUMBER(precision=15, scale=5),
        CHARGED_MB=NUMBER(precision=15, scale=5),
        SETTLEMENT_GROSS_CHARGE_BRL=NUMBER(precision=15, scale=5),
        DIRECTION=CHAR(3),
        SOURCE_FILE_NAME=VARCHAR(255),
        SOURCE_SYSTEM=VARCHAR(255),
        JOB_NAME=VARCHAR(255),
        ETL_BATCH_ID=VARCHAR(50),
        ROW_CREATE_TS=TIMESTAMP,
        ROW_CREATE_USER=VARCHAR(128),
    )

    BASE_TAP = pathlib.Path(input("INSIRA A PASTA QUE CONTEM OS ARQUIVOS PARA ENVIO: "))
    create_processed_folder(BASE_TAP)
    logger.info("Configuração finalizada!")

    logger.info("Buscando arquivos...")
    paths = sorted(BASE_TAP.iterdir(), key=os.path.getmtime)
    errors_dataframe = None

    # Conexão ao host
    try:
        logger.info("Criando conexão com o host")
        create_context(
            host=TERADATA_HOST,
            username=TERADATA_USER,
            password=TERADATA_PASSWORD,
            logmech="LDAP",
            database=TERADATA_DB,
        )
        logger.info("Conexão criada com sucesso!")
    except TeradataMlException as e:
        logger.error("Erro ao conectar ao host: %s", e)
        sys.exit()

    # Criação da staging caso não exista e configura o queryband
    try:
        ensure_staging_table(TABLE_NAME, logger)
    except ValueError as e:
        logger.error("Erro ao criar a tabela: %s", e)
        sys.exit()
    except Exception as e:
        logger.error("Erro ao criar a tabela: %s", e)

    uploaded_files = []

    # Carregamento do arquivo, sanitização de colunas e upload com fastload para o teradata
    try:
        for file in paths:
            if str(file).endswith(".csv"):
                logger.info("Carregando arquivo: %s", str(file))
                logger.info(
                    "Datas no arquivo: %s", list_dates_in_file(str(file), logger)
                )
                df = load_file(str(file))

                logger.info("Ajustando colunas...")
                df = sanitize_numeric_col(
                    df,
                    ["CHARGED_MINUTES", "CHARGED_MB", "SETTLEMENT_GROSS_CHARGE_BRL"],
                    is_integer=False,
                )
                df = sanitize_numeric_col(
                    df, ["CHARGED_SMS", "NUMBER_OF_CALLS"], is_integer=True
                )
                df = sanitize_str_col(
                    df,
                    [
                        "DEVICE_TAC_CODE",
                        "IMSI",
                        "MSISDN",
                        "PMN_SETTLEMENT_TADIG_CODE",
                        "CALL_TYPE",
                        "APN_NETWORK",
                    ],
                )
                df = sanitize_date_col(
                    df, ["TAP_FILE_CURRENT_PROCESSING_DATE", "DATE_CALL"]
                )

                df = add_audit_columns(
                    DIRECTION,
                    SOURCE_SYSTEM,
                    JOB_NAME,
                    JOB_NAME_ABBR,
                    TERADATA_USER,
                    df,
                    str(file),
                    logger,
                )

                logger.info("Reordenando colunas para bater com o DDL do Teradata...")
                # Garante que o DF tenha EXATAMENTE as colunas na ordem do OrderedDict
                df = df[list(TABLE_COLUMNS.keys())]

                logger.info("Arquivo carregado com sucesso. Shape: %s", str(df.shape))
                logger.info(
                    "Dataframe carregado com as seguintes colunas: \n%s", df.dtypes
                )
                # df.to_csv("test.csv", sep=";", decimal=",", encoding="utf_8")
                # sys.exit()
                logger.info("Enviando para o Teradata...")

                try:
                    result = fastload(
                        df,
                        TABLE_NAME,
                        TERADATA_DB,
                        types=TABLE_COLUMNS,
                        if_exists=IF_EXISTS,
                        batch_size=BATCH_SIZE_TERADATA,
                        open_sessions=2,
                    )
                    if not result["errors_dataframe"].empty:
                        logger.info(
                            "Resultado do envio: %s", result["errors_dataframe"].head(2)
                        )
                        errors_dataframe = result["errors_dataframe"].head(2)
                        errors_dataframe.to_csv(
                            "errors.csv", sep=";", decimal=",", encoding="utf_16"
                        )
                        sys.exit()
                    logger.info("Informações enviadas com sucesso!")
                except TeradataMlException as e:
                    logger.error(
                        "Não foi possível subir as informações. Erro encontrado: \n%s",
                        e,
                    )
                    sys.exit()

                logger.info("Movendo arquivo para a pasta de processados")
                move_file(BASE_TAP, str(file))
            uploaded_files.append(str(file))
        move_data_from_stg_to_final_table(TERADATA_DB, TABLE_NAME, logger)
    except MemoryError as e:
        logger.info("Arquivos enviados: %s", uploaded_files)
        logger.error("Erro de Memória: %s", e)


if __name__ == "__main__":
    main()
