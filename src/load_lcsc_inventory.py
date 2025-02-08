"""Job to load LCSC inventory data into the Dolt database."""

from datetime import datetime, UTC
from pathlib import Path
import shutil

from loguru import logger
import polars as pl
import requests

from _dolt_lib import (
    dolt_clone_repository,
    import_polars_df_into_dolt_table,
    dolt_commit_and_push,
)

DATA_FOLDER = Path(__file__).parent.parent / "data"

DOLT_REPO_NAME = "jlcpcb-lcsc-inventory"
DOLT_USERNAME = "societal-sandpaper"
DOLT_EMAIL = "societal-sandpaper+dolthub@proton.me"

DATA_FOLDERS = {
    "data": DATA_FOLDER,
    "source_download": DATA_FOLDER / "source_download",
    "dolt_repo": DATA_FOLDER / DOLT_REPO_NAME,
}


def download_lcsc_sqlite_database() -> Path:
    """Download the LCSC inventory SQLite database.

    Source: https://github.com/CDFER/jlcpcb-parts-database
    """
    output_path = DATA_FOLDERS["source_download"] / "lcsc_inventory.sqlite"
    url = "https://cdfer.github.io/jlcpcb-parts-database/jlcpcb-components.sqlite3"
    response = requests.get(url)
    response.raise_for_status()
    with open(output_path, "wb") as file:
        file.write(response.content)

    logger.info(
        f"Downloaded the LCSC inventory SQLite database: {output_path.stat().st_size:,} bytes"
    )

    return output_path


def download_jlcpcb_basic_csv() -> Path:
    """Download the JLCPCB basic parts CSV file.

    Source: https://github.com/CDFER/jlcpcb-parts-database
    """
    output_path = DATA_FOLDERS["source_download"] / "jlcpcb_components_basic.csv"
    url = "https://cdfer.github.io/jlcpcb-parts-database/jlcpcb-components-basic-preferred.csv"
    response = requests.get(url)
    response.raise_for_status()
    with open(output_path, "wb") as file:
        file.write(response.content)

    logger.info(
        f"Downloaded the JLCPCB basic parts CSV file: {output_path.stat().st_size:,} bytes"
    )

    return output_path


def load_all_sqlite_tables_into_dolt(sqlite_path: Path, dolt_repo_path: Path) -> None:
    """Load all tables from an SQLite database into Dolt."""
    logger.info("Loading all tables from SQLite database")

    assert sqlite_path.is_file(), f"SQLite file not found: {sqlite_path}"

    table_names: list[str] = ["components", "manufacturers", "categories"]
    logger.debug(f"Copying {len(table_names)} sqlite tables: {table_names}")

    for table_name in table_names:
        logger.info(f"Loading table: {table_name}")

        df = pl.read_database_uri(
            f"SELECT * FROM {table_name}", uri=f"sqlite://{sqlite_path}"
        )
        import_polars_df_into_dolt_table(
            dolt_repo_path=dolt_repo_path,
            table_name=table_name,
            df=df,
        )
        logger.info(f"Loaded table: {table_name}")


def load_basic_csv_into_dolt(csv_path: Path, dolt_repo_path: Path) -> None:
    """Load the basic JLCPCB parts CSV file into Dolt."""
    logger.info(f"Loading basic CSV file: {csv_path}")

    df = pl.read_csv(
        csv_path,
        infer_schema_length=None,  # Use all rows to infer schema.
    )
    starting_row_count = df.height

    # Remove duplicates on `lcsc` col, keeping the max value of any columns where
    # the value differs across rows. Selected max because sometimes values like
    # "Min Order Qty" differ, and the max value makes sense.
    assert df.columns[0] == "lcsc"
    df = df.sort(df.columns, descending=True, nulls_last=True).unique(
        "lcsc",
        keep="first",  # Keep the max value (non-null) of any duplicate columns.
        maintain_order=True,
    )
    logger.info(
        f"Removed duplicates from the basic CSV file: {starting_row_count:,} -> {df.height:,}"
    )
    rows_removed = starting_row_count - df.height
    assert rows_removed < 100, f"Removed too many rows: {rows_removed} > 100"

    import_polars_df_into_dolt_table(
        dolt_repo_path=dolt_repo_path, table_name="jlcpcb_components_basic", df=df
    )

    logger.info("Loaded basic CSV file")


def main() -> None:
    """Main function."""
    logger.info("Starting the LCSC/JLCPCB inventory loading job.")

    # Clean up the data folder.
    if DATA_FOLDER.is_dir():
        shutil.rmtree(DATA_FOLDER)
        logger.info("Removed the 'data' folder.")
    elif DATA_FOLDER.exists():
        msg = f"Unexpected file found at 'data' folder: {DATA_FOLDER}"
        raise RuntimeError(msg)
    for folder_path in DATA_FOLDERS.values():
        folder_path.mkdir(parents=True, exist_ok=True)

    sqlite_path = download_lcsc_sqlite_database()
    basic_csv_path = download_jlcpcb_basic_csv()
    dolt_repo_path = dolt_clone_repository(
        dolt_repo_name=DOLT_REPO_NAME,
        dolt_username=DOLT_USERNAME,
        repo_parent_path=DATA_FOLDER,
    )
    assert dolt_repo_path.absolute() == DATA_FOLDERS["dolt_repo"].absolute()

    load_all_sqlite_tables_into_dolt(sqlite_path, dolt_repo_path)
    load_basic_csv_into_dolt(basic_csv_path, dolt_repo_path)

    logger.info("Finished loading data into Dolt.")

    dolt_commit_and_push(
        dolt_repo_path=dolt_repo_path,
        dolt_username=DOLT_USERNAME,
        dolt_email=DOLT_EMAIL,
        commit_message=f"Automated data update: {datetime.now(UTC).isoformat()}",
    )

    logger.info("Finished the LCSC/JLCPCB inventory loading job.")


if __name__ == "__main__":
    main()
