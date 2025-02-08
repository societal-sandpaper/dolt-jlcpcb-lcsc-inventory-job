import shutil
import subprocess
import tempfile
from pathlib import Path

import polars as pl
from loguru import logger


def dolt_clone_repository(
    dolt_username: str, dolt_repo_name: str, repo_parent_path: Path, *, depth: int = 1
) -> Path:
    """Clone the Dolt repository.

    Returns the path to the cloned repository.
    """
    logger.info("Cloning the Dolt repository.")
    clone_command = [
        "dolt",
        "clone",
        f"--depth={depth}",
        f"{dolt_username}/{dolt_repo_name}",
    ]

    logger.debug(f"Running command: {clone_command}")
    subprocess.run(clone_command, check=True, cwd=repo_parent_path)

    # Clean up the temporary `repo_parent_path / '.dolt'` folder.
    temp_dolt_folder = repo_parent_path / ".dolt"
    if temp_dolt_folder.is_dir():
        files_list = [f for f in temp_dolt_folder.rglob("*") if not f.is_dir()]
        if len(files_list) > 0:
            msg = (
                f"Unexpected files in the temporary Dolt folder: {files_list}. "
                "Expected only directories."
            )
            raise RuntimeError(msg)
        shutil.rmtree(temp_dolt_folder)

    # Verify that the clone was successful.
    dolt_repo_path = repo_parent_path / dolt_repo_name
    if not dolt_repo_path.is_dir():
        msg = (
            "Cloning the Dolt repository failed. Expected the folder to exist: "
            f"{dolt_repo_path}"
        )
        raise RuntimeError(msg)
    if not (dolt_repo_path / ".dolt").is_dir():
        msg = (
            "Cloning the Dolt repository failed. Expected the '.dolt' folder to "
            f"exist: {dolt_repo_path / '.dolt'}"
        )
        raise RuntimeError(msg)
    return dolt_repo_path


def dolt_run_command(
    dolt_command: list[str] | str, dolt_repo_path: Path, *, check: bool = True
) -> None:
    """Run a Dolt command, passing its output through to stdout/stderr.

    Useful for commands that don't return any output.
    """
    shell = bool(isinstance(dolt_command, str))

    logger.debug(f"Running command: {dolt_command}")

    subprocess.run(dolt_command, shell=shell, check=check, cwd=dolt_repo_path)


def dolt_run_command_capture_output(
    dolt_command: list[str] | str, dolt_repo_path: Path, *, check: bool = True
) -> subprocess.CompletedProcess[bytes]:
    """Run a Dolt command, capturing its output in an easy-to-read format."""
    shell = bool(isinstance(dolt_command, str))

    logger.debug(f"Running command: {dolt_command}")

    return subprocess.run(
        dolt_command, shell=shell, check=check, cwd=dolt_repo_path, capture_output=True
    )


def dolt_sql_query_to_polars_df(dolt_repo_path: Path, query: str) -> pl.DataFrame:
    """Run a SQL query on the Dolt repository and return the result as a Polars DF."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_sql_file = Path(temp_dir) / "temp.sql"
        temp_sql_file.write_text(query)

        dolt_command = [
            "dolt",
            "sql",
            "--result-format=parquet",
            "--file",
            str(temp_sql_file.absolute()),  # Input SQL file path.
        ]
        result = dolt_run_command_capture_output(
            dolt_command,
            dolt_repo_path=dolt_repo_path,
        )

    parquet_bytes: bytes = result.stdout
    del result

    # Hack: Fix the progress message bug (https://github.com/dolthub/dolt/issues/8839)
    if parquet_bytes.startswith(b"Processed 0.0% of the file\n"):
        parquet_bytes = parquet_bytes.split(b"\n", 1)[1]
        logger.debug("Trimmed 0% progress message from the start.")
    if parquet_bytes.endswith(b"Processed 100.0% of the file\n"):
        parquet_bytes = parquet_bytes.rsplit(b"Processed", 1)[0]
        logger.debug("Trimmed 100% progress message from the end.")

    df = pl.read_parquet(parquet_bytes)
    return df


def dolt_list_tables(dolt_repo_path: Path) -> list[str]:
    """List all tables in a Dolt repository."""
    df = dolt_sql_query_to_polars_df(dolt_repo_path, "SHOW TABLES;")

    if len(df.columns) != 1:
        msg = f"Expected one column, but got {len(df.columns)} columns."
        raise RuntimeError(msg)

    table_names = df[df.columns[0]].to_list()
    if not all(isinstance(name, str) for name in table_names):
        msg = "Expected all table names to be strings."
        raise RuntimeError(msg)

    return table_names


def import_polars_df_into_dolt_table(
    dolt_repo_path: Path,
    table_name: str,
    df: pl.DataFrame,
) -> None:
    """Load a Polars DataFrame into a Dolt table.

    Requires the repo is already cloned. Does not commit the changes.

    """
    logger.info(f"Loading table '{table_name}' into Dolt repo: {df}")

    existing_table_list: list[str] = dolt_list_tables(dolt_repo_path)
    if table_name not in existing_table_list:
        # Table is not in database.
        msg = (
            f"Table '{table_name}' does not exist in the database. Create a migration"
            " to create the table."
        )
        raise ValueError(msg)

    with tempfile.TemporaryDirectory() as temp_dir:
        dolt_table_path = Path(temp_dir) / f"{table_name}.pq"
        df.write_parquet(dolt_table_path)

        add_command: list[str] = [
            "dolt",
            "table",
            "import",
            # Options: "--create-table", "append-table", "--replace-table"
            "--append-table",
            "--file-type=parquet",
            table_name,
            # Absolute path is in the temp directory.
            str(dolt_table_path.absolute()),
        ]
        logger.debug(f"Running command: {add_command}")
        dolt_run_command(add_command, dolt_repo_path=dolt_repo_path)
        logger.debug(f"Loaded table '{table_name}' into Dolt repo.")


def dolt_check_status_is_repo_dirty(dolt_repo_path: Path) -> bool:
    """Check if the Dolt repository is dirty (thus, there are changes to commit).

    Call this function before doing 'dolt add .' to check whether
    there are any changes to commit.
    """
    result = dolt_run_command_capture_output(
        ["dolt", "status"],
        dolt_repo_path=dolt_repo_path,
    )

    return not result.stdout.strip().endswith(b"working tree clean")


def dolt_commit_and_push(
    dolt_repo_path: Path, *, dolt_username: str, dolt_email: str, commit_message: str
) -> None:
    """Commit and push the changes to the Dolt repository."""
    logger.info("Committing and pushing changes to Dolt.")

    # Configure the user.
    dolt_run_command(
        ["dolt", "config", "--add", "user.name", dolt_username],
        dolt_repo_path=dolt_repo_path,
    )
    dolt_run_command(
        ["dolt", "config", "--add", "user.email", dolt_email],
        dolt_repo_path=dolt_repo_path,
    )

    # Check if the repository is dirty.
    if not dolt_check_status_is_repo_dirty(dolt_repo_path):
        logger.info("No changes to commit. Done trying to commit changes.")
        return

    # Add, commit, and push.
    dolt_run_command(["dolt", "add", "."], dolt_repo_path=dolt_repo_path)
    dolt_run_command(
        ["dolt", "commit", "-m", commit_message], dolt_repo_path=dolt_repo_path
    )
    dolt_run_command(["dolt", "push"], dolt_repo_path=dolt_repo_path)

    logger.info("Committed and pushed changes to Dolt.")


def dolt_run_sql(dolt_repo_path: Path, query: str) -> None:
    """Run a SQL query on the Dolt repository, ignoring the output.

    This method is most useful for non-`SELECT` queries (e.g., `INSERT`, `UPDATE`,
    `DELETE`).
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_sql_file = Path(temp_dir) / "temp.sql"
        temp_sql_file.write_text(query)

        dolt_command = [
            "dolt",
            "sql",
            "--file",
            str(temp_sql_file.absolute()),  # Input SQL file path.
        ]
        logger.debug(f"Running command: {dolt_command}")
        dolt_run_command(
            dolt_command,
            dolt_repo_path=dolt_repo_path,
        )


def dolt_truncate_tables(dolt_repo_path: Path, table_names: list[str] | str) -> None:
    """Truncate tables in a Dolt repository, in order.

    This method is useful for deleting all rows in a table.
    """
    if isinstance(table_names, str):
        table_names = [table_names]

    valid_table_names = dolt_list_tables(dolt_repo_path)
    if not set(table_names).issubset(set(valid_table_names)):
        missing_table_names = set(table_names) - set(valid_table_names)
        msg = (
            "The following tables are not in the repository: "
            f"{list(missing_table_names)}"
        )
        raise ValueError(msg)

    for table_name in table_names:
        query = f"DELETE FROM {table_name};"
        dolt_run_sql(dolt_repo_path, query)
        logger.info(f"Truncated table '{table_name}'.")
