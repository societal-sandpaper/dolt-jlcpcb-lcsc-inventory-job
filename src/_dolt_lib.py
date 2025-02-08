from pathlib import Path
import subprocess
import tempfile
import shutil

import polars as pl
from loguru import logger


def dolt_clone_repository(
    dolt_username: str, dolt_repo_name: str, repo_parent_path: Path
) -> Path:
    """Clone the Dolt repository.

    Returns the path to the cloned repository.
    """
    logger.info("Cloning the Dolt repository.")
    clone_command = f"dolt clone {dolt_username}/{dolt_repo_name}"

    logger.debug(f"Running command: {clone_command}")
    subprocess.run(clone_command, shell=True, check=True, cwd=repo_parent_path)

    # Clean up the temporary `repo_parent_path / '.dolt'` folder.
    temp_dolt_folder = repo_parent_path / ".dolt"
    if temp_dolt_folder.is_dir():
        files_list = [f for f in temp_dolt_folder.rglob("*") if not f.is_dir()]
        if len(files_list) > 0:
            msg = f"Unexpected files in the temporary Dolt folder: {files_list}. Expected only directories."
            raise RuntimeError(msg)
        shutil.rmtree(temp_dolt_folder)

    # Verify that the clone was successful.
    dolt_repo_path = repo_parent_path / dolt_repo_name
    assert dolt_repo_path.is_dir()
    assert (dolt_repo_path / ".dolt").is_dir()
    return dolt_repo_path


def dolt_run_command(
    dolt_command: list[str] | str, dolt_repo_path: Path, *, check: bool = True
) -> None:
    """Run a Dolt command in the repository, passing its output through to stdout/stderr."""
    if isinstance(dolt_command, str):
        shell = True
    else:
        shell = False

    logger.debug(f"Running command: {dolt_command}")

    subprocess.run(dolt_command, shell=shell, check=check, cwd=dolt_repo_path)


def dolt_run_command_capture_output(
    dolt_command: list[str] | str, dolt_repo_path: Path, *, check: bool = True
) -> subprocess.CompletedProcess[bytes]:
    """Run a Dolt command in the repository, passing its output through to stdout/stderr."""
    if isinstance(dolt_command, str):
        shell = True
    else:
        shell = False

    logger.debug(f"Running command: {dolt_command}")

    return subprocess.run(
        dolt_command, shell=shell, check=check, cwd=dolt_repo_path, capture_output=True
    )


def dolt_list_tables(dolt_repo_path: Path) -> list[str]:
    """List all tables in a Dolt repository."""
    dolt_command = 'dolt sql --query "SHOW TABLES;" --result-format=csv'
    logger.debug(f"Running command: {dolt_command}")
    result = dolt_run_command_capture_output(
        dolt_command,
        dolt_repo_path=dolt_repo_path,
    )

    # Important for `pl.read_csv` to work. Otherwise, it assumes you're passing a path.
    assert isinstance(result.stdout, bytes)

    df = pl.read_csv(result.stdout)
    assert len(df.columns) == 1
    return df[df.columns[0]].to_list()


def import_polars_df_into_dolt_table(
    dolt_repo_path: Path,
    table_name: str,
    df: pl.DataFrame,
) -> None:
    """Load a Polars DataFrame into a Dolt table.

    Requires the repo is already cloned. Does not commit the changes.

    Args:
        enable_force_flag: If True, the `--force` flag will be added to the `dolt table import` command.
            The force flag forces overwriting the schema of the existing table, if it changed.
    """
    logger.info(f"Loading table '{table_name}' into Dolt repo: {df}")

    existing_table_list: list[str] = dolt_list_tables(dolt_repo_path)
    if table_name in existing_table_list:
        # Table is already in database. Replace the contents, but keep the existing schema.
        action_flag = "--replace-table"
        logger.debug(
            f"Table '{table_name}' already exists in the database. "
            "Keeping the schema and replacing its contents."
        )
    else:
        # Table is not in database.
        msg = f"Table '{table_name}' does not exist in the database. Create a migration to create the table."
        raise NotImplementedError(msg)

    with tempfile.TemporaryDirectory() as temp_dir:
        dolt_table_path = Path(temp_dir) / f"{table_name}.pq"
        df.write_parquet(dolt_table_path)

        add_command: list[str] = [
            "dolt",
            "table",
            "import",
            action_flag,  # "--create-table" or "--replace-table"
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
    """Commits and push the changes to the Dolt repository."""
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
