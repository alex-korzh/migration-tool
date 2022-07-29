import argparse
import asyncio
import logging
import os
from datetime import datetime

import asyncpg  # type: ignore

logger = logging.getLogger("migrations")
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger.setLevel(logging.INFO)

PLACES = 4
MIGRATIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS migrations (
        version VARCHAR(4) NOT NULL UNIQUE PRIMARY KEY 
    );
"""
INIT_VERSION = """
    INSERT INTO migrations(version) VALUES ('0000')
"""
CURRENT_VERSION = """
    SELECT version FROM migrations
"""
UPDATE_VERSION = """
    UPDATE migrations SET version = $1;
"""


class MigrationManager:
    def __init__(self):
        self.db_url = os.getenv("MIGRATE_DATABASE_URL", None)
        self.versions_uri = os.getenv("VERSIONS_URI", None)
        self.__prepare_folder()

    def __prepare_folder(self):
        if os.path.isdir(self.versions_uri):
            return

        os.mkdir(self.versions_uri)

    async def __aenter__(self) -> "MigrationManager":
        self.conn = await asyncpg.connect(self.db_url)
        logger.debug("successfully connected to database")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.conn.close()
        logger.debug("successfully disconnected from database")

    async def generate(self, migration_name: str | None) -> None:
        logger.debug("generate method called")

        filename = await self.__generate_name(migration_name)

        open(os.path.join(self.versions_uri, f"{filename}_upgrade.sql"), "a").close()
        open(os.path.join(self.versions_uri, f"{filename}_downgrade.sql"), "a").close()

        logger.info("migration file generated successfully")

    async def migrate(self, migration_number: str) -> None:
        logger.debug("migrate method called")
        last_migration_number = await self._get_last_migration_number()

        file_ids = []
        for i in range(int(last_migration_number) + 1, int(migration_number) + 1):
            file_ids.append(self.__int_to_number(i))
        file_names = []
        for i in file_ids:
            file_names.extend(
                [
                    f
                    for f in os.listdir(path=self.versions_uri)
                    if f.startswith(i) and f.endswith("_upgrade.sql")
                ]
            )
        for file_name in file_names:
            with open(os.path.join(self.versions_uri, file_name)) as f:
                sql = f.read()
            async with self.conn.transaction():
                await self.conn.execute(sql)
                await self.conn.execute(UPDATE_VERSION, file_name[:PLACES])
            logger.info(f"migration {file_name} executed successfully")

    async def downgrade(self, migration_number: str) -> None:
        logger.debug("downgrade method called")
        file_ids = []
        current_version = await self._get_last_migration_number()
        for i in range(int(current_version), int(migration_number), -1):
            file_ids.append(self.__int_to_number(i))
        file_names = []
        for i in file_ids:
            file_names.extend(
                [
                    f
                    for f in os.listdir(path=self.versions_uri)
                    if f.startswith(i) and f.endswith("_downgrade.sql")
                ]
            )
        for file_name in file_names:
            with open(os.path.join(self.versions_uri, file_name)) as f:
                sql = f.read()
            new_version = self.__int_to_number(int(file_name[:PLACES]) - 1)
            async with self.conn.transaction():
                await self.conn.execute(sql)
                await self.conn.execute(UPDATE_VERSION, new_version)
            logger.info(f"successfully executed {file_name}")

    async def __generate_name(self, migration_name: str | None) -> str:
        files = [f for f in os.listdir(path=self.versions_uri) if f.endswith(".sql")]

        if len(files) == 0:
            return "0001_initial"

        if not migration_name:
            now = datetime.now().strftime("%Y%m%d_%H%M")
            migration_name = f"auto_{now}"
        else:
            migration_name = migration_name.replace(" ", "_")

        previous_filename = max(files, key=lambda x: int(x[:PLACES]))
        number = int(previous_filename[:PLACES]) + 1
        if number > int(PLACES * "9"):
            raise Exception("Too many migrations")
        str_number = self.__int_to_number(number)
        return f"{str_number}_{migration_name}"

    async def __create_migrations_table(self):
        await self.conn.execute(MIGRATIONS_TABLE)

    async def _get_last_migration_number(self) -> str:
        await self.__create_migrations_table()
        row = await self.conn.fetchrow(CURRENT_VERSION)
        if not row:
            await self.conn.execute(INIT_VERSION)
        return row["version"] if row else "0000"

    def __int_to_number(self, i: int) -> str:
        return (PLACES - len(str(i))) * "0" + str(i)


async def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command")
    commands.required = True
    generate = commands.add_parser("generate")
    generate.add_argument("-m", type=str, required=False)
    migrate = commands.add_parser("migrate")
    migrate.add_argument("number")
    downgrade = commands.add_parser("downgrade")
    downgrade.add_argument("number")

    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    async with MigrationManager() as manager:
        if args.command == "generate":
            await manager.generate(args.m)
        elif args.command == "migrate":
            await manager.migrate(args.number)
        elif args.command == "downgrade":
            await manager.downgrade(args.number)


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()
