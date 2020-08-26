#!/usr/bin/env python3

import argparse
import asyncio
import configparser
import getpass
import logging
import pathlib
import stat
import sys

from obsgit.asyncobs import AsyncOBS
from obsgit.exporter import Exporter
from obsgit.importer import Importer
from obsgit.storagelfs import StorageLFS
from obsgit.storageobs import StorageOBS

from obsgit.git import Git

LOG = logging.getLogger(__name__)


def read_config(config_filename):
    """Read or create a configuration file in INI format"""
    if not config_filename:
        print("Configuration file not provided")
        sys.exit(-1)

    if not pathlib.Path(config_filename).exists():
        print(f"Configuration file {config_filename} not found.")
        print("Use create_config to create a new configuration file")
        sys.exit(-1)

    config = configparser.ConfigParser()
    config.read(config_filename)
    return config


def create_config(args):
    if not args.config:
        print("Configuration file not provided")
        sys.exit(-1)

    config = configparser.ConfigParser()

    config["export"] = {
        "url": args.api,
        "username": args.username,
        "password": args.password if args.password else "<password>",
        "link": args.link,
    }

    config["import"] = {
        "url": args.api,
        "username": args.username,
        "password": args.password if args.password else "<password>",
    }

    if args.storage == "obs":
        config["storage"] = {
            "type": "obs",
            "url": args.api,
            "username": args.username,
            "password": args.password if args.password else "<password>",
            "storage": f"home:{args.username}:storage/files",
        }
    elif args.storage == "lfs":
        config["storage"] = {
            "type": "lfs",
        }
    else:
        print(f"Storage type {args.storage} not valid")
        sys.exit(-1)

    config["git"] = {"prefix": args.prefix}

    with args.config.open("w") as f:
        config.write(f)

    # Only the user can read and write the file
    args.config.chmod(stat.S_IRUSR | stat.S_IWUSR)

    print(f"Edit {args.config} to adjust the configuration and passwords")

    return config


async def export(args, config):
    project = args.project
    repository = pathlib.Path(args.repository).expanduser().absolute().resolve()
    package = args.package

    obs = AsyncOBS(
        config["export"]["url"],
        config["export"]["username"],
        config["export"]["password"],
        config["export"]["link"],
        verify_ssl=not args.disable_verify_ssl,
    )

    if not await obs.authorized(project, package):
        print("No authorization to access project or package in build service")
        sys.exit(-1)

    if not await obs.exists(project, package):
        print("Project or package not found in build service")
        sys.exit(-1)

    git = Git(repository, config["git"]["prefix"])
    git.create()
    print("Initialized the git repository")

    storage_type = config["storage"]["type"]
    if storage_type == "obs":
        storage_obs = AsyncOBS(
            config["storage"]["url"],
            config["storage"]["username"],
            config["storage"]["password"],
            verify_ssl=not args.disable_verify_ssl,
        )
        storage_project, storage_package = pathlib.Path(
            config["storage"]["storage"]
        ).parts
        await storage_obs.create(storage_project, storage_package, disabled=True)
        print("Remote storage in OBS initialized")

        storage = await StorageOBS(storage_obs, storage_project, storage_package, git)
    elif storage_type == "lfs":
        storage = StorageLFS(git)

        if not await storage.is_installed():
            print("LFS extension not installed")
            await obs.close()
            sys.exit(-1)
        print("Git LFS extension enabled in the repository")

        overlaps = storage.overlaps()
        if overlaps:
            print("Multiple LFS tracks are overlaped. Fix them manually.")
            for a, b in overlaps:
                print(f"* {a} - {b}")
    else:
        raise NotImplementedError(f"Storage {storage_type} not implemented")

    exporter = Exporter(
        obs,
        git,
        storage,
        args.skip_project_meta,
        args.skip_all_project_meta,
        args.skip_all_package_meta,
    )
    if package:
        # To have a self consisten unit, maybe we need to export also
        # the project metadata
        if not ((git.path / ".obs").exists() or args.skip_all_project_meta):
            await exporter.project_metadata(project)
        await exporter.package(project, package)
    else:
        await exporter.project(project)

    if storage_type == "obs":
        await storage_obs.close()
    await obs.close()


async def import_(args, config):
    repository = pathlib.Path(args.repository).expanduser().absolute().resolve()
    project = args.project
    package = args.package

    obs = AsyncOBS(
        config["import"]["url"],
        config["import"]["username"],
        config["import"]["password"],
        config["export"]["link"],
        verify_ssl=not args.disable_verify_ssl,
    )

    git = Git(repository, config["git"]["prefix"])
    if not git.exists():
        print("Project or package not found in build service")
        sys.exit(-1)
    git.analyze_history()

    storage_type = config["storage"]["type"]
    if storage_type == "obs":
        storage_obs = AsyncOBS(
            config["storage"]["url"],
            config["storage"]["username"],
            config["storage"]["password"],
            verify_ssl=not args.disable_verify_ssl,
        )
        storage_project, storage_package = pathlib.Path(
            config["storage"]["storage"]
        ).parts

        if not await storage_obs.authorized(storage_project, storage_package):
            print("No authorization to access the file storage in build service")
            sys.exit(-1)

        if not await storage_obs.exists(storage_project, storage_package):
            print("File storage not found in build service")
            sys.exit(-1)

        storage = await StorageOBS(storage_obs, storage_project, storage_package, git)
    elif storage_type == "lfs":
        storage = StorageLFS(git)

        if not await storage.is_installed():
            print("LFS extension not installed")
            sys.exit(-1)
        print("Git LFS extension enabled in the repository")
    else:
        raise NotImplementedError(f"Storage {storage_type} not implemented")

    importer = Importer(
        obs,
        git,
        storage,
        args.skip_project_meta,
        args.skip_all_project_meta,
        args.skip_all_package_meta,
    )
    if package:
        # If the project is not present, maybe we want to create it
        if not (await obs.exists(project) or args.skip_all_project_meta):
            await importer.project_metadata(project)
        await importer.package(project, package)
    else:
        await importer.project(project)

    if storage_type == "obs":
        await storage_obs.close()
    await obs.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OBS-git simple bridge tool")
    parser.add_argument(
        "--config",
        "-c",
        default=pathlib.Path("~", ".obsgit").expanduser(),
        help="configuration file",
    )
    parser.add_argument(
        "--level", "-l", help="logging level",
    )
    parser.add_argument(
        "--disable-verify-ssl", action="store_true", help="disable SSL verification",
    )

    subparser = parser.add_subparsers()

    parser_create_config = subparser.add_parser(
        "create-config", help="create default config file"
    )
    parser_create_config.add_argument(
        "--api", "-a", default="https://api.opensuse.org", help="url for the api",
    )
    parser_create_config.add_argument(
        "--username", "-u", default=getpass.getuser(), help="username for login",
    )
    parser_create_config.add_argument(
        "--password", "-p", help="password for login",
    )
    parser_create_config.add_argument(
        "--link",
        "-l",
        choices=["never", "always", "auto"],
        default="never",
        help="expand package links",
    )
    parser_create_config.add_argument(
        "--storage",
        "-s",
        choices=["obs", "lfs"],
        default="lfs",
        help="type of storage for large files",
    )
    parser_create_config.add_argument(
        "--prefix",
        default="packages",
        help="git directory where all the packages will be stored",
    )
    parser_create_config.set_defaults(func=create_config)

    parser_export = subparser.add_parser("export", help="export between OBS and git")
    parser_export.add_argument("project", help="OBS project name")
    parser_export.add_argument(
        "repository", nargs="?", default=".", help="git repository directory"
    )
    parser_export.add_argument("--package", "-p", help="OBS package name")
    parser_export.add_argument(
        "--skip-project-meta", action="store_true", help="skip update project _meta",
    )
    parser_export.add_argument(
        "--skip-all-project-meta",
        action="store_true",
        help="skip update all project metadata",
    )
    parser_export.add_argument(
        "--skip-all-package-meta",
        action="store_true",
        help="skip update all package metadata",
    )
    parser_export.set_defaults(func=export)

    parser_import = subparser.add_parser("import", help="import between git and OBS")
    parser_import.add_argument(
        "repository", nargs="?", default=".", help="git repository directory"
    )
    parser_import.add_argument("project", help="OBS project name")
    parser_import.add_argument("--package", "-p", help="OBS package name")
    parser_import.add_argument(
        "--skip-project-meta", action="store_true", help="skip update project _meta",
    )
    parser_import.add_argument(
        "--skip-all-project-meta",
        action="store_true",
        help="skip update all project metadata",
    )
    parser_import.add_argument(
        "--skip-all-package-meta",
        action="store_true",
        help="skip update all package metadata",
    )
    parser_import.set_defaults(func=import_)

    args = parser.parse_args()

    if args.level:
        numeric_level = getattr(logging, args.level.upper(), None)
        if not isinstance(numeric_level, int):
            print(f"Invalid log level: {args.level}")
            sys.exit(-1)
        logging.basicConfig(level=numeric_level)

    if "func" not in args:
        parser.print_help()
        sys.exit(-1)

    if args.func == create_config:
        args.func(args)
    else:
        config = read_config(args.config)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(args.func(args, config))
        loop.close()
