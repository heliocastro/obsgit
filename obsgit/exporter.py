import asyncio
import chardet
import pathlib

class Exporter:
    """Export projects and packages from OBS to git"""

    BINARY = {
        ".xz",
        ".gz",
        ".bz2",
        ".zip",
        ".gem",
        ".tgz",
        ".png",
        ".pdf",
        ".jar",
        ".oxt",
        ".whl",
        ".rpm",
    }
    NON_BINARY_EXCEPTIONS = {".obscpio"}
    NON_BINARY = {
        ".changes",
        ".spec",
        ".patch",
        ".diff",
        ".conf",
        ".yml",
        ".keyring",
        ".sig",
        ".sh",
        ".dif",
        ".txt",
        ".service",
        ".asc",
        ".cabal",
        ".desktop",
        ".xml",
        ".pom",
        ".SUSE",
        ".in",
        ".obsinfo",
        ".1",
        ".init",
        ".kiwi",
        ".rpmlintrc",
        ".rules",
        ".py",
        ".sysconfig",
        ".logrotate",
        ".pl",
        ".dsc",
        ".c",
        ".install",
        ".8",
        ".md",
        ".html",
        ".script",
        ".xml",
        ".test",
        ".cfg",
        ".el",
        ".pamd",
        ".sign",
        ".macros",
    }

    def __init__(
        self,
        obs,
        git,
        storage,
        skip_project_meta,
        skip_all_project_meta,
        skip_all_package_meta,
    ):
        self.obs = obs
        self.git = git
        self.storage = storage
        self.skip_project_meta = skip_project_meta
        self.skip_all_project_meta = skip_all_project_meta
        self.skip_all_package_meta = skip_all_package_meta

    @staticmethod
    def is_binary(filename):
        """Use some heuristics to detect if a file is binary"""
        # Shortcut the detection based on the file extension
        suffix = pathlib.Path(filename).suffix
        if suffix in Exporter.BINARY or suffix in Exporter.NON_BINARY_EXCEPTIONS:
            return True
        if suffix in Exporter.NON_BINARY:
            return False

        # Small (5Kb) files are considered as text
        if filename.stat().st_size < 5 * 1024:
            return False

        # Read a chunk of the file and try to determine the encoding, if
        # the confidence is low we assume binary
        with filename.open("rb") as f:
            chunk = f.read(4 * 1024)
            try:
                chunk.decode("utf-8")
            except UnicodeDecodeError:
                encoding = chardet.detect(chunk)
            else:
                return False
        return encoding["confidence"] < 0.8

    async def project(self, project):
        """Export a project from OBS to git"""
        packages_obs = set(await self.obs.packages(project))
        packages_git = set(self.git.packages())
        packages_delete = packages_git - packages_obs

        if not ((self.git.path / ".obs").exists() and self.skip_all_project_meta):
            await self.project_metadata(project)

        await asyncio.gather(
            *(self.package(project, package) for package in packages_obs),
            *(self.git.delete(package) for package in packages_delete),
        )

        await self.storage.commit()

    async def project_metadata(self, project):
        """Export the project metadata from OBS to git"""
        metadata_path = self.git.path / ".obs"
        metadata_path.mkdir(exist_ok=True)

        metadata = [
            "_project",
            "_attribute",
            "_config",
            "_pattern",
        ]
        if not self.skip_project_meta:
            metadata.append("_meta")

        await asyncio.gather(
            *(
                self.obs.download(project, meta, filename_path=metadata_path / meta)
                for meta in metadata
            )
        )

    async def package(self, project, package):
        """Export a package from OBS to git"""
        package_path = self.git.prefix / package
        package_path.mkdir(exist_ok=True)

        print(f"{project}/{package} ...")

        if not (
            (self.git.prefix / package / ".obs").exists() and self.skip_all_package_meta
        ):
            await self.package_metadata(project, package)

        # We do not know, before downloading, if a file is binary or
        # text.  The strategy for now is to download all the files
        # (except the ones already in the remote storage or in git),
        # and upload later the ones that are binary.  We need to
        # remove those after that

        files_md5_obs, revision = await self.obs.files_md5_revision(project, package)
        files_md5_obs = set(files_md5_obs)
        files_md5_git = set(await self.git.files_md5(package))

        # TODO: one optimization is to detect the files that are
        # stored in the local "files" cache, that we already know that
        # are binary, and do a transfer if the MD5 is different
        files_download = {
            filename
            for filename, md5 in (files_md5_obs - files_md5_git)
            if md5 not in self.storage.index
        }

        files_obs = {filename for filename, _ in files_md5_obs}
        files_git = {filename for filename, _ in files_md5_git}
        files_delete = files_git - files_obs

        await asyncio.gather(
            *(
                self.obs.download(
                    project,
                    package,
                    filename,
                    filename_path=package_path / filename,
                    rev=revision,
                )
                for filename in files_download
            ),
            *(self.git.delete(package, filename) for filename in files_delete),
        )

        # TODO: do not over-optimize here, and detect old binary files
        # Once we download the full package, we store the new binary files
        files_md5_store = [
            (filename, md5)
            for filename, md5 in files_md5_obs
            if filename in files_download
            and Exporter.is_binary(package_path / filename)
        ]
        files_md5_obs_store = [
            (filename, md5)
            for filename, md5 in files_md5_obs
            if md5 in self.storage.index
        ]
        await self.storage.store_files(package, files_md5_store + files_md5_obs_store)

    async def package_metadata(self, project, package):
        metadata_path = self.git.prefix / package / ".obs"
        metadata_path.mkdir(exist_ok=True)

        metadata = (
            "_meta",
            "_attribute",
            "_history",
        )
        await asyncio.gather(
            *(
                self.obs.download(
                    project, package, meta, filename_path=metadata_path / meta
                )
                for meta in metadata
            )
        )