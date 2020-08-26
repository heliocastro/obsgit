import asyncio
import functools
import hashlib
import logging
import xml.etree.ElementTree as ET

class Importer:
    def __init__(
            self,
            obs,
            git,
            storage,
            skip_project_meta,
            skip_all_project_meta,
            skip_all_package_meta,
    ):
        self.logger = logging.getLogger("obsgit.importer")
        self.obs = obs
        self.git = git
        self.storage = storage
        self.skip_project_meta = skip_project_meta
        self.skip_all_project_meta = skip_all_project_meta
        self.skip_all_package_meta = skip_all_package_meta

    @functools.lru_cache()
    def project_name(self):
        metadata_path = self.git.path / ".obs" / "_meta"
        return ET.parse(metadata_path).getroot().get("name")

    def replace_project(self, filename_path, project, project_name=None):
        project_name = project_name if project_name else self.project_name()
        with filename_path.open() as f:
            return f.read().replace(project_name, project)

    @functools.lru_cache()
    def changes_git_entry(self, package):
        commit_hash, author, email, commit_date, = self.git.last_revision_to(package)
        entry = "-" * 67
        commit_date = commit_date.strftime("%a %b %d %H:%M:%S UTC %Y")
        entry = f"{entry}\n{commit_date} - {author} <{email}>"
        entry = f"{entry}\n\n- Last git synchronization: {commit_hash}\n\n"
        return entry

    def prepend_changes(self, filename_path, package):
        with filename_path.open("rb") as f:
            return self.changes_git_entry(package).encode("utf-8") + f.read()

    async def project(self, project):
        # TODO: What if the project in OBS is more modern? Is there a
        # way to detect it?

        # First import the project metadata, as a side effect can
        # create the project
        if not (await self.obs.exists(project) and self.skip_all_project_meta):
            await self.project_metadata(project)

        packages_obs = set(await self.obs.packages(project))
        packages_git = set(self.git.packages())
        packages_delete = packages_obs - packages_git

        # Order the packages, uploading the links the last
        packages_git = sorted(
            packages_git, key=lambda x: (self.git.prefix / x / "_link").exists()
        )

        # To avoid stressing OBS / IBS we group the imports
        # TODO: decide if fully serialize the fetch
        group_size = 4
        packages_git = list(packages_git)
        packages_git_groups = [
            packages_git[i : i + group_size]
            for i in range(0, len(packages_git), group_size)
        ]
        for packages_git_group in packages_git_groups:
            await asyncio.gather(
                *(self.package(project, package) for package in packages_git_group),
            )

        await asyncio.gather(
            *(self.obs.delete(project, package) for package in packages_delete),
        )

    async def project_metadata(self, project):
        metadata_path = self.git.path / ".obs"

        metadata = [
            # "_project",
            "_attribute",
            "_config",
            "_pattern",
        ]
        if not self.skip_project_meta:
            metadata.append("_meta")

        await asyncio.gather(
            *(
                self.obs.upload(
                    project,
                    meta,
                    data=self.replace_project(metadata_path / meta, project),
                )
                for meta in metadata
            )
        )

    async def _git_files_md5(self, package):
        files_md5 = []
        for filename, md5 in await self.git.files_md5(package):
            filename_path = self.git.prefix / package / filename
            if filename_path.suffix == ".changes":
                md5 = hashlib.md5()
                md5.update(self.prepend_changes(filename_path, package))
                md5 = md5.hexdigest()
            files_md5.append((filename, md5))
        return files_md5

    async def package(self, project, package):
        print(f"{project}/{package} ...")

        if not (await self.obs.exists(project, package) and self.skip_all_package_meta):
            await self.package_metadata(project, package)

        package_path = self.git.prefix / package

        files_md5_obs, _ = await self.obs.files_md5_revision(project, package)
        files_md5_obs = set(files_md5_obs)
        files_md5_git = set(await self._git_files_md5(package))

        # TODO: reading the files is part of StorageXXX class
        meta_file = package_path / ".obs" / "files"
        if meta_file.exists():
            with (meta_file).open() as f:
                files_md5_git_store = {tuple(line.split()) for line in f.readlines()}
        else:
            files_md5_git_store = set()

        files_md5_upload = files_md5_git - files_md5_obs
        files_md5_transfer = files_md5_git_store - files_md5_obs

        files_obs = {filename for filename, _ in files_md5_obs}
        files_git = {filename for filename, _ in files_md5_git}
        files_git_store = {filename for filename, _ in files_md5_git_store}
        files_delete = files_obs - files_git - files_git_store

        await asyncio.gather(
            *(
                self.obs.upload(
                    project,
                    package,
                    filename,
                    filename_path=package_path / filename,
                    rev="repository",
                )
                for filename, _ in files_md5_upload
                if not filename.endswith(".changes")
            ),
            *(
                self.obs.upload(
                    project,
                    package,
                    filename,
                    data=self.prepend_changes(package_path / filename, package),
                    rev="repository",
                )
                for filename, _ in files_md5_upload
                if filename.endswith(".changes")
            ),
            *(
                self.storage.transfer(
                    md5, project, package, filename, self.obs, rev="repository"
                )
                for filename, md5 in files_md5_transfer
            ),
            *(
                self.obs.delete(project, package, filename, rev="repository")
                for filename in files_delete
            ),
        )

        if files_md5_upload or files_md5_transfer or files_delete:
            # Create the directory XML to generate a commit
            directory = ET.Element("directory")
            for filename, md5 in files_md5_git | files_md5_git_store:
                entry = ET.SubElement(directory, "entry")
                entry.attrib["name"] = filename
                entry.attrib["md5"] = md5

            head_hash = self.git.head_hash()

            await self.obs.command(
                project,
                package,
                cmd="commitfilelist",
                data=ET.tostring(directory),
                user=self.obs.username,
                comment=f"Import {head_hash}",
            )

    async def package_metadata(self, project, package):
        metadata_path = self.git.prefix / package / ".obs"
        metadata = (
            "_meta",
            # "_attribute",
            # "_history",
        )

        # Validate that the metadata can be re-allocated
        project_name = self.project_name()
        package_project_name = (
            ET.parse(metadata_path / "_meta").getroot().get("project")
        )
        if project_name != package_project_name:
            self.logger.warning(f"Please, edit the metadata for {package}")

        await asyncio.gather(
            *(
                self.obs.upload(
                    project,
                    package,
                    meta,
                    data=self.replace_project(
                        metadata_path / meta, project, package_project_name
                    ),
                )
                for meta in metadata
            )
        )