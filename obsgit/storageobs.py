import asyncio
import datetime
import xml.etree.ElementTree as ET

class StorageOBS:
    """File storage in OBS"""

    async def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        await instance.__init__(*args, **kwargs)
        return instance

    async def __init__(self, obs, project, package, git):
        self.obs = obs
        self.project = project
        self.package = package
        self.git = git

        self.index = set()
        self.sync = True

        await self._update_index()

    async def _update_index(self):
        # TODO: we do not clean the index, we only add elements
        files_md5, _ = await self.obs.files_md5_revision(self.project, self.package)
        for filename, md5 in files_md5:
            assert filename == md5, f"Storage {self.project}/{self.package} not valid"
            self.index.add(filename)

    async def transfer(self, md5, project, package, filename, obs, **params):
        """Copy a file to the file storage from a remote OBS"""
        assert (
            md5 in self.index
        ), f"File {package}/{filename} ({md5}) missing from storage"
        # TODO: replace "transfer" with copy_to and copy_from.
        # TODO: when both OBS services are the same, use the copy pack
        #       / commit trick from
        #       https://github.com/openSUSE/open-build-service/issues/9615
        print(f"(StorageOBS) transfering {project}/{package}/{filename}")
        await self.obs.transfer(
            self.project, self.package, md5, project, package, filename, obs, **params
        )
        print(f"(StorageOBS) transferred {project}/{package}/{filename}")

    async def _store(self, filename_path, md5):
        """Store a file with md5 into the storage"""
        self.index.add(md5)
        self.sync = False

        print(f"(StorageOBS) storing {filename_path}")
        await self.obs.upload(
            self.project,
            self.package,
            md5,
            filename_path=filename_path,
            rev="repository",
        )
        print(f"(StorageOBS) stored {filename_path}")

    async def store_files(self, package, files_md5):
        package_path = self.git.prefix / package
        files_md5_exists = [
            (filename, md5)
            for filename, md5 in files_md5
            if (package_path / filename).exists()
        ]

        await asyncio.gather(
            *(
                self._store(package_path / filename, md5)
                for filename, md5 in files_md5_exists
            )
        )

        await asyncio.gather(
            *(self.git.delete(package, filename) for filename, _ in files_md5_exists)
        )

        with (package_path / ".obs" / "files").open("w") as f:
            f.writelines(
                f"{filename}\t\t{md5}\n" for filename, md5 in sorted(files_md5)
            )

    async def fetch(self, md5, filename_path):
        """Download a file from the storage under a different filename"""
        self.obs.download(self.project, self.package, md5, filename_path=filename_path)

    async def commit(self):
        """Commit files"""
        # If the index is still valid, we do not commit a change
        if self.sync:
            return

        directory = ET.Element("directory")
        for md5 in self.index:
            entry = ET.SubElement(directory, "entry")
            entry.attrib["name"] = md5
            entry.attrib["md5"] = md5
        commit_date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        await self.obs.command(
            self.project,
            self.package,
            cmd="commitfilelist",
            data=ET.tostring(directory),
            user=self.obs.username,
            comment=f"Storage syncronization {commit_date}",
        )
        self.sync = True
