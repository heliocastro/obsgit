import fnmatch
import itertools
import pathlib
import subprocess

from obsgit.exporter import Exporter

class StorageLFS:
    """File storage in git LFS"""

    def __init__(self, git):
        self.git = git
        # When using the OBS storage we can avoid some downloads, but
        # is not the case for LFS.  In this model the index will be
        # empty always.
        self.index = set()
        self.tracked = set()

        self._update_tracked()

    def _update_tracked(self):
        out = subprocess.run(
            ["git", "lfs", "track"],
            cwd=self.git.path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            check=False
        )
        for line in out.stdout.splitlines():
            if line.startswith(" " * 4):
                self.tracked.add(line.split()[0])

    async def is_installed(self):
        out = subprocess.run(
            ["git", "lfs", "install"],
            cwd=self.git.path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False
        )
        is_installed = out.returncode == 0

        # Track the default extensions already, we can later include
        # specific files
        if is_installed:
            for binary in Exporter.BINARY | Exporter.NON_BINARY_EXCEPTIONS:
                await self._store(pathlib.Path(f"*{binary}"))

        return is_installed

    def overlaps(self):
        return [
            (a, b)
            for a, b in itertools.combinations(self.tracked, 2)
            if fnmatch.fnmatch(a, b)
        ]

    def transfer(self, md5, project, package, filename, obs):
        pass

    def _tracked(self, filename):
        return any(fnmatch.fnmatch(filename, track) for track in self.tracked)

    async def _store(self, filename_path):
        # When registering general patterms, like "*.gz" we do not
        # have a path relative to the git repository
        try:
            filename_path = filename_path.relative_to(self.git.path)
        except ValueError:
            pass

        # TODO: we can edit `.gitattributes` manually
        subprocess.run(
            ["git", "lfs", "track", filename_path],
            cwd=self.git.path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False
        )
        self.tracked.add(str(filename_path))
        await self.commit()

    async def store_files(self, package, files_md5):
        package_path = self.git.prefix / package
        for filename, _ in files_md5:
            if not self._tracked(filename):
                await self._store(package_path / filename)

    async def fetch(self):
        pass

    async def delete(self, filename_path):
        subprocess.run(
            ["git", "lfs", "untrack", filename_path],
            cwd=self.git.path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False
        )

    async def commit(self):
        subprocess.run(
            ["git", "add", ".gitattributes"],
            cwd=self.git.path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False
        )
