import asyncio
import datetime
import hashlib
import pathlib
import shutil
import pygit2

class Git:
    """Local git repository"""

    def __init__(self, path, prefix=None):
        self.path = pathlib.Path(path)
        self.prefix = self.path / prefix if prefix else self.path
        self.first_entry = {}

    # TODO: Extend it to packages and files
    def exists(self):
        """Check if the path is a valid git repository"""
        return (self.path / ".git").exists()

    def create(self):
        """Create a local git repository"""
        self.prefix.mkdir(parents=True, exist_ok=True)
        pygit2.init_repository(self.path)

    async def delete(self, package, filename=None):
        """Delete a package or a file from a git repository"""
        loop = asyncio.get_running_loop()
        if filename:
            await loop.run_in_executor(None, (self.prefix / package / filename).unlink)
        else:
            await loop.run_in_executor(None, shutil.rmtree, self.prefix / package)

    def packages(self):
        """List of packages in the git repository"""
        return [
            package.parts[-1]
            for package in self.prefix.iterdir()
            if package.is_dir() and package.parts[-1] not in (".git", ".obs")
        ]

    def _md5(self, package, filename):
        md5 = hashlib.md5()
        with (self.prefix / package / filename).open("rb") as f:
            while True:
                chunk = f.read(1024 * 4)
                if not chunk:
                    break
                md5.update(chunk)
        return md5.hexdigest()

    async def files_md5(self, package):
        """List of (filename, md5) for a package"""
        loop = asyncio.get_event_loop()
        files = [
            file_.parts[-1]
            for file_ in (self.prefix / package).iterdir()
            if file_.is_file()
        ]
        md5s = await asyncio.gather(
            *(
                loop.run_in_executor(None, self._md5, package, filename)
                for filename in files
            )
        )
        return zip(files, md5s)

    def head_hash(self):
        return pygit2.Repository(self.path).head.target

    def _patches(self):
        repo = pygit2.Repository(self.path)
        last = repo[repo.head.target]
        for commit in repo.walk(
                last.id, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_TIME
        ):
            if len(commit.parents) == 1:
                for patch in commit.tree.diff_to_tree(commit.parents[0].tree):
                    yield commit, patch
            elif len(commit.parents) == 0:
                for patch in commit.tree.diff_to_tree():
                    yield commit, patch

    def analyze_history(self):
        packages_path = {
            (self.prefix / package).relative_to(self.path)
            for package in self.packages()
        }

        for commit, patch in self._patches():
            packages = packages_path & set(
                pathlib.Path(patch.delta.new_file.path).parents
            )
            assert len(packages) <= 1
            if packages:
                package = packages.pop()
                self.first_entry.setdefault(
                    package,
                    (
                        commit.oid,
                        commit.author.name,
                        commit.author.email,
                        datetime.datetime.utcfromtimestamp(commit.commit_time),
                    ),
                )

    def last_revision_to(self, package):
        package_path = (self.prefix / package).relative_to(self.path)
        return self.first_entry[package_path]