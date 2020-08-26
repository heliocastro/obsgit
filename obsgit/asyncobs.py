import aiohttp
import logging
import xml.etree.ElementTree as ET

class AsyncOBS:
    """Minimal asynchronous interface for OBS"""

    def __init__(self, url, username, password, link="auto", verify_ssl=True):
        self.logger = logging.getLogger("obsgit.asyncobs")

        self.url = url
        self.username = username
        self.link = link

        conn = aiohttp.TCPConnector(limit=5, limit_per_host=5, verify_ssl=verify_ssl)
        auth = aiohttp.BasicAuth(username, password)
        self.client = aiohttp.ClientSession(connector=conn, auth=auth)

    async def close(self):
        """Close the client session"""

        # This method must be called at the end of the object
        # livecycle.  Check aiohttp documentation for details
        if self.client:
            await self.client.close()
            self.client = None

    async def create(self, project, package=None, disabled=False):
        """Create a project and / or package"""
        if await self.authorized(project) and not await self.exists(project):
            # TODO: generate the XML via ElementTree and ET.dump(root)
            if not disabled:
                data = (
                    f'<project name="{project}"><title/><description/>'
                    f'<person userid="{self.username}" role="maintainer"/>'
                    "</project>"
                )
            else:
                data = (
                    f'<project name="{project}"><title/><description/>'
                    f'<person userid="{self.username}" role="maintainer"/>'
                    "<build><disable/></build><publish><disable/></publish>"
                    "<useforbuild><disable/></useforbuild></project>"
                )
            self.logger.debug(f"Creating remote project {project} [disabled: {disabled}]")
            await self.client.put(f"{self.url}/source/{project}/_meta", data=data)

        if ( package
             and await self.authorized(project, package)
             and not await self.exists(project, package) ):
            if not disabled:
                data = (
                    f'<package name="{package}" project="{project}"><title/>'
                    "<description/></package>"
                )
            else:
                data = (
                    f'<package name="{package}" project="{project}"><title/>'
                    "<description/><build><disable/></build><publish><disable/>"
                    "</publish><useforbuild><disable/></useforbuild></package>"
                )
            self.logger.debug(
                f"Creating remote package {project}/{package} [disabled: {disabled}]"
            )
            await self.client.put(
                f"{self.url}/source/{project}/{package}/_meta", data=data
            )

    async def _download(self, url_path, filename_path, **params):
        self.logger.debug(f"Start download {url_path} to {filename_path}")
        async with self.client.get(f"{self.url}/{url_path}", params=params) as resp:
            with filename_path.open("wb") as f:
                while True:
                    chunk = await resp.content.read(1024 * 4)
                    if not chunk:
                        break
                    f.write(chunk)
        self.logger.debug(f"End download {url_path} to {filename_path}")

    async def download(self, project, *path, filename_path, **params):
        """Download a file from a project or package"""
        url_path = "/".join(("source", project, *path))
        await self._download(url_path, filename_path, **params)

    async def _upload(self, url_path, filename_path=None, data=None, **params):
        if filename_path:
            self.logger.debug(f"Start upload {filename_path} to {url_path}")
            with filename_path.open("rb") as f:
                resp = await self.client.put(
                    f"{self.url}/{url_path}", data=f, params=params
                )
            self.logger.debug(f"End upload {filename_path} to {url_path}")
        elif data is not None:
            self.logger.debug(f"Start upload to {url_path}")
            resp = await self.client.put(
                f"{self.url}/{url_path}", data=data, params=params
            )
            self.logger.debug(f"End upload to {url_path}")
        else:
            resp = None
            self.logger.warning("Filename nor data provided. Nothing to upload")

        if resp and resp.status != 200:
            self.logger.warning(f"PUT {resp.status} on {url_path}")

    async def upload(self, project, *path, filename_path=None, data=None, **params):
        """Upload a file to a project or package"""
        url_path = "/".join(("source", project, *path))
        await self._upload(url_path, filename_path=filename_path, data=data, **params)

    async def _delete(self, url_path, **params):
        self.logger.debug(f"Delete {url_path}")
        await self.client.delete(f"{self.url}/{url_path}", params=params)

    async def delete(self, project, *path, **params):
        """Delete a file, project or package"""
        url_path = "/".join(("source", project, *path))
        await self._delete(url_path, **params)

    async def _command(self, url_path, cmd, filename_path=None, data=None, **params):
        params["cmd"] = cmd
        if filename_path:
            self.logger.debug(f"Start command {cmd} {filename_path} to {url_path}")
            with filename_path.open("rb") as f:
                await self.client.post(f"{self.url}/{url_path}", data=f, params=params)
            self.logger.debug(f"End command {cmd} {filename_path} to {url_path}")
        elif data:
            self.logger.debug(f"Start command {cmd} to {url_path}")
            await self.client.post(f"{self.url}/{url_path}", data=data, params=params)
            self.logger.debug(f"End command {cmd} to {url_path}")

    async def command(
            self, project, *path, cmd, filename_path=None, data=None, **params
    ):
        """Send a command to a project or package"""
        url_path = "/".join(("source", project, *path))
        await self._command(
            url_path, cmd, filename_path=filename_path, data=data, **params
        )

    async def _transfer(self, url_path, to_url_path, to_obs=None, **params):
        to_obs = to_obs if to_obs else self
        self.logger.debug(f"Start transfer from {url_path} to {to_url_path}")
        resp = await self.client.get(f"{self.url}/{url_path}")
        to_url = to_obs.url if to_obs else self.url
        await to_obs.client.put(
            f"{to_url}/{to_url_path}", data=resp.content, params=params
        )
        self.logger.debug(f"End transfer from {url_path} to {to_url_path}")

    async def transfer(
            self,
            project,
            package,
            filename,
            to_project,
            to_package=None,
            to_filename=None,
            to_obs=None,
            **params,
    ):
        """Copy a file between (two) OBS instances"""
        to_package = to_package if to_package else package
        to_filename = to_filename if to_filename else filename
        await self._transfer(
            f"source/{project}/{package}/{filename}",
            f"source/{to_project}/{to_package}/{to_filename}",
            to_obs,
            **params,
        )

    async def _xml(self, url_path, **params):
        try:
            async with self.client.get(f"{self.url}/{url_path}", params=params) as resp:
                return ET.fromstring(await resp.read())
        except Exception:
            return ET.fromstring('<directory rev="latest"/>')

    async def packages(self, project):
        """List of packages inside an OBS project"""
        root = await self._xml(f"source/{project}")
        return [entry.get("name") for entry in root.findall(".//entry")]

    async def files_md5_revision(self, project, package):
        """List of (filename, md5) for a package, and the active revision"""
        root = await self._xml(f"/source/{project}/{package}", rev="latest")

        revision = root.get("rev")

        if root.find(".//entry[@name='_link']") is not None:
            project_link = (
                await self._xml(f"/source/{project}/{package}/_link", rev="latest")
            ).get("project")

            if project_link and project_link != project and self.link == "never":
                print(
                    f"ERROR: Link {project}/{package} pointing outside ({project_link})"
                )
                return [], None

            if (
                project_link and project_link != project and self.link == "auto"
            ) or self.link == "always":
                revision = root.find(".//linkinfo").get("xsrcmd5")
                root = await self._xml(f"/source/{project}/{package}", rev=revision)

        files_md5 = [
            (entry.get("name"), entry.get("md5")) for entry in root.findall(".//entry")
        ]

        return files_md5, revision

    async def exists(self, project, package=None):
        """Check if a project or package exists in OBS"""
        url = (
            f"{self.url}/source/{project}/{package}"
            if package
            else f"{self.url}/source/{project}"
        )
        async with self.client.head(url) as resp:
            return resp.status != 404

    async def authorized(self, project, package=None):
        """Check if the user is authorized to access the project or package"""
        url = (
            f"{self.url}/source/{project}/{package}"
            if package
            else f"{self.url}/source/{project}"
        )
        async with self.client.head(url) as resp:
            return resp.status != 401
