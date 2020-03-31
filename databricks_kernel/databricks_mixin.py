import asyncio
import json
import logging
import re
from json.decoder import JSONDecodeError
from pathlib import Path

import aiohttp

from . import magics
from .comm import Comm
from .exceptions import (
    ClusterNotOnlineException,
    CommandCanceled,
    CommandError,
    IncompleteResults,
    NoSuchMagic,
)
from .utils import Config
from . import html

logger = logging.getLogger("asyncio")
logger.setLevel(logging.DEBUG)


def get_cluster_state(clusters, cluster_id):
    try:
        return [x["state"] for x in clusters if x["id"] == cluster_id][0]
    except IndexError:
        return False


class DatabricksMixin(object):
    _config_path = Path.home() / ".jupyter" / "databricks.json"
    _context_id = None

    config = Config({})
    session = None

    async def _build_session(self):
        if self.session:
            await self.session.close()

        self.session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.config.api_key}"}
        )

    async def _get_or_create_context_id(self):
        if not self._context_id:
            async with self.session.post(
                f"{self.config.uri}/api/1.2/contexts/create",
                json={"language": self.language, "clusterId": self.config.cluster_id},
            ) as r:

                r.raise_for_status()
                body = await r.json()
                self._context_id = body["id"]

        return self._context_id

    async def _fetch_cluster_list(self):
        async with self.session.get(f"{self.config.uri}/api/2.0/clusters/list") as r:
            r.raise_for_status()
            results = await r.json()

        clusters = [
            {
                "id": x["cluster_id"],
                "name": x["cluster_name"],
                "state": x["state"].lower(),
            }
            for x in results["clusters"]
        ]

        return clusters

    async def _start_cluster(self, cluster_id):
        clusters = await self._fetch_cluster_list()
        state = prev_state = get_cluster_state(clusters, cluster_id)

        async with self.session.post(
            f"{self.config.uri}/api/2.0/clusters/start",
            json={"cluster_id": cluster_id},
        ) as r:
            r.raise_for_status()

            while state != "running":
                await asyncio.sleep(2)
                clusters = await self._fetch_cluster_list()
                state = get_cluster_state(clusters, cluster_id)
                if prev_state != state:
                    self.send_comm_message(
                        "databricks.config", await self._config_changed()
                    )

                prev_state = state

    async def _check_status(self, command_id):
        context_id = await self._get_or_create_context_id()

        async with self.session.get(
            f"{self.config.uri}/api/1.2/commands/status?clusterId={self.config.cluster_id}&contextId={context_id}&commandId={command_id}"  # noqa
        ) as r:
            r.raise_for_status()
            return await r.json()

    async def _is_online_cluster(self, cluster_id=None):
        cluster_id = cluster_id or self.config.cluster_id
        clusters = await self._fetch_cluster_list()
        state = get_cluster_state(clusters, cluster_id)
        return state in ["running", "resizing"]

    async def _destroy_context(self):
        if self.config.cluster_id and self._context_id:
            async with self.session.post(
                f"{self.config.uri}/api/1.2/contexts/destroy",
                json={
                    "clusterId": self.config.cluster_id,
                    "contextId": self._context_id,
                },
            ) as r:
                r.raise_for_status()

    async def _cancel_command(self, command_id):
        command_status = await self._check_status(command_id)

        if command_status["status"] in ["Running", "Queued"]:
            self.log(f"Cancelling command {command_id}")

            context_id = await self._get_or_create_context_id()

            async with self.session.post(
                f"{self.config.uri}/api/1.2/commands/cancel",
                json={
                    "clusterId": self.config.cluster_id,
                    "contextId": context_id,
                    "commandId": command_id,
                },
            ) as r:
                r.raise_for_status()

    async def _config_changed(self, data=None, *args):
        if data:
            data = data["data"]

        if type(data) == dict:
            self.config.update(data)

            with self._config_path.open("w") as f:
                f.write(self.config.to_json())

            await self._build_session()

        try:
            clusters = await self._fetch_cluster_list()
        except Exception as e:
            print(e)
            clusters = []

        if not self.config.cluster_id:
            self.config.cluster_id = clusters[0]["id"]

        return {
            "config": self.config.__dict__,
            "clusters": clusters,
        }

    async def _handle_actions(self, content, *args):
        action = content["data"]["action"]
        data = content["data"]["data"]
        if action == "start_cluster":
            await self._start_cluster(data["cluster_id"])

    async def init_session(self):
        if self._config_path.exists():
            with self._config_path.open() as f:
                try:
                    self.config = Config(json.load(f))
                except (JSONDecodeError, TypeError):
                    logger.warn("Could not read")

        await self._build_session()

        self.comms.update(
            **{
                x.uuid: x
                for x in [
                    Comm("databricks.config", self._config_changed),
                    Comm("databricks.actions", self._handle_actions),
                ]
            }
        )

        # periodically send new config update
        while True:
            self.send_comm_message("databricks.config", await self._config_changed())
            await asyncio.sleep(5)

    async def _run_command(self, code):

        if not await self._is_online_cluster():
            raise ClusterNotOnlineException()

        context_id = await self._get_or_create_context_id()

        async with self.session.post(
            f"{self.config.uri}/api/1.2/commands/execute",
            json={
                "language": self.language,
                "clusterId": self.config.cluster_id,
                "contextId": context_id,
                "command": code,
            },
        ) as r:
            r.raise_for_status()
            body = await r.json()

        command_id = body["id"]

        cmd_status = await self._check_status(command_id)
        while cmd_status["status"] in ["Running", "Queued"]:
            cmd_status = await self._check_status(command_id)
            await asyncio.sleep(1)

            if self.interrupt_execution:
                await self._cancel_command(command_id)
                raise CommandCanceled()

        return cmd_status

    async def _execute_code(
        self,
        code,
        silent=False,
        store_history=False,
        user_expressions={},
        allow_stdin=False,
        stop_on_error=True,
    ):
        response = await self._run_command(code)
        logger.warn(str(response)[:200])

        if "results" not in response:
            raise IncompleteResults()

        results = response["results"]

        if "resultType" not in results:
            raise IncompleteResults()

        result_type = results["resultType"]

        if result_type == "error":
            summary = results["summary"]
            cause = results["cause"]
            raise CommandError(summary, cause)
        elif result_type == "table":
            return {
                "html": html.table(
                    results["data"], [x["name"] for x in results["schema"]]
                )
            }
        elif result_type == "text":
            return {"text": results["data"]}
        else:
            raise NotImplementedError(f"Not sure how to handle {result_type}.")

    async def _execute_magic(self, cmd, params):
        if not hasattr(magics, cmd):
            raise NoSuchMagic(cmd)

        return await getattr(magics, cmd)(self, params)

    async def execute_code(
        self,
        code,
        silent=False,
        store_history=False,
        user_expressions={},
        allow_stdin=False,
        stop_on_error=True,
    ):
        match_magic = re.match(r"^%(\w+)\s(.+?)$", code)
        if match_magic:
            cmd, params = match_magic.groups()
            params = re.findall(r"[\"'](.+?)[\"']", params)
            return await self._execute_magic(cmd, *params)
        else:
            return await self._execute_code(
                code,
                silent=False,
                store_history=False,
                user_expressions={},
                allow_stdin=False,
                stop_on_error=True,
            )

    async def do_shutdown(self, *args):
        print(args)
        await self._destroy_context()
        await self.session.close()
