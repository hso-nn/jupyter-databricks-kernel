import json
import time
import traceback
from pathlib import Path

import requests
from ipykernel.comm import Comm
from ipykernel.kernelbase import Kernel
from IPython.core.formatters import HTMLFormatter
from ipywidgets import Text, Widget
from metakernel import MetaKernel
from tabulate import tabulate


class InputText(object):
    def __init__(self):
        pass

    def _repr_html_(self):
        return '<input type="text"/>'


class DatabricksBaseKernel(MetaKernel):
    implementation = "Databricks"
    implementation_version = "1.0"
    banner = "Databrick - run commands on a databricks cluster."

    _session = None
    _context_id = None
    _config = {}
    comm = None
    _config_path = Path.home() / ".jupyter" / "databricks.json"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        with self._config_path.open() as f:
            self._config = json.load(f)

        # config_changed comm
        self.comm = Comm(target_name="databricks.config")
        self.comm.on_msg(self._config_changed)
        comm_id = self.comm_manager.register_comm(self.comm)

    def _config_changed(self, msg):
        self.log.warn(msg)

        if not self.comm:
            return

        try:
            data = msg["content"]["data"]
        except:
            return

        if type(data) == dict:
            self._config.update(**data)

            with self._config_path.open("w") as f:
                json.dump(self._config, f)

        if self._config:
            self.comm.send({"config": self._config, "clusters": self.cluster_list})
        else:
            self.comm.send({"config": {}})

    @property
    def cluster_list(self):
        resp = self.http_session.get(f"{self.databricks_url}/api/2.0/clusters/list")
        resp.raise_for_status()
        results = resp.json()

        clusters = [
            {
                "id": x["cluster_id"],
                "name": "{} ({})".format(x["cluster_name"], x["state"].lower()),
            }
            for x in results["clusters"]
        ]

        return clusters

    @property
    def databricks_url(self):
        return self._config.get("uri").strip("/")

    @property
    def api_key(self):
        return self._config.get("api_key")

    @property
    def cluster_id(self):
        return self._config.get("cluster_id", self.cluster_list[0]["id"])

    @property
    def http_session(self):
        if not self._session:

            self._session = requests.Session()
            self._session.headers.update({"Authorization": f"Bearer {self.api_key}"})

        return self._session

    @property
    def context_id(self):
        if not self._context_id:
            r = self.http_session.post(
                f"{self.databricks_url}/api/1.2/contexts/create",
                json={"language": self.language, "clusterId": self.cluster_id},
            )
            # if r.status_code == 500 and r.json()['error'].startswith("ClusterNotReadyException"):
            #     self._start_cluster()

            r.raise_for_status()
            body = r.json()

            self._context_id = r.json()["id"]

        return self._context_id

    # def _start_cluster(self):
    #     self.http_session.post(
    #         f"{self.databricks_url}/api/2.0/clusters/start", json={"cluster_id", self.cluster_id}
    #     )

    def _check_status(self, command_id):
        r = self.http_session.get(
            f"{self.databricks_url}/api/1.2/commands/status?clusterId={self.cluster_id}&contextId={self.context_id}&commandId={command_id}"
        )
        r.raise_for_status()
        return r.json()

    def run_command(self, code):
        r = self.http_session.post(
            f"{self.databricks_url}/api/1.2/commands/execute",
            json={
                "language": self.language,
                "clusterId": self.cluster_id,
                "contextId": self.context_id,
                "command": code,
            },
        )

        r.raise_for_status()
        body = r.json()

        command_id = r.json()["id"]

        body = self._check_status(command_id)
        while body["status"] in ["Running", "Queued"]:
            body = self._check_status(command_id)
            time.sleep(10)

        return body

    def do_execute_direct(self, code):
        try:
            response = self.run_command(code)
        except Exception as err:
            self.Error(str(err))
            self.Error("".join(traceback.format_tb(err.__traceback__)))
            self.Print()
            return

        if not "results" in response:
            self.Error(json.dumps(response))
            self.Print()

        results = response["results"]

        if results["resultType"] == "error":
            self.Error(response["cause"])
        elif results["resultType"] == "text":
            self.Print(results["data"])
        elif results["resultType"] == "table":
            self.Error(
                "Displaying tables is not supported, please use the .show() function instead."
            )
            self.Print()
        else:
            self.Error(json.dumps(response))
            self.Print()

    def do_execute_file(self, filename):
        path = Path(filename)
        if not path.suffix or path.suffix == ".ipynb":
            path = path.with_suffix(".ipynb")
            with path.open() as f:
                b = json.load(f)
            status = [
                self.do_execute_direct("".join(x["source"]))
                for x in b["cells"]
                if x["cell_type"] == "code"
            ]
            return max(status)
        else:
            if not suffix == lang_ext:
                raise TypeError(f"Either specify another notebook or {lang_ext} file.")
            with filename.open() as f:
                code = "".join(f.readlines())
            return self.do_execute_direct(code)
