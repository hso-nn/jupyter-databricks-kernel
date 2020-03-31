import asyncio
import datetime
import hashlib
import hmac
import json
import logging
import time
import traceback
import uuid

import zmq
from zmq.asyncio import Context

from . import html
from .exceptions import CommandCanceled, CommandError

DELIM = b"<IDS|MSG>"

logger = logging.getLogger("asyncio")
logger.setLevel(logging.DEBUG)

ctx = Context()


def str_to_bytes(s):
    return s.encode("ascii")


def encode_message(msg):
    return str_to_bytes(json.dumps(msg))


def sign(auth, msgs):
    """
    Sign a message with a secure signature.
    """
    h = auth.copy()
    for m in msgs:
        h.update(m)
    return str_to_bytes(h.hexdigest())


class KernelBase(object):
    version = "2.0.0"
    language = None
    language_version = None
    language_info = None
    comms = {}
    widgets = {}
    error_timestamp = None

    def __init__(self, config):
        self.jupyter_config = config
        self.base_url = f"{self.jupyter_config.transport}://{self.jupyter_config.ip}"

        secure_key = str_to_bytes(config.key)
        signature_schemes = {"hmac-sha256": hashlib.sha256}
        self.auth = hmac.HMAC(
            secure_key, digestmod=signature_schemes[config.signature_scheme]
        )

        self.engine_id = str(uuid.uuid4())

        self.shell_handlers = {
            "kernel_info_request": self.handle_kernel_info_request,
            "comm_info_request": self.handle_comm_info_request,
            "execute_request": self.handle_execute_request,
            "comm_msg": self.handle_comm_msg,
        }

        self.control_handlers = {
            "interrupt_request": self.handle_interrupt_request,
            "shutdown_request": self.do_shutdown,
        }

        self.execution_count = 0

    async def _receive(self, sock):
        raw_msg = await sock.recv_multipart()

        [
            ids,
            _del,
            _sig,
            raw_header,
            _parent_header,
            _metadata,
            raw_content,
            *_,
        ] = raw_msg
        header = json.loads(raw_header)
        content = json.loads(raw_content)

        return ids, header, content

    async def _init_heartbeat_channel(self):
        sock = ctx.socket(zmq.REP)
        sock.bind(f"{self.base_url}:{self.jupyter_config.hb_port}")
        logger.info("Heartbeat socket initialized")

        while True:
            msg = await sock.recv_multipart()
            sock.send_multipart(msg)

    async def _init_iopub_channel(self):
        self.iopub = ctx.socket(zmq.PUB)
        self.iopub.bind(f"{self.base_url}:{self.jupyter_config.iopub_port}")
        logger.info("IOPUB socket initialized")
        self.publish_status("starting")
        while True:
            ids, header, content = await self._receive(self.iopub)

    async def _init_shell_channel(self):
        self.shell = ctx.socket(zmq.ROUTER)
        self.shell.bind(f"{self.base_url}:{self.jupyter_config.shell_port}")
        logger.info("Shell socket initialized")

        while True:
            ids, header, content = await self._receive(self.shell)
            msg_type = header["msg_type"]
            if msg_type in self.shell_handlers:
                self.publish_status("busy", header)
                await self.shell_handlers[msg_type](content, header, ids)
                self.publish_status("idle", header)
            else:
                logger.warn(f"Unknown shell message type {msg_type}.")

    async def _init_control_channel(self):
        sock = ctx.socket(zmq.ROUTER)
        sock.bind(f"{self.base_url}:{self.jupyter_config.control_port}")
        logger.info("Control socket initialized")

        while True:
            ids, header, content = await self._receive(sock)
            msg_type = header["msg_type"]
            if msg_type in self.control_handlers:
                await self.control_handlers[msg_type](content, header, ids)
            else:
                logger.error(f"Unknown control message type {msg_type}.")

    async def _init_stdin_channel(self):
        sock = ctx.socket(zmq.ROUTER)
        sock.bind(f"{self.base_url}:{self.jupyter_config.stdin_port}")
        logger.info("Stdin socket initialized")

        while True:
            ids, header, content = await self._receive(sock)
            logger.warn("iopub {} {} {}".format(ids, header, content))

    async def _init_sockets(self):
        await asyncio.gather(
            self._init_iopub_channel(),
            self._init_shell_channel(),
            self._init_heartbeat_channel(),
            self._init_control_channel(),
            self._init_stdin_channel(),
            self.init_session(),
        )

    def publish_status(self, status, parent=None):
        self.send(
            self.iopub, "status", {"execution_state": status}, parent_header=parent
        )

    def error(self, msg):
        self.send(self.iopub, "stream", {"name": "stderr", "text": str(msg)})

    def log(self, msg):
        self.send(self.iopub, "stream", {"name": "stdout", "text": str(msg)})

    def start(self):
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
        loop.run_until_complete(self._init_sockets())

    def send(
        self,
        sock,
        msg_type,
        content={},
        parent_header={},
        ids=None,
        username="kernel",
        metadata={},
    ):
        header = {
            "date": datetime.datetime.now().isoformat(),
            "msg_id": str(uuid.uuid4()),
            "username": username,
            "session": self.engine_id,
            "msg_type": msg_type,
            "version": "5.3",
        }

        msgs = [
            encode_message(header),
            encode_message(parent_header),
            encode_message(metadata),
            encode_message(content),
        ]
        signature = sign(self.auth, msgs)
        parts = [DELIM, signature, *msgs]
        if ids:
            parts = [ids] + parts

        sock.send_multipart(parts)

    def send_comm_message(self, target, msg):
        comms = [
            comm_id for comm_id, comm in self.comms.items() if comm.target == target
        ]
        for comm_id in comms:
            self.send(self.iopub, "comm_msg", {"comm_id": comm_id, "data": msg})

    async def handle_comm_info_request(self, content, headers, ids):
        target = content["target_name"]
        comms = {
            x.uuid: {"target_name": x.target}
            for x in self.comms.values()
            if x.target == target
        }
        self.send(
            self.shell,
            "comm_info_reply",
            {"comms": comms, "status": "ok"},
            headers,
            ids,
            username="username",
        )

    async def handle_comm_msg(self, content, headers, ids):
        comm_id = content["comm_id"]

        if comm_id not in self.comms:
            return

        r = await self.comms[comm_id].on_recv(content, headers, ids)
        if r:
            self.send(
                self.iopub, "comm_msg", {"comm_id": comm_id, "data": r}, headers, ids
            )

    async def handle_kernel_info_request(self, content, headers, ids):
        response = {
            "protocol_version": "5.3",
            "implemention": "databricks",
            "implementation_version": self.version,
            "language_info": {
                "name": self.language,
                "version": self.language_version,
                **self.language_info,
            },
            "status": "ok",
        }
        self.send(self.shell, "kernel_info_reply", response, headers, ids)
        self.publish_status("idle", headers)

    def display_data(self, msg, headers, ids):
        self.send(
            self.iopub,
            "display_data",
            {"data": {"text/html": msg}, "metadata": {}},
            headers,
            ids,
        )

    def print_stdout(self, msg, headers, ids):
        self.send(
            self.iopub, "stream", {"name": "stdout", "text": msg}, headers, ids,
        )

    def print_stderr(self, msg, headers, ids):
        self.send(
            self.iopub, "stream", {"name": "stderr", "text": msg}, headers, ids,
        )

    async def handle_execute_request(self, content, headers, ids):
        self.execution_count += 1
        self.interrupt_execution = False

        queue_time = datetime.datetime.strptime(
            headers["date"], "%Y-%m-%dT%H:%M:%S.%f%z"
        ).timestamp()
        if self.error_timestamp and queue_time < self.error_timestamp:
            self.send(
                self.shell, "execute_reply", {"status": "abort"}, headers, ids,
            )
            return

        try:
            msg = await self.execute_code(**content)
            status = "ok"
            print(msg)
            if "text" in msg:
                self.print_stdout(str(msg.get("text")), headers, ids)
            if "html" in msg:
                self.display_data(msg.get("html"), headers, ids)

        except CommandCanceled:
            status = "abort"
            self.print_stderr("Command canceled.", headers, ids)

        except CommandError as e:
            if e.summary and e.cause:
                self.display_data(html.stacktrace(e.summary, e.cause), headers, ids)
            else:
                self.print_stderr(e.cause, headers, ids)
            status = "error"

        except Exception as e:
            msg = str(e)
            #if getattr(e, "skip_traceback", False):
            msg = msg + "\n" + "".join(traceback.format_tb(e.__traceback__))

            status = "error"

            self.print_stderr(msg, headers, ids)

        self.send(
            self.shell,
            "execute_reply",
            {"status": status, "execution_count": self.execution_count},
            headers,
            ids,
        )

        if status == "error" or status == "abort":
            self.error_timestamp = time.time()

    async def handle_interrupt_request(self, content, headers, ids):
        self.interrupt_execution = True

    async def execute_code(
        self,
        code,
        silent=False,
        store_history=False,
        user_expressions={},
        allow_stdin=False,
        stop_on_error=True,
    ):

        raise NotImplementedError()

    async def init_session(self):
        raise NotImplementedError()
