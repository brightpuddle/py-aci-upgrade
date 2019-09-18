import yaml
import json
import time
from datetime import datetime
from enum import Enum
from typing import Callable, Union, Dict, List

import requests
from requests.exceptions import ConnectionError, Timeout
import colorama  # type: ignore
from colorama import Style, Fore
import urllib3  # type: ignore

urllib3.disable_warnings()
colorama.init()


class State(Enum):
    OK = 1
    FAIL = 2
    PENDING = 3


############################################################
# Logging
############################################################


class Logger(object):
    def __init__(self):
        # Default to INFO level logging
        self.min_lvl = 1
        self.min_file_lvl = 0
        self.log_file = open("upgrade.log", "w+", buffering=1)

    def set_level(self, lvl: int) -> None:
        self.min_lvl = lvl

    def to_file(self, sev, msg, **kwargs) -> None:
        json_log = {"msg": msg, "sev": sev, "timestamp": str(datetime.now())}
        json_log.update(kwargs)
        self.log_file.write(json.dumps(json_log) + "\n")

    def msg(self, msg: str, **kwargs) -> None:
        if len(kwargs) > 0:
            fields = []
            for k, v in kwargs.items():
                k = Style.DIM + k + Style.RESET_ALL  # type: ignore
                fields.append(f"{k}={v}")
            field_str = " ".join(fields)
            print(f"{msg} : {field_str}")
        else:
            print(msg)

    def debug(self, msg: str, **kwargs) -> None:
        header = Fore.BLUE + "[DBG] " + Style.RESET_ALL  # type: ignore
        if self.min_lvl == 0:
            self.msg(f"{header} {msg}", **kwargs)
        if self.min_file_lvl == 0:
            self.to_file("dbg", msg, **kwargs)

    def info(self, msg: str, **kwargs) -> None:
        header = Fore.GREEN + "[INF] " + Style.RESET_ALL  # type: ignore
        if self.min_lvl <= 1:
            self.msg(f"{header} {msg}", **kwargs)
        if self.min_file_lvl <= 1:
            self.to_file("dbg", msg, **kwargs)

    def warning(self, msg: str, **kwargs) -> None:
        header = Fore.YELLOW + "[WRN] " + Style.RESET_ALL  # type: ignore
        if self.min_lvl <= 2:
            self.msg(f"{header} {msg}", **kwargs)
        if self.min_file_lvl <= 2:
            self.to_file("dbg", msg, **kwargs)

    def error(self, msg: str, **kwargs) -> None:
        header = Fore.RED + "[ERR] " + Style.RESET_ALL  # type: ignore
        if self.min_lvl <= 3:
            self.msg(f"{header} {msg}", **kwargs)
        if self.min_file_lvl <= 3:
            self.to_file("dbg", msg, **kwargs)


log = Logger()


############################################################
# Config
############################################################


def load_config():
    """Prompt for missing values."""
    with open("config.yaml") as file:
        data = file.read()
        cfg = yaml.Loader(data).get_data()
        if cfg["debug"]:
            log.set_level(0)
        return cfg


config = load_config()

############################################################
# JSON helpers
############################################################


def get_path(kind, obj, *keys):
    for key in keys:
        if isinstance(key, int):
            if isinstance(obj, list) and len(obj) > key:
                obj = obj[key]
            else:
                obj = None
                break
        if isinstance(key, str):
            if isinstance(obj, dict) and key in obj:
                obj = obj[key]
            else:
                obj = None
                break
    if isinstance(obj, kind):
        return obj
    if isinstance(obj, str) and kind is int:
        return int(obj)
    return kind()


def get_node_dn(dn: str) -> str:
    return "/".join(dn.split("/")[:3])


############################################################
# HTTP client libarary
############################################################


class AuthException(Exception):
    pass


Result = List[Dict[str, Dict[str, Dict[str, str]]]]


class Client(object):
    """APIC HTTP client.

    Handles token and abstracts queries.
    """

    def __init__(self, config):
        self.args = config
        self.cache = {}
        self.jar = requests.cookies.RequestsCookieJar()
        self.last_refresh = time.time()
        self.login()

    def request(self, relative_url, method="GET", **kwargs):
        """Return raw requests result"""
        url = "https://%s%s.json" % (self.args["ip"], relative_url)
        log.debug(method, url=url)
        if method == "POST" and "data" in kwargs:
            kwargs["data"] = json.dumps(kwargs["data"])

        return requests.request(
            method, url, cookies=self.jar, verify=False, timeout=5, **kwargs
        )

    def get(self, relative_url, cache=False, **kwargs) -> Result:
        """Fetch and unwrap GET request"""
        cache_key = relative_url + kwargs.get("params", {}).__repr__()
        if cache and cache_key in self.cache:
            return self.cache[cache_key]
        self.refresh_token()
        res = self.request(relative_url, **kwargs)
        result = get_path(list, res.json(), "imdata")
        log.debug("Response length for %s" % relative_url, length=len(result))
        if cache:
            self.cache[cache_key] = result
        return result

    def get_class(self, cls, **kwargs) -> List[Dict[str, str]]:
        """Shortcut for fetching and unwrapping class request"""
        res = self.get(f"/api/class/{cls}", **kwargs)
        result = []
        for row in res:
            record = get_path(dict, row, cls, "attributes")
            if record:
                result.append(record)
        return result

    def post(self, relative_url, body) -> Result:
        """Fetch and unwrap POST request"""
        self.refresh_token()
        res = self.request(relative_url, method="POST", data=body)
        return get_path(list, res.json(), "imdata")

    def login(self):
        """Login to the APIC"""
        res = self.request(
            "/api/aaaLogin",
            method="POST",
            data={
                "aaaUser": {
                    "attributes": {"name": self.args["usr"], "pwd": self.args["pwd"]}
                }
            },
        )
        if res.status_code != 200:
            raise Exception("Login error.")
        if get_path(str, res.json(), "imdata", 0, "error"):
            raise AuthException("Authentication error.")
        self.jar = res.cookies
        self.last_refresh = time.time()
        return self

    def refresh_token(self):
        """Check last token refresh and refresh if needed"""
        elapsed_time = time.time() - self.last_refresh
        if elapsed_time > (8 * 60):
            res = self.request("/api/aaaRefresh")
            self.jar = res.cookies
            self.last_refresh = time.time()
        return self


############################################################
# Workflow helpers
############################################################


class GatingEvent(Exception):
    pass


def panic_gate(fn: Callable[[], State], message: str) -> State:
    """Workflow gate that panics"""
    state = workflow_gate(fn())
    if state == State.FAIL:
        raise GatingEvent(message)
    return state


def workflow_gate(state: State) -> State:
    if state == state.FAIL:
        log.error("Gating condition: exit, syslog, halt upgrade, etc.")
        exit(1)
    return state


class LoginLoopTimeout(Exception):
    pass


def login_loop_for(x: int, config) -> Union[Client, None]:
    start_time = time.time()
    login_int = config["login_interval"]
    once = x == -1
    while time.time() - start_time < x or once:
        once = False
        try:
            client = Client(config)
            return client
        except Exception:
            log.debug(f"Login failed. Trying again in {login_int}s...")
            time.sleep(login_int)


def loop_for(
    x,
    client,
    fn,
    wait_msg="In progress. Checking again in {}s...",
    fail_msg="Action failed. Trying again in {}s...",
):
    """Loop function fn for x seconds, reattempting login
    Returns on success or timeout
    """
    once = x == -1
    start_time = time.time()
    retry_int = config["retry_interval"]
    while client is not None and (time.time() - start_time < x or once):
        once = False
        elapsed_time = time.time() - start_time
        remaining_time = x - elapsed_time
        try:
            state = fn(client)
            if state == State.OK:
                return state
            elif state == State.PENDING:
                log.debug(wait_msg.format(retry_int))
                time.sleep(retry_int)
                client = login_loop_for(remaining_time, client.args)
            else:
                log.info(fail_msg.format(retry_int))
                time.sleep(retry_int)
                client = login_loop_for(remaining_time, client.args)
        except KeyboardInterrupt:
            exit(0)
        except Timeout:
            log.debug(f"Connection timeout. Trying again in {retry_int}s...")
            time.sleep(retry_int)
        except ConnectionError:
            log.debug(f"Connection error. Attempting login...")
            client = login_loop_for(remaining_time, client.args)
        except AuthException:
            log.debug(f"Authentication error. Attempting login...")
            client = login_loop_for(remaining_time, client.args)
        except Exception as e:
            log.debug(
                f"Unexpected Error. Trying login in {retry_int}s...", error=str(e)
            )
            time.sleep(retry_int)
            client = login_loop_for(remaining_time, client.args)
    # If we reach here timeout has exceeded and action failed
    log.error("Exceeded retry timeout")
    return State.FAIL
