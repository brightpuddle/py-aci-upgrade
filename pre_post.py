import json
import os
from typing import Dict, List, Callable

from util import config, log, loop_for, login_loop_for, State, Client


Snapshot = Dict[str, List[Dict[str, str]]]

############################################################
# Pre-change data collection
############################################################


def load_snapshot(client: Client) -> Snapshot:
    """Load/create snapshot"""
    fn = client.args["snapshot_file"]
    data = {"faults": [], "devices": []}
    if not os.path.isfile(fn):
        with open(fn, "w") as f:
            log.info(f"Creating new snapshot {fn}...")
            data["faults"] = get_faults(client)
            data["devices"] = get_devices(client)
            data["isis_routes"] = get_interpod_routes(client)
            f.write(json.dumps(data, indent=2))
            return data
    with open(fn, "r") as f:
        log.info(f"Loading snapshot from {fn}...")
        return json.loads(f.read())


def get_faults(client: Client) -> List[Dict[str, str]]:
    """Get current fault list"""
    return client.get_class("faultInst")


def get_devices(client: Client) -> List[Dict[str, str]]:
    """Get current device list"""
    return client.get_class("topSystem")


def get_interpod_routes(client: Client) -> List[Dict[str, str]]:
    """Get current inter-pod routes"""
    tepQueries = []
    for pod in client.get_class("fabricSetupP"):
        if pod.get("podType") == "physical":
            tepPool = pod.get("tepPool")
            if tepPool:
                tepQueries.append(f'eq(isisRoute.pfx,"{tepPool}")')
    if len(tepQueries) > 0:
        tepQuery = ",".join(tepQueries)
        return client.get_class(
            "isisRoute",
            params={
                "rsp-subtree-include": "relations",
                "query-target-filter": f"or({tepQuery})",
            },
        )
    return []


############################################################
# Post-change comparison
############################################################

enabled_post_checks = []


CheckFn = Callable[[Client, Snapshot], State]


def enable_post(fn: CheckFn) -> CheckFn:
    enabled_post_checks.append(fn)
    return fn


def run_post_checks(client: Client, snapshot: Snapshot) -> State:
    # Non-zero code indicates an error condition
    for check in enabled_post_checks:
        log.info(f"Comparing: {check.__doc__}...")
        state = check(client, snapshot)
        if state == State.FAIL:
            log.error("Failed on comparison", check=check.__doc__)
            return state
    return State.OK


@enable_post
def compare_faults(client: Client, snapshot: Snapshot) -> State:
    """faults"""
    new_faults = []
    has_new_critical_fault = False
    for current_fault in get_faults(client):
        new_fault = True
        for previous_fault in snapshot["faults"]:
            previous_dn = previous_fault.get("dn", "")
            if previous_dn and previous_dn == current_fault.get("dn", ""):
                new_fault = False
        if new_fault and current_fault.get("severity") != "cleared":
            new_faults.append(current_fault)
    if len(new_faults) > 0:
        log.warning("%d new faults found" % len(new_faults))
        by_code = {}
        for fault in new_faults:
            if fault["severity"] == "critical":
                has_new_critical_fault = True
            if client.args["debug"]:
                log.debug(
                    "New fault:",
                    code=fault["code"],
                    dn=fault["dn"],
                    severity=fault["severity"],
                    description=fault["descr"],
                )
            else:
                code = fault.get("code", "")
                if code in by_code:
                    by_code[code]["count"] += 1
                else:
                    by_code[code] = {
                        "count": 1,
                        "severity": fault.get("severity", ""),
                        "description": fault.get("descr", ""),
                    }
        for code, fault_meta in sorted(by_code.items()):
            log.warning("New fault(s)", **fault_meta)
        return State.FAIL if has_new_critical_fault else State.OK
    log.debug("No new faults found")
    return State.OK


@enable_post
def compare_devices(client: Client, snapshot: Snapshot) -> State:
    """devices"""
    has_missing = False
    current_dns = [r.get("dn", "") for r in get_devices(client)]
    for device in snapshot["devices"]:
        snapshot_dn = device.get("dn", "")
        if snapshot_dn not in current_dns:
            # Device is missing!
            log.warning("missing device", dn=device.get("dn"), name=device.get("name"))
            has_missing = True
    return State.FAIL if has_missing else State.OK


@enable_post
def compare_routes(client: Client, snapshot: Snapshot) -> State:
    """ISIS inter-pod routes"""
    current_dns = [r.get("dn", "") for r in get_interpod_routes(client)]
    has_missing = False
    for route in snapshot["isis_routes"]:
        dn = route.get("dn", "")
        if dn not in current_dns:
            # Route is missing!
            log.warning(
                "missing ISIS route", dn=route.get("dn"), prefix=route.get("pfx")
            )
            has_missing = True
    return State.FAIL if has_missing else State.OK


def init(timeout: int = 3600) -> State:
    """Always create a new snapshot, for the start of the upgrade"""
    if os.path.isfile(config.get("snapshot_file")):
        os.remove(config.get("snapshot_file"))
    return run(timeout=timeout)


def run(timeout: int = 3600) -> State:
    client = login_loop_for(timeout, config)
    if client is None:
        return State.FAIL

    snapshot_exists = os.path.isfile(client.args["snapshot_file"])
    snapshot = load_snapshot(client)

    def _run_checks(client: Client) -> State:
        return run_post_checks(client, snapshot)

    if snapshot_exists:
        state = loop_for(timeout, _run_checks, fail_msg="Post-check unsuccessful")
        if state == State.OK:
            log.info("Snapshot compare successful.")
        elif state == State.FAIL:
            log.error("Snapshot compare failed.")
        return state
    else:
        return State.OK


if __name__ == "__main__":
    run(timeout=-1)
