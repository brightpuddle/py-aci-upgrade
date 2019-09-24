from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable

from util import get_node_dn, get_path, log, loop_for, State, Client


############################################################
# Point-in-time health checks
############################################################

enabled_health_checks = []


CheckFn = Callable[[Client], State]


def enable(fn: CheckFn) -> CheckFn:
    enabled_health_checks.append(fn)
    return fn


def run_checks(client: Client) -> State:
    # Non-zero indicates a gating condition
    for check in enabled_health_checks:
        log.info(f"Checking: {check.__doc__}...")
        state = check(client)
        if state == State.FAIL:
            log.error("Failed on health check", check=check.__doc__)
            return state
    return State.OK


@enable
def check_firmware_download(client: Client) -> State:
    """firmware download status"""
    # Verify dnldStatus == 'downloaded' in firmwareFirmware
    # Note: this checks all firmware downloads; not just target code
    for record in client.get_class("firmwareFirmware"):
        if "fullVersion" not in record:
            continue
        status = record.get("dnldStatus", "")
        if client.args["debug"]:
            log.debug(
                "Firmware download status:",
                name=record.get("name"),
                status=record.get("dnldStatus"),
            )
        if status != "downloaded":
            log.warning(
                "Failed firmware download",
                name=record.get("name"),
                description=record.get("description"),
                status=record.get("dnldStatus"),
            )
            return State.FAIL
    return State.OK


@enable
def check_running_firmware(client: Client) -> State:
    """current running firmware"""
    # Verify only one version from firmwareRunning and firmwareCtrlrRunning
    versions = set()
    for record in client.get_class("firmwareRunning"):
        versions.add(record["peVer"])
    for record in client.get_class("fimrwareCtrlrRunning"):
        versions.add(record["version"])
    if len(versions) > 1:
        log.warning("Multiple firmware versions found", versions=list(versions))
    elif client.args["debug"] and len(versions) > 0:
        log.debug("Firmware:", version=versions.pop())
    return State.OK


@enable
def check_maintenance_groups(client: Client) -> State:
    """switches are in maintenance groups"""
    # Verify all switches from topSystem are also in maintUpgJob objects
    job_dns = []
    for job in client.get_class("maintUpgJob"):
        if job.get("maintGrp", "") != "" and job["dn"].startswith("topology"):
            job_dns.append(get_node_dn(job["dn"]))
    for device in client.get_class("topSystem"):
        if device["role"] == "spine" or device["role"] == "leaf":
            if get_node_dn(device["dn"]) not in job_dns:
                log.warning("Device not in maintenance group", name=device["name"])
                return State.FAIL
    log.debug("All devices in maintenance groups")
    return State.OK


@enable
def check_fabric_scale(client: Client) -> State:
    """fabric-wide scale"""
    # Verify fabric-wide MO counts are < limits from fvcapRule
    over_scale = False
    metrics = {
        "fvCEp": {"name": "endpoints"},
        "fvAEPg": {"name": "EPGs"},
        "fvBD": {"name": "BDs"},
        "fvCtx": {"name": "VRFs"},
        "fvTenant": {"name": "tenants"},
        #
        # API doesn't provide these limits
        "vzBrCP": {"name": "contracts", "limit": 10000},
        "vzFilter": {"name": "filters", "limit": 10000},
    }
    for record in client.get_class("fvcapRule", cache=True):
        subj = record.get("subj")
        if subj in metrics and record["dn"].startswith("uni"):
            metrics[subj]["limit"] = int(record.get("constraint", 0))

    def get_count(class_name):
        res = client.get(
            f"/api/class/{class_name}", params={"rsp-subtree-include": "count"}
        )
        return get_path(int, res, 0, "moCount", "attributes", "count")

    for class_name in metrics:
        metrics[class_name]["count"] = get_count(class_name)

    for class_name, metric in metrics.items():
        # TODO validate scenario where limit isn't found
        if "limit" in metric and metric["count"] > metric["limit"]:
            over_scale = True
            log.warning(f"Over scale limit for {class_name}:", **metric)
        elif "limit" in metric and client.args["debug"]:
            log.debug(
                f'Scale for {metric["name"]}:',
                count=metric["count"],
                limit=metric["limit"],
                mo=class_name,
            )
    return State.FAIL if over_scale else State.OK


@enable
def check_switch_scale(client: Client) -> State:
    """per-switch scale"""
    # Verify counts from ctxClassCnt are < limits from fvcapRule
    from collections import defaultdict

    metrics = defaultdict(lambda: defaultdict(dict))
    # map ctxClassCnt counts to fvcapRule limits
    count_to_limit = {"l2BD": "fvBD", "fvEpP": "fvCEp", "l3Dom": "fvCtx"}
    # Build dict with device/mo/metric
    counts = client.get_class(
        "ctxClassCnt", params={"rsp-subtree-class": "l2BD,fvEpP,l3Dom"}
    )
    for record in counts:
        node_dn = get_node_dn(record["dn"])
        key = count_to_limit.get(record["name"])
        if key:
            metrics[node_dn][key]["count"] = get_path(int, record, "count")

    # Add limits to the metrics dict
    limits = client.get_class("fvcapRule", cache=True)
    for record in limits:
        if record["dn"].startswith("topology"):
            node_dn = get_node_dn(record["dn"])
            subj = record["subj"]
            if node_dn in metrics and subj in count_to_limit.values():
                limit = get_path(int, record, "constraint")
                metrics[node_dn][subj]["limit"] = limit

    # Validate metrics
    over_limit = False
    for node_dn, by_mo in metrics.items():
        for mo, metric in by_mo.items():
            count = metric.get("count", 0)
            limit = metric.get("limit", 0)
            if count > 0 and count >= limit:
                over_limit = True
                log.warning(
                    f"Over scale limit on {node_dn}", mo=mo, count=count, limit=limit
                )
            if client.args["debug"]:
                log.debug(
                    f"Scale metric on {node_dn}:", mo=mo, count=count, limit=limit
                )
    return State.FAIL if over_limit else State.OK


@enable
def check_tcam_scale(client: Client) -> State:
    """per-leaf TCAM scale"""
    # Verify polUsageCum <= polUsageCapCum for eqptcapacityPolUsage5min
    over_limit = False
    for record in client.get_class("eqptcapacityPolUsage5min"):
        node_dn = get_node_dn(record["dn"])
        count = get_path(int, record, "polUsageCum")
        limit = get_path(int, record, "polUsageCapCum")
        if count > 0 and count >= limit:
            over_limit = True
            log.warning(f"Over TCAM scale on {node_dn}", count=count, limit=limit)
        if client.args["debug"]:
            log.debug(f"TCAM scale on {node_dn}", count=count, limit=limit)
    return State.FAIL if over_limit else State.OK


@enable
def check_vpc_health(client: Client) -> State:
    """vPC health"""
    # Verify peerSt == 'up' for vpcDom
    for vpc in client.get_class("vpcDom"):
        if vpc["peerSt"] != "up":
            log.warning("vPC not up", id=vpc["id"], state=vpc["peerSt"])
            return State.FAIL
    log.debug("All vPCs are up")
    return State.OK


@enable
def check_apic_cluster(client: Client) -> State:
    """APIC cluster state"""
    # Verify health == 'fully-fit' in infraWiNode
    for controller in client.get_class("infraWiNode"):
        if controller.get("health") != "fully-fit":
            log.warning("not fully-fit")
            return State.FAIL
    return State.OK


@enable
def check_apic_interfaces(client: Client) -> State:
    """APIC interfaces state"""
    # Verify operSt == 'up' for at least 2 ints in cnwPhysIf
    apic_ints = defaultdict(set)
    for record in client.get_class("cnwPhysIf"):
        node_dn = get_node_dn(record["dn"])
        if record.get("operSt", "") == "up":
            apic_ints[node_dn].add(record["id"])
    for dn, ints in apic_ints.items():
        if len(ints) < 2:
            log.warning("APIC {dn} has < 2 active interfaces")
            return State.FAIL
    return State.OK


@enable
def check_backup(client: Client) -> State:
    """last backup status"""
    # Verify executeTime is within last 24hrs for configJob
    recent_backup = False
    latest_backup = None
    for backup in client.get_class("configJob"):
        iso_backup_str = backup["executeTime"][:19]
        this_backup_time = datetime.strptime(iso_backup_str, "%Y-%m-%dT%H:%M:%S")
        if latest_backup is None or this_backup_time > latest_backup:
            latest_backup = this_backup_time
        last_24hrs = datetime.now() - timedelta(hours=24)
        if this_backup_time >= last_24hrs and backup["operSt"] == "success":
            recent_backup = True
    latest = "None" if latest_backup is None else latest_backup.isoformat()
    if not recent_backup:
        log.warning("Backup not performed within 24 hours", last_backup=latest)
        return State.FAIL
    elif client.args["debug"]:
        log.debug("Last backup performed within 24 hours", last_backup=latest)
    return State.OK


@enable
def check_vcenter(client: Client) -> State:
    """VMware vCenter state"""
    # Verify operSt == 'online' in compCtrlr
    for ctrlr in client.get_class("compCtrlr"):
        if ctrlr.get("operSt", "") != "online":
            log.warning("vCenter offline", name=ctrlr["name"])
            return State.FAIL
    log.debug("All vCenter(s) online")
    return State.OK


@enable
def check_dvs(client: Client) -> State:
    """VMware DVS state"""
    # Verify state == 'poweredOn' in compHv
    for dvs in client.get_class("compHv"):
        if dvs.get("state", "") != "poweredOn":
            log.warning("vSwitch offline", name=dvs["name"])
            return State.FAIL
    log.debug("All vSwitch(s) online")
    return State.OK


def check_ntp_state(client: Client) -> State:
    """NTP sync"""
    # Verify srvStatus == 'synced' in datetimeClkPol
    synced_peers = set()
    for ntp in client.get_class("datetimeClkPol"):
        if "synced" in ntp.get("srvStatus", ""):
            synced_peers.add(ntp["dn"])
    if len(synced_peers) == 0:
        log.warning("NTP not synced to at least 1 peer")
        return State.FAIL
    log.debug("NTP synced.")
    return State.OK


def run(timeout=3600) -> State:
    state = loop_for(timeout, run_checks, fail_msg="Health check unsuccessful")
    if state == State.OK:
        log.info("Health check successful.")
    elif state == State.FAIL:
        log.debug("Health check failed.")
    return state


if __name__ == "__main__":
    run(timeout=-1)
