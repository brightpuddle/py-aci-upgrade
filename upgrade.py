from datetime import datetime
from typing import Dict, List
from util import (
    Client,
    GatingEvent,
    State,
    config,
    get_path,
    log,
    login_loop_for,
    loop_for,
    panic_gate,
)


def backup(client: Client, timeout=600) -> State:
    """Run backup job"""
    log.info("Triggering configuration backup...")
    backup_job = client.args["backup_job"]
    backup_dn = f"uni/fabric/configexp-{backup_job}"

    # Start backup job
    res = client.request(
        f"/api/node/mo/{backup_dn}",
        method="POST",
        data={
            "configExportP": {
                "attributes": {"dn": backup_dn, "adminSt": "triggered"},
                "children": [],
            }
        },
    )

    if res.status_code != 200:
        log.error("Failed to run backup")
        return State.FAIL

    # Verify completion
    def verify_completion(client):
        jobs = client.get(
            f"/api/node/mo/uni/backupst/jobs-[{backup_dn}]",
            params={"query-target": "children", "target-subtree-class": "configJob"},
        )
        last_job_status = get_path(str, jobs[-1], "configJob", "attributes", "operSt")
        if last_job_status == "success":
            return State.OK
        else:
            log.debug(f"status: {last_job_status}")
            return State.PENDING

    state = loop_for(timeout, client, verify_completion)
    if state == State.OK:
        log.info("Backup successful.")
    return state


def tech_support(client: Client, timeout=600) -> State:
    """Collect tech support from APICs"""
    log.info("Collecting tech-support from APICs...")
    job_name = client.args["tech_support"]
    job_dn = f"uni/fabric/tsexp-{job_name}"
    res = client.request(
        f"/api/node/mo/{job_dn}",
        method="POST",
        data={
            "dbgexpTechSupP": {
                "attributes": {
                    "dn": job_dn,
                    "rn": f"tsexp-{job_name}",
                    "adminSt": "triggered",
                },
                "children": [],
            }
        },
    )
    if res.status_code != 200:
        log.error("Failed to collect tech support")
        return State.FAIL

    def verify_completion(client):
        res = client.get(
            f"/api/node/mo/expcont/expstatus-tsexp-{job_name}",
            params={
                "query-target": "subtree",
                "target-subtree-class": "dbgexpTechSupStatus",
            },
        )
        status = get_path(
            str, res[-1], "dbgexpTechSupStatus", "attributes", "exportStatus"
        )
        log.debug(f"tech support status: {status}")
        if status == "success":
            return State.OK
        return State.PENDING

    state = loop_for(timeout, client, verify_completion)
    if state == State.OK:
        log.info("Tech support successful.")
    return state


def get_maint_job(client: Client, group: str) -> List[Dict[str, str]]:
    """Fetch maintenance job for current group"""
    return client.get_class(
        "maintUpgJob",
        params={"query-target-filter": f'eq(maintUpgJob.maintGrp,"{group}")'},
    )


class MaintGroup(object):
    def __init__(self, client: Client, group: str, version_str: str):
        self.group = group
        self.version_str = version_str
        self.device_count = len(get_maint_job(client, group))

    def is_already_upgraded(self, client: Client) -> bool:
        """Is this group already running the target code?"""
        for job in get_maint_job(client, self.group):
            log.debug(
                "Code status",
                current_version=job.get("desiredVersion"),
                target_version=self.version_str,
                group=self.group,
            )
            is_target_ver = job.get("desiredVersion") == self.version_str
            is_done = job.get("upgradeStatus") == "completeok"
            if not is_target_ver or (is_target_ver and not is_done):
                return False
        return True

    def is_firmware_downloaded(self, client: Client) -> bool:
        """Is the firmware downloaded for this version?"""
        is_firmware_downloaded = False
        for record in client.get_class("firmwareFirmware"):
            is_target_ver = record.get("fullVersion") == self.version_str
            is_downloaded = record.get("dnldStatus") == "downloaded"
            if is_target_ver and is_downloaded:
                is_firmware_downloaded = True
        return is_firmware_downloaded

    def verify_complete(self, client: Client) -> State:
        """Are we done with this group yet?"""
        state = State.OK
        jobs = get_maint_job(client, self.group)
        if len(jobs) < self.device_count:
            log.debug(
                "Some devices are still offline.",
                online=len(jobs),
                expected=self.device_count,
            )
            state = State.PENDING
        for job in jobs:
            status = job.get("upgradeStatus")
            job_ver = job.get("desiredVersion")
            node_id = get_path(
                str, get_path(str, job.get("dn", "").split("/"), 2).split("-"), 1
            )
            log.debug(
                "Upgrade status",
                percent=job.get("instlProgPct"),
                node_id=node_id,
                status=status,
                target_version=job_ver,
            )
            if status != "completeok":
                state = State.PENDING
            if job_ver != self.version_str:
                state = State.PENDING
        return state


def upgrade_apics(client: Client, timeout=600) -> State:
    """Upgrade controllers."""

    version = client.args["apic_version"]

    group = MaintGroup(client, group="AllCtrlrs", version_str=f"apic-{version}")

    log.info("Checking if upgrade is required for controllers...", version=version)
    if group.is_already_upgraded(client):
        log.info("Controllers already running target code", version=version)
        return State.OK

    log.info("Verifying firmware is in repository.", version=version)
    if not group.is_firmware_downloaded(client):
        log.error(f"{version} not in firmware repository")
        return State.FAIL

    log.info("Starting controller upgrade.", version=version)
    now = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S.000+00:00")
    res = client.request(
        "/api/node/mo/uni/controller",
        method="POST",
        data={
            "ctrlrInst": {
                "attributes": {"dn": "uni/controller", "status": "modified"},
                "children": [
                    {
                        "firmwareCtrlrFwP": {
                            "attributes": {
                                "dn": "uni/controller/ctrlrfwpol",
                                "version": f"apic-{version}",
                            },
                            "children": [],
                        }
                    },
                    {
                        "maintCtrlrMaintP": {
                            "attributes": {
                                "dn": "uni/controller/ctrlrmaintpol",
                                "adminSt": "triggered",
                                "adminState": "up",
                            },
                            "children": [],
                        }
                    },
                    {
                        "trigSchedP": {
                            "attributes": {
                                "dn": "uni/controller/schedp-ConstSchedP",
                                "status": "modified",
                            },
                            "children": [
                                {
                                    "trigAbsWindowP": {
                                        "attributes": {
                                            "dn": "uni/controller/schedp-ConstSchedP/abswinp-ConstAbsWindowP",
                                            "date": now,
                                        },
                                        "children": [],
                                    }
                                }
                            ],
                        }
                    },
                ],
            }
        },
    )
    if res.status_code != 200:
        log.error(f"APIC upgrade failed", code=res.status_code)
        return State.FAIL

    state = loop_for(3600, client, group.verify_complete)
    if state == State.OK:
        log.info("APIC successfully upgraded.", version=version)
    return state


def upgrade_switches(fw_group: str, client: Client, timeout=600) -> State:
    """Upgrade maintenance group."""

    version = client.args["switch_version"]
    group = MaintGroup(client, group=fw_group, version_str=f"n9000-{version}")

    log.info("Checking if upgrade is required for switches...", group=fw_group)
    if group.is_already_upgraded(client):
        log.info("Group already upgraded.", group=fw_group, version=version)
        return State.OK

    log.info("Verifying firmware is in repository...", version=version)
    if not group.is_firmware_downloaded(client):
        log.error("Firmware not in firmware repository.", version=version)
        return State.FAIL

    log.info("Starting upgrade.", group=fw_group, version=version)
    dn = f"uni/fabric/fwpol-{fw_group}"
    res = client.request(
        f"/api/node/mo/{dn}",
        method="POST",
        data={
            "firmwareFwP": {
                "attributes": {"dn": dn, "version": f"n9000-{version}"},
                "children": [],
            }
        },
    )
    if res.status_code != 200:
        log.error("Failed to trigger upgrade", group=fw_group)
        return State.FAIL

    state = loop_for(3600, client, group.verify_complete)
    if state == State.OK:
        log.info("Switches upgraded successfully.", group=fw_group, version=version)
    return state


def run(client=None, timeout=600) -> State:
    """Initial entry point."""
    if client is None:
        client = login_loop_for(timeout, config)
        if client is None:
            return State.FAIL
    try:
        panic_gate(lambda: backup(client, timeout), "backup")
        panic_gate(lambda: tech_support(client, timeout), "tech support")
        panic_gate(lambda: upgrade_apics(client, timeout), "APIC upgrade")
        for group in client.args["firmware_groups"]:
            panic_gate(
                lambda: upgrade_switches(group, client, timeout),
                f"{group} switch upgrade",
            )
    except GatingEvent as e:
        log.error(f"Failed upgrade on {e}.")
        return State.FAIL
    return State.OK


if __name__ == "__main__":
    run()
