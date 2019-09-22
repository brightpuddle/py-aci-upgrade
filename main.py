from util import config, login_loop_for, log, State
import pre_post
import health
import upgrade


def cli_header(msg) -> None:
    print("\n", "=" * 10, msg)


def main() -> State:
    """Initial entry point."""
    # This is just to make sure APIC access is functional and credentials work
    # Actual clients used for the upgrade are initialized as needed
    client = login_loop_for(120, config)
    if client is None:
        return State.FAIL

    # Pre change checks
    cli_header("Pre-change snapshot")
    if pre_post.init(timeout=120) == State.FAIL:
        log.error("Failed pre-check collection.")
        return State.FAIL
    cli_header("Pre-change health check")
    if health.run(timeout=120) == State.FAIL:
        log.error("Failed pre-change health check.")
        return State.FAIL

    # Pre upgrade prep
    cli_header("Configuration backup")
    if upgrade.backup(600) == State.FAIL:
        log.error("Failed configuration backup.")
        return State.FAIL
    cli_header("Tech support")
    if upgrade.tech_support(600) == State.FAIL:
        log.error("Failed collecting tech support.")
        return State.FAIL

    # APIC upgrade
    cli_header("APIC upgrade")
    if upgrade.upgrade_apics(3600) == State.FAIL:
        log.error("Failed upgrading APICs.")
        return State.FAIL
    cli_header("APIC post-upgrade comparison checks")
    if pre_post.run(timeout=3600) == State.FAIL:
        log.error("Failed post-check.")
        return State.FAIL
    cli_header("APIC post-upgrade health checks")
    if health.run(timeout=600) == State.FAIL:
        log.error("Failed health check.")
        return State.FAIL

    # Switch upgrades
    for group in config.get("firmware_groups", []):
        cli_header("Switch upgrade")
        if upgrade.upgrade_switches(group, 3600) == State.FAIL:
            log.error(f"Failed switch upgrade for group {group}.")
        cli_header("Switch post-upgrade comparison checks")
        if pre_post.run(timeout=3600) == State.FAIL:
            log.error("Failed post-check.")
            return State.FAIL
        cli_header("Switch post-upgrade health checks")
        if health.run(timeout=600) == State.FAIL:
            log.error("Failed health check.")
            return State.FAIL

    return State.OK


if __name__ == "__main__":
    main()
