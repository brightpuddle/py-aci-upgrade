from util import config, login_loop_for, log, panic_gate, GatingEvent, State
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

    try:
        # Pre change checks
        cli_header("Pre-change snapshot")
        panic_gate(lambda: pre_post.init(timeout=120), "pre/post check")
        cli_header("Pre-change health check")
        panic_gate(lambda: health.run(timeout=120), "health check")

        # Pre upgrade prep
        cli_header("Configuration backup")
        panic_gate(lambda: upgrade.backup(600), "backup")
        cli_header("Tech support")
        panic_gate(lambda: upgrade.tech_support(600), "tech support")

        # APIC upgrade
        cli_header("APIC upgrade")
        panic_gate(lambda: upgrade.upgrade_apics(3600), "APIC upgrade")
        cli_header("APIC post-upgrade comparison checks")
        panic_gate(lambda: pre_post.run(timeout=3600), "pre/post check")
        cli_header("APIC post-upgrade health checks")
        panic_gate(lambda: health.run(timeout=600), "health check")

        # Switch upgrades
        for group in config['firmware_groups']:
            cli_header("Switch upgrade")
            panic_gate(
                lambda: upgrade.upgrade_switches(group, 3600),
                f"{group} upgrade",
            )
            cli_header("Switch post-upgrade comparison checks")
            panic_gate(lambda: pre_post.run(timeout=3600), "pre/post check")
            cli_header("Switch post-upgrade health checks")
            panic_gate(lambda: health.run(timeout=600), "health check")

    except GatingEvent as e:
        log.error(f"Failed upgrade on {e}.")
        return State.FAIL
    return State.OK


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
