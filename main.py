from util import config, login_loop_for, log, panic_gate, GatingEvent
import pre_post
import health
import upgrade


def main():
    """Initial entry point."""
    # Initialize API client
    client = login_loop_for(120, config)

    try:
        # Pre change checks
        panic_gate(lambda: pre_post.init(client, timeout=120), "pre/post check")
        panic_gate(lambda: health.run(client, timeout=120), "health check")

        # Pre upgrade prep
        panic_gate(lambda: upgrade.backup(client, 600), "backup")
        panic_gate(lambda: upgrade.tech_support(client, 600), "tech support")

        # APIC upgrade
        panic_gate(lambda: upgrade.upgrade_apics(client, 3600), "APIC upgrade")
        panic_gate(lambda: pre_post.run(client, timeout=3600), "pre/post check")
        panic_gate(lambda: health.run(client, timeout=600), "health check")

        # Switch upgrades
        for group in client.args["firmware_groups"]:
            panic_gate(
                lambda: upgrade.upgrade_switches(group, client, 3600),
                f"{group} upgrade",
            )
            panic_gate(lambda: pre_post.run(client, timeout=3600), "pre/post check")
            panic_gate(lambda: health.run(client, timeout=600), "health check")

    except GatingEvent as e:
        log.error(f"Failed upgrade on {e}.")
        return 1


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
