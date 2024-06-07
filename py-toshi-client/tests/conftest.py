import time

import docker
import pytest


@pytest.fixture(scope="session")
def toshi_container():
    client = docker.from_env()
    port = 8080

    container = client.containers.run(
        "toshi", name="toshi-test", ports={port: 8080}, detach=True
    )
    time.sleep(0.5)

    container = client.containers.get(container.name)
    ports = container.attrs["NetworkSettings"]["Ports"]
    host_url = ""
    for container_port, host_info in ports.items():
        if host_info:
            host_url = f"http://{host_info[0]['HostIp']}:{host_info[0]['HostPort']}"

    yield host_url

    container.stop()
    container.remove()
