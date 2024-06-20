import os
import time

import docker
import pytest

CI = bool(os.environ.get("CI"))


@pytest.fixture(scope="session")
def toshi_container():
    container_name = "toshi-test"
    client = docker.from_env()
    port = 8080

    # Check if container is running
    running_containers = client.containers.list()
    for container in running_containers:
        # If it is running delete it for a clean test env
        if container.name == container_name:
            container.stop()
            container.remove()

    container = client.containers.run(
        image="toshi", name=container_name, ports={port: port}, detach=True
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
