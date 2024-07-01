import os
import time

import docker
import pytest

from toshi_client.index.field_options import TextOptionIndexing
from toshi_client.index.index_builder import IndexBuilder
from toshi_client.models.document import Document

CI = bool(os.environ.get("CI"))


@pytest.fixture(scope="module")
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


class Lyrics(Document):
    @staticmethod
    def index_name() -> str:
        return "lyrics"

    def __init__(
        self,
        lyrics: str,
        year: int,
        idx: int,
        artist: str,
        genre: str,
        song: str,
        test_facet: str,
    ):
        self.lyrics = lyrics
        self.year = year
        self.idx = idx
        self.artist = artist
        self.genre = genre
        self.song = song
        self.test_facet = test_facet


@pytest.fixture
def black_keys_lyrics_document():
    return Lyrics(
        lyrics="Gold on the ceiling, I ain't blind, just a matter of time",
        year=2011,
        idx=2,
        artist="The Black Keys",
        genre="Rock",
        song="Gold on the Ceiling",
        test_facet="/a/b",
    )


@pytest.fixture
def nirvana_lyrics_document():
    return Lyrics(
        lyrics="With the lights out, it's less dangerous, here we are now, entertain us",
        year=1991,
        idx=4,
        artist="Nirvana",
        genre="Grunge",
        song="Smells Like Teen Spirit",
        test_facet="/a/b",
    )


@pytest.fixture
def radiohead_lyrics_document():
    return Lyrics(
        lyrics="I'm a creep, I'm a weirdo, what the hell am I doing here?",
        year=1992,
        idx=3,
        artist="Radiohead",
        genre="Alternative Rock",
        song="Creep",
        test_facet="/a/b",
    )


@pytest.fixture
def lyric_documents(
    black_keys_lyrics_document, nirvana_lyrics_document, radiohead_lyrics_document
):
    return [
        black_keys_lyrics_document,
        nirvana_lyrics_document,
        radiohead_lyrics_document,
    ]


@pytest.fixture
def lyrics_index():
    builder = IndexBuilder()

    builder.add_text_field(name="lyrics", stored=True, indexing=TextOptionIndexing())
    builder.add_i64_field(name="year", stored=True, indexed=True)
    builder.add_u64_field(name="idx", stored=True, indexed=True)
    builder.add_text_field(name="artist", stored=True, indexing=TextOptionIndexing())
    builder.add_text_field(name="genre", stored=True, indexing=TextOptionIndexing())
    builder.add_text_field(name="song", stored=True, indexing=TextOptionIndexing())
    builder.add_facet_field(name="test_facet", stored=True)

    return builder.build("lyrics")
