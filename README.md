# Toshi Python Client
This repository is a client for the [Full-Text Search Engine Toshi](https://github.com/toshi-search/Toshi). It allows you to use all features implemented in the Toshi engine.

### How to install
The client is available as a package on Pypi and can be installed via:
```shell
pip install toshi_client
```

### Examples
For a full set of examples check out the [integration tests](https://github.com/SmartMonkey-git/py-toshi-client/blob/main/tests/test_integration_sync.py).

Here is a basic example:

1. Create an index and submit it

```python
from toshi_client.client import ToshiClient
from toshi_client.index.index_builder import IndexBuilder
from toshi_client.index.field_options import TextOptionIndexing

builder = IndexBuilder()

builder.add_text_field(name="lyrics", stored=True, indexing=TextOptionIndexing())
builder.add_i64_field(name="year", stored=True, indexed=True)
builder.add_u64_field(name="idx", stored=True, indexed=True)
builder.add_text_field(name="artist", stored=True, indexing=TextOptionIndexing())
builder.add_text_field(name="genre", stored=True, indexing=TextOptionIndexing())
builder.add_text_field(name="song", stored=True, indexing=TextOptionIndexing())
builder.add_facet_field(name="test_facet", stored=True)

index = builder.build("lyrics")
    
client = ToshiClient("http://localhost:8080")
client.create_index(index=index)
```

2. Create a document class according to the just created index

```python
from toshi_client.models.document import Document

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
    ):
        self.lyrics = lyrics
        self.year = year
        self.idx = idx
        self.artist = artist
        self.genre = genre
        self.song = song
```
3. Submit a document to Toshi

```python
doc = Lyrics(
    lyrics="Gold on the ceiling, I ain't blind, just a matter of time",
    year=2011,
    idx=2,
    artist="The Black Keys",
    genre="Rock",
    song="Gold on the Ceiling",
    test_facet="/a/b",
)
client.add_document(document=doc)
```
4. Use a query of your choice to retrieve it

```python
from toshi_client.query.term_query import TermQuery

query = TermQuery(term="ceiling", field_name="lyrics")
documents = client.search(query, Lyrics)
```
