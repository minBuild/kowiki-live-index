from kowiki.blobstore import FileBlobStore, put_text
from kowiki.chunker import SectionChunker
from kowiki.embedder import HashEmbedder
from kowiki.models import DocEvent, Op
from kowiki.stages import ChunkStep, EmbedStep, IndexStep, ParseStep
from tests.helpers import chunked_doc


class CountingEmbedder(HashEmbedder):
    def __init__(self):
        super().__init__(16)
        self.texts: list[str] = []

    def embed(self, texts):
        self.texts += texts
        return super().embed(texts)


def test_parse_step_reads_blob(tmp_path, article_html):
    blobs = FileBlobStore(tmp_path)
    uri = put_text(blobs, "raw/kowiki/1/10.html.gz", article_html)
    event = DocEvent(
        doc_id="kowiki:1", op=Op.UPSERT, rev_id=10, event_ts=1, title="테스트 도시", blob_uri=uri
    )

    [parsed] = ParseStep(blobs)([event])

    assert parsed.doc_id == "kowiki:1"
    assert parsed.sections[1].path == ["역사"]


def test_delete_passes_through_every_step(index):
    deleted = DocEvent(doc_id="kowiki:1", op=Op.DELETE, rev_id=11, event_ts=2)

    [parsed] = ParseStep(None)([deleted])
    [chunked] = ChunkStep(SectionChunker())([parsed])
    [embedded] = EmbedStep(HashEmbedder(16), index)([chunked])

    assert parsed.sections == chunked.chunks == embedded.chunks == []
    assert embedded.op is Op.DELETE


def test_embed_step_reuses_vectors_of_indexed_chunks(index):
    embedder = CountingEmbedder()
    step = EmbedStep(embedder, index)
    IndexStep(index)(step([chunked_doc(1, ["역사", "교통"])]))
    embedder.texts.clear()

    [embedded] = step([chunked_doc(2, ["역사", "문화"])])

    assert embedder.texts == ["문서 > 문화\n문화"]
    assert len(embedded.chunks) == 2
    assert all(c.vector for c in embedded.chunks)
