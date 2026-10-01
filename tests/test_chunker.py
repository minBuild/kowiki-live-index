from kowiki.chunker import FixedChunker, SectionChunker, approx_tokens
from kowiki.models import Section

LONG = " ".join(f"{i}번째 문장은 청크를 나누는 시험에 쓰인다." for i in range(60))


def sections(history: str) -> list[Section]:
    return [
        Section(path=[], blocks=["테스트 도시는 가상의 도시이다."]),
        Section(path=["역사"], blocks=[history]),
        Section(path=["교통"], blocks=["1호선이 지나간다.", "버스 노선이 많다."]),
    ]


def test_chunks_respect_token_budget():
    chunks = SectionChunker(max_tokens=80, overlap_tokens=10)("kowiki:1", "테스트 도시", sections(LONG))

    assert len(chunks) > 3
    assert all(c.tokens <= 80 for c in chunks)
    assert [c.ordinal for c in chunks] == list(range(len(chunks)))


def test_chunks_do_not_cross_sections():
    chunks = SectionChunker(max_tokens=80)("kowiki:1", "테스트 도시", sections(LONG))

    assert {c.heading for c in chunks} == {"테스트 도시", "테스트 도시 > 역사", "테스트 도시 > 교통"}
    assert (
        next(c for c in chunks if c.heading.endswith("교통")).text == "1호선이 지나간다.\n버스 노선이 많다."
    )


def test_long_paragraph_is_split_with_overlap():
    chunks = SectionChunker(max_tokens=80, overlap_tokens=20)("kowiki:1", "테스트 도시", sections(LONG))
    history = [c.text for c in chunks if c.heading.endswith("역사")]

    first_tail = history[0].split(". ")[-1]
    assert history[1].startswith(first_tail)


def test_same_input_gives_same_ids():
    chunker = SectionChunker(max_tokens=80)

    first = chunker("kowiki:1", "테스트 도시", sections(LONG))
    second = chunker("kowiki:1", "테스트 도시", sections(LONG))

    assert [c.id for c in first] == [c.id for c in second]
    assert len({c.id for c in first}) == len(first)


def test_editing_one_section_keeps_other_chunk_ids():
    chunker = SectionChunker(max_tokens=80)
    before = chunker("kowiki:1", "테스트 도시", sections(LONG))
    after = chunker("kowiki:1", "테스트 도시", sections("역사 문단을 통째로 고쳤다."))

    def ids(chunks, heading):
        return {c.id for c in chunks if c.heading == heading}

    assert ids(before, "테스트 도시") == ids(after, "테스트 도시")
    assert ids(before, "테스트 도시 > 교통") == ids(after, "테스트 도시 > 교통")
    assert not ids(before, "테스트 도시 > 역사") & ids(after, "테스트 도시 > 역사")


def test_fixed_chunks_all_change_when_text_is_inserted_in_front():
    chunker = FixedChunker(max_tokens=80, overlap_tokens=0)
    before = chunker("kowiki:1", "테스트 도시", [Section(path=[], blocks=[LONG])])
    after = chunker("kowiki:1", "테스트 도시", [Section(path=[], blocks=["앞에 한 문장을 더했다. " + LONG])])

    assert len(before) > 3
    assert not {c.id for c in before} & {c.id for c in after}


def test_approx_tokens_is_roughly_two_chars_per_token():
    assert approx_tokens("가" * 100) == 51
