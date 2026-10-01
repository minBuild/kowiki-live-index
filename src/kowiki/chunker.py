import re
from collections.abc import Callable

from kowiki.ids import chunk_ids
from kowiki.models import Chunk, Section
from kowiki.settings import Settings

SENTENCE_END = re.compile(r"(?<=[.!?。])\s+")

TokenCounter = Callable[[str], int]


def approx_tokens(text: str) -> int:
    """한국어 기준으로 두 글자에 한 토큰 정도로 어림한다."""
    return len(text) // 2 + 1


def make_counter(tokenizer: str | None) -> TokenCounter:
    if not tokenizer:
        return approx_tokens
    from tokenizers import Tokenizer

    tok = Tokenizer.from_pretrained(tokenizer)
    return lambda text: len(tok.encode(text, add_special_tokens=False).ids)


def build_chunks(doc_id: str, pieces: list[tuple[str, str]], count: TokenCounter) -> list[Chunk]:
    inputs = [f"{heading}\n{text}" for heading, text in pieces]
    ids = chunk_ids(doc_id, inputs)
    return [
        Chunk(id=cid, ordinal=i, heading=heading, text=text, tokens=count(inp))
        for i, (cid, inp, (heading, text)) in enumerate(zip(ids, inputs, pieces, strict=True))
    ]


class SectionChunker:
    """섹션 경계를 넘지 않게 문단을 묶어 청크를 만든다."""

    def __init__(self, max_tokens: int = 400, overlap_tokens: int = 40, count: TokenCounter = approx_tokens):
        self.max_tokens = max_tokens
        self.overlap_tokens = overlap_tokens
        self.count = count

    def __call__(self, doc_id: str, title: str, sections: list[Section]) -> list[Chunk]:
        pieces = []
        for section in sections:
            heading = " > ".join([title, *section.path])
            budget = max(self.max_tokens - self.count(heading), 32)
            pieces += [(heading, text) for text in self._pack(section.blocks, budget)]
        return build_chunks(doc_id, pieces, self.count)

    def _pack(self, blocks: list[str], budget: int) -> list[str]:
        out, current, size = [], [], 0
        for block in blocks:
            n = self.count(block) + 1
            if n > budget:
                if current:
                    out.append("\n".join(current))
                    current, size = [], 0
                out += self._split(block, budget)
                continue
            if current and size + n > budget:
                out.append("\n".join(current))
                current, size = [], 0
            current.append(block)
            size += n
        if current:
            out.append("\n".join(current))
        return out

    def _split(self, text: str, budget: int) -> list[str]:
        units = [u for line in text.split("\n") for u in SENTENCE_END.split(line) if u.strip()]
        out, current, size = [], [], 0
        for unit in units:
            for piece in self._cut(unit, budget - 1):
                n = self.count(piece) + 1
                if current and size + n > budget:
                    out.append(" ".join(current))
                    current, size = self._tail(current, budget - n)
                current.append(piece)
                size += n
        if current:
            out.append(" ".join(current))
        return out

    def _tail(self, units: list[str], room: int) -> tuple[list[str], int]:
        kept, size = [], 0
        for unit in reversed(units):
            n = self.count(unit) + 1
            if size + n > min(self.overlap_tokens, room):
                break
            kept.insert(0, unit)
            size += n
        return kept, size

    def _cut(self, unit: str, budget: int) -> list[str]:
        n = self.count(unit)
        if n <= budget:
            return [unit]
        width = max(1, len(unit) * budget // n)
        while width > 1 and self.count(unit[:width]) > budget:
            width = width * 9 // 10
        return [unit[i : i + width] for i in range(0, len(unit), width)]


class FixedChunker:
    """본문을 고정 길이로 자른다. 섹션 청킹과 비교하는 용도."""

    def __init__(self, max_tokens: int = 400, overlap_tokens: int = 40, count: TokenCounter = approx_tokens):
        self.max_tokens = max_tokens
        self.overlap_tokens = overlap_tokens
        self.count = count

    def __call__(self, doc_id: str, title: str, sections: list[Section]) -> list[Chunk]:
        text = "\n".join(block for section in sections for block in section.blocks)
        if not text:
            return []
        chars_per_token = len(text) / self.count(text)
        width = max(1, int((self.max_tokens - self.count(title)) * chars_per_token))
        step = max(1, width - int(self.overlap_tokens * chars_per_token))
        pieces = [(title, text[i : i + width]) for i in range(0, len(text), step)]
        return build_chunks(doc_id, pieces, self.count)


def make_chunker(settings: Settings) -> SectionChunker | FixedChunker:
    cls = SectionChunker if settings.chunk_strategy == "section" else FixedChunker
    return cls(settings.chunk_max_tokens, settings.chunk_overlap_tokens, make_counter(settings.tokenizer))
