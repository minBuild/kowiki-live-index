import re

from bs4 import BeautifulSoup, NavigableString, Tag

from kowiki.models import Section

SKIP_SECTIONS = {
    "각주",
    "주석",
    "출처",
    "참고 문헌",
    "참고 자료",
    "참조",
    "외부 링크",
    "같이 보기",
    "관련 항목",
    "읽을거리",
    "더 읽어보기",
}
NOISE = ", ".join(
    [
        "style",
        "script",
        "link",
        "meta",
        "sup.mw-ref",
        ".mw-references-wrap",
        "ol.references",
        ".navbox",
        ".navbox-styles",
        ".navbar",
        ".sidebar",
        ".hatnote",
        ".side-box",
        ".metadata",
        ".ambox",
        ".mw-empty-elt",
        "figure",
        ".thumb",
        ".gallery",
        ".mwe-math-element",
        ".noprint",
        "[typeof~='mw:Extension/mapframe']",
    ]
)
HEADINGS = {"h2", "h3", "h4", "h5", "h6"}
BREAKS = {"br", "p", "div", "li", "tr", "td", "th", "dt", "dd", "ul", "ol", "dl", "table"}
SPACES = re.compile(r"\s+")
INVISIBLE = dict.fromkeys(map(ord, "\u200b\ufeff"))


def parse_article(html: str) -> list[Section]:
    """Parsoid HTML을 제목 경로가 붙은 섹션 목록으로 바꾼다. 넘겨주기 문서는 빈 목록."""
    soup = BeautifulSoup(html, "lxml")
    if soup.find("link", rel="mw:PageProp/redirect"):
        return []
    body = soup.body or soup
    for node in body.select(NOISE):
        node.decompose()
    sections: list[Section] = []
    _walk(body, [], sections, None)
    return [s for s in sections if s.blocks]


def _walk(node: Tag, path: list[str], out: list[Section], current: Section | None) -> None:
    if current is None:
        current = Section(path=path, blocks=[])
        out.append(current)
    for child in node.children:
        if not isinstance(child, Tag):
            continue
        if child.name == "section":
            title = _heading(child)
            if title is None:
                _walk(child, path, out, current)
            elif title not in SKIP_SECTIONS:
                _walk(child, [*path, title], out, None)
        elif child.name == "div":
            _walk(child, path, out, current)
        elif child.name in ("p", "blockquote", "pre"):
            _add(current, _text(child))
        elif child.name in ("ul", "ol"):
            items = [t for li in child.find_all("li", recursive=False) if (t := _text(li))]
            _add(current, "\n".join(f"- {t}" for t in items))
        elif child.name == "dl":
            _add(
                current,
                "\n".join(t for x in child.find_all(["dt", "dd"], recursive=False) if (t := _text(x))),
            )
        elif child.name == "table":
            _add(current, _table(child))


def _heading(section: Tag) -> str | None:
    first = next((c for c in section.children if isinstance(c, Tag)), None)
    if first is None or first.name not in HEADINGS:
        return None
    return _text(first) or None


def _table(table: Tag) -> str:
    rows = []
    for tr in table.find_all("tr"):
        if tr.find_parent("table") is not table:
            continue
        cells = [t for cell in tr.find_all(["th", "td"], recursive=False) if (t := _text(cell))]
        if cells:
            rows.append(" | ".join(cells))
    return "\n".join(rows)


def _text(node: Tag) -> str:
    parts = []
    for item in node.descendants:
        if type(item) is NavigableString:
            parts.append(str(item))
        elif isinstance(item, Tag) and item.name in BREAKS:
            parts.append(" ")
    return SPACES.sub(" ", "".join(parts).translate(INVISIBLE)).strip()


def _add(section: Section, text: str) -> None:
    if text:
        section.blocks.append(text)
