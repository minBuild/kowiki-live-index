from kowiki.wiki_html import parse_article


def test_sections_keep_heading_path(article_html):
    sections = parse_article(article_html)

    assert [s.path for s in sections] == [[], ["역사"], ["역사", "근대"], ["교통"]]


def test_lead_has_infobox_and_paragraph_without_refs(article_html):
    lead = parse_article(article_html)[0]

    assert lead.blocks == [
        "인구 | 1,234명\n좌표 | 북위 37° / 동경 126°\n면적 | 12.3km2",
        "테스트 도시는 가상의 도시이다. 이 문서는 파서 시험용이다.",
    ]


def test_lists_and_tables_become_lines(article_html):
    sections = {tuple(s.path): s.blocks for s in parse_article(article_html)}

    assert sections[("역사", "근대")][1] == "- 1905년 역 개통\n- 1910년 시청 설립 임시 청사"
    assert sections[("교통",)] == ["노선 | 개통\n1호선 | 1974년", "틀이 만든 문단도 본문에 들어간다."]


def test_noise_and_reference_sections_are_dropped(article_html):
    text = "\n".join(b for s in parse_article(article_html) for b in s.blocks)

    for noise in (
        "다른 뜻에 대해서는",
        "도시 목록",
        "출처 내용",
        "공식 웹사이트",
        "둘러보기 상자",
        "역 사진",
        "infobox{",
    ):
        assert noise not in text


def test_redirect_page_has_no_sections():
    html = '<html><body><link rel="mw:PageProp/redirect" href="./서울특별시"/></body></html>'

    assert parse_article(html) == []
