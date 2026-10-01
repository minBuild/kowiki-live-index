class Topics:
    """파이프라인 토픽 이름. prefix로 여러 벌을 띄울 수 있다."""

    def __init__(self, prefix: str = ""):
        self.events = f"{prefix}doc.events"
        self.parsed = f"{prefix}doc.parsed"
        self.chunks = f"{prefix}doc.chunks"
        self.embeddings = f"{prefix}chunk.embeddings"

    def main(self) -> list[str]:
        return [self.events, self.parsed, self.chunks, self.embeddings]


def dlq(topic: str) -> str:
    return f"{topic}.dlq"
