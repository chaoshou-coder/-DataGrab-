from datetime import datetime

from datagrab.pipeline.downloader import Downloader
from datagrab.timeutils import BEIJING_TZ


def test_split_range_chunks():
    downloader = Downloader(source=None, writer=None, concurrency=1, batch_days=10, startup_jitter_max=0)
    start = datetime(2024, 1, 1, tzinfo=BEIJING_TZ)
    end = datetime(2024, 1, 21, tzinfo=BEIJING_TZ)
    chunks = list(downloader._split_range(start, end))

    assert len(chunks) == 2
    assert chunks[0][0].date() == start.date()
    assert chunks[-1][1].date() == end.date()
    for chunk_start, chunk_end in chunks:
        assert (chunk_end - chunk_start).days <= 10
