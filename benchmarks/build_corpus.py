"""
Build a JSONL text corpus from Project Gutenberg HTML files in corpus/.

Strips HTML tags, skips <script>/<style> blocks, and removes the Gutenberg
license header/footer surrounding the actual book content.

Output: benchmarks/data/corpus.jsonl
Each record: {"key": "pg1342:000042", "value": "text line"}
"""

import json
import os
import re
import sys
from html.parser import HTMLParser

CORPUS_DIR = os.path.join(os.path.dirname(__file__), "..", "corpus")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "corpus.jsonl")

START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"

MIN_LINE_LENGTH = 10


class TextExtractor(HTMLParser):
    """Strips HTML, suppresses script/style/pg-header/pg-footer content."""

    _SKIP_TAGS = {"script", "style"}

    def __init__(self):
        super().__init__()
        self._skip_depth = 0
        self._pg_skip = False
        self.chunks = []

    def handle_starttag(self, tag, attrs):
        attr_dict = dict(attrs)
        tag_id = attr_dict.get("id", "")
        if tag in self._SKIP_TAGS or tag_id in ("pg-header", "pg-footer"):
            self._skip_depth += 1
        elif self._skip_depth == 0:
            self.chunks.append(" ")

    def handle_endtag(self, tag):
        attr_dict = {}  # endtag has no attrs; track by depth
        if self._skip_depth > 0:
            self._skip_depth -= 1
        else:
            self.chunks.append(" ")

    def handle_data(self, data):
        if self._skip_depth == 0:
            self.chunks.append(data)

    def get_text(self):
        return "".join(self.chunks)


def extract_book_text(html_content):
    """Return lines of main book text, stripping header/footer and HTML."""
    parser = TextExtractor()
    parser.feed(html_content)
    raw = parser.get_text()

    lines = raw.splitlines()

    # Find Gutenberg START/END markers to isolate book content
    start_idx = 0
    end_idx = len(lines)
    for i, line in enumerate(lines):
        if START_MARKER in line:
            start_idx = i + 1
            break
    for i in range(len(lines) - 1, -1, -1):
        if END_MARKER in lines[i]:
            end_idx = i
            break

    book_lines = lines[start_idx:end_idx]

    # Clean: collapse whitespace, filter short lines
    result = []
    for line in book_lines:
        line = re.sub(r"\s+", " ", line).strip()
        if len(line) >= MIN_LINE_LENGTH:
            result.append(line)

    return result


def book_id_from_filename(filename):
    """pg1342-images.html -> pg1342"""
    stem = os.path.splitext(os.path.basename(filename))[0]
    return stem.replace("-images", "")


def build_corpus():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    html_files = sorted(
        f for f in os.listdir(CORPUS_DIR) if f.endswith(".html")
    )
    if not html_files:
        print(f"No HTML files found in {CORPUS_DIR}", file=sys.stderr)
        sys.exit(1)

    total_lines = 0
    books_processed = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for filename in html_files:
            path = os.path.join(CORPUS_DIR, filename)
            book_id = book_id_from_filename(filename)

            with open(path, encoding="utf-8") as f:
                html = f.read()

            lines = extract_book_text(html)
            for line_num, text in enumerate(lines):
                record = {"key": f"{book_id}:{line_num:06d}", "value": text}
                out.write(json.dumps(record, ensure_ascii=False) + "\n")

            books_processed += 1
            total_lines += len(lines)
            print(f"  {book_id}: {len(lines):,} lines")

    size_kb = os.path.getsize(OUTPUT_FILE) // 1024
    print(f"\nProcessed {books_processed} books, {total_lines:,} lines written")
    print(f"Output: {OUTPUT_FILE} ({size_kb:,} KB)")


if __name__ == "__main__":
    build_corpus()
