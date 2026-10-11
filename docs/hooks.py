"""MkDocs hooks that publish the Markdown files of the repository as pages.

README.md, COMPONENTS.md and the other documents stay the single source of
truth and keep working when browsed on Codeberg. This hook splits them into
pages of the site along their headings, drops the emojis in front of the
headings and the horizontal rules between sections, and rewrites their links:
links to a section point to the page it ended up on, links to docs/assets/ to
the site's assets, and links to any other file of the repository to the file
on Codeberg.
"""

import posixpath
import re
from collections import defaultdict
from pathlib import Path

from markdown.extensions.toc import slugify
from mkdocs.structure.files import File, InclusionLevel

REPO = Path(__file__).resolve().parent.parent
ASSETS = REPO / "docs" / "assets"
SOURCE_URL = "https://codeberg.org/eljoth/tram/src/branch/main/"

# Title of every generated page, in the order of the navigation.
TITLES = {
    "guide/getting-started.md": "Getting Started",
    "guide/pipelines.md": "Pipelines",
    "guide/api.md": "HTTP API",
    "guide/configuration.md": "Configuration Sources",
    "guide/metrics.md": "Metrics",
    "guide/security.md": "Security",
    "components/index.md": "Components",
    "components/sources.md": "Sources",
    "components/stages.md": "Stages",
    "components/sinks.md": "Sinks",
    "kubernetes.md": "Kubernetes Operator",
    "benchmarks.md": "Benchmarks",
    "contributing.md": "Contributing",
}

# Where the sections of each document go, by heading without its emoji.
# A section takes its subsections along unless they are listed themselves;
# None drops it. Its heading becomes a second-level heading of the page, or,
# with UNWRAP, is dropped and its subsections take its place.
UNWRAP = True
SECTIONS = {
    "README.md": {
        "Tram: A Lightweight, Extensible Data Pipeline Framework": None,
        "Quick Start": ("guide/getting-started.md", UNWRAP),
        "API Endpoints": ("guide/api.md", False),
        "Updating a Stream": ("guide/api.md", False),
        "Metrics": ("guide/metrics.md", UNWRAP),
        "Example API Usage": ("guide/getting-started.md", False),
        "Core Concepts": ("guide/pipelines.md", UNWRAP),
        "Available Components": ("guide/pipelines.md", False),
        "Advanced Configuration": ("guide/pipelines.md", UNWRAP),
        "Configuration Sources": ("guide/configuration.md", UNWRAP),
        "Security": ("guide/security.md", UNWRAP),
    },
    "COMPONENTS.md": {
        "Components Documentation": ("components/index.md", UNWRAP),
        "Sources": ("components/sources.md", UNWRAP),
        "Available Sources": ("components/index.md", False),
        "Stages": ("components/stages.md", UNWRAP),
        "Available Stages": ("components/index.md", False),
        "Sinks": ("components/sinks.md", UNWRAP),
        "Available Sinks": ("components/index.md", False),
        "Notes": ("components/index.md", False),
    },
    "internal/adapters/k8scrd/README.md": {
        "Kubernetes Operator": ("kubernetes.md", UNWRAP),
    },
    "BENCHMARK.md": {
        "Benchmarks": ("benchmarks.md", UNWRAP),
    },
    "CONTRIBUTING.md": {
        "Contributing to Tram": ("contributing.md", UNWRAP),
        "Table of Contents": None,
    },
}

HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
EMOJI = re.compile(r"^[^\w\s`*\[(]+\s*")
RULE = re.compile(r"^\s*(-{3,}|\*{3,}|_{3,})\s*$")
FENCE = re.compile(r"^\s*(```|~~~)")
LINK = re.compile(r"(!?\[[^\]]*\]\()([^)\s]+)(\))")


def on_files(files, config):
    chunks, anchors = split_all()
    for page, title in TITLES.items():
        content = f"# {title}\n\n" + "".join(
            rewrite(source, page, text, anchors) for source, text in chunks[page]
        )
        files.append(File.generated(config, page, content=content))
    for asset in ASSETS.rglob("*"):
        if asset.is_file():
            files.append(
                File(
                    asset.relative_to(ASSETS.parent).as_posix(),
                    src_dir=str(ASSETS.parent),
                    dest_dir=config["site_dir"],
                    use_directory_urls=config["use_directory_urls"],
                    inclusion=InclusionLevel.NOT_IN_NAV,
                )
            )
    return files


def split_all():
    """Returns the parts of the documents per page, and for every (document,
    anchor) the page it is on and whether the page has the anchor."""
    chunks, anchors = defaultdict(list), {}
    for source, sections in SECTIONS.items():
        text = (REPO / source).read_text(encoding="utf-8")
        for page, part in split(source, text, sections, anchors):
            chunks[page].append((source, part))
    return chunks, anchors


def split(source, text, sections, anchors):
    # Stack of enclosing sections: (level, page, shift of heading levels).
    stack, page, shift, in_fence = [], None, 0, False
    current, parts = [], []

    def flush():
        if page and current:
            parts.append((page, "".join(current)))
        current.clear()

    for line in text.splitlines(keepends=True):
        heading = None if in_fence else HEADING.match(line)
        if FENCE.match(line):
            in_fence = not in_fence
        elif heading:
            level, title = len(heading[1]), EMOJI.sub("", heading[2])
            while stack and stack[-1][0] >= level:
                stack.pop()
            parent_page, parent_shift = (stack[-1][1], stack[-1][2]) if stack else (None, 0)

            unwrap = False
            if title in sections:
                target = sections[title]
                new_page, unwrap = target if target else (None, False)
                new_shift = (1 if unwrap else 2) - level
            else:
                new_page, new_shift = parent_page, parent_shift
            stack.append((level, new_page, new_shift))

            if new_page != page:
                flush()
                page, shift = new_page, new_shift
            shift = new_shift
            if page:
                # An unwrapped heading leaves no anchor; links to it go to the page.
                anchors.setdefault((source, slugify(title, "-")), (page, not unwrap))
            if page and not unwrap:
                current.append("#" * (level + shift) + " " + title + "\n")
            continue
        elif RULE.match(line):
            continue
        current.append(line)
    flush()
    return parts


def rewrite(source, page, text, anchors):
    lines, in_fence = [], False
    for line in text.splitlines(keepends=True):
        if FENCE.match(line):
            in_fence = not in_fence
        elif not in_fence:
            line = LINK.sub(lambda m: m[1] + target(source, page, m[2], anchors) + m[3], line)
        lines.append(line)
    return "".join(lines)


def target(source, page, link, anchors):
    if "://" in link or link.startswith("mailto:"):
        return link

    path, _, anchor = link.partition("#")
    # Codeberg keeps the dash an emoji at the start of a heading leaves in
    # its anchor (#-security); MkDocs drops it (#security).
    anchor = anchor.lstrip("-")
    path = posixpath.normpath(posixpath.join(posixpath.dirname(source), path)) if path else source

    if path in SECTIONS:
        dest, keep = anchors.get((path, anchor), (first_page(path), False))
        if not keep:
            anchor = ""
    elif path.startswith("docs/assets/"):
        dest = path.removeprefix("docs/")
    else:
        return SOURCE_URL + path + ("#" + anchor if anchor else "")

    suffix = "#" + anchor if anchor else ""
    if dest == page:
        return suffix or "#"
    return posixpath.relpath(dest, posixpath.dirname(page)) + suffix


def first_page(source):
    return next(target[0] for target in SECTIONS[source].values() if target)
