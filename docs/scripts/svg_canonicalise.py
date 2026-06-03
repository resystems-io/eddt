#!/usr/bin/env python3
"""Canonicalise <defs> child ordering in dvisvgm-generated SVG files.

dvisvgm 3.2.1 emits font-subset glyph definitions in non-deterministic
order even from identical PDF input, producing byte-different SVG files on
each build. Sort all self-closing child elements within each <defs> block
by their id attribute value so repeated builds are byte-identical.
"""
import re
import sys


def sort_key(line: str) -> str:
    m = re.search(r"id='([^']+)'", line)
    return m.group(1) if m else line


def canonicalise(path: str) -> None:
    text = open(path).read()

    def replace_defs(m: re.Match) -> str:
        entries = [l for l in m.group(1).splitlines() if l.strip()]
        return '<defs>\n' + '\n'.join(sorted(entries, key=sort_key)) + '\n</defs>'

    text = re.sub(r'<defs>(.*?)</defs>', replace_defs, text, flags=re.DOTALL)
    open(path, 'w').write(text)


if __name__ == '__main__':
    for path in sys.argv[1:]:
        canonicalise(path)
