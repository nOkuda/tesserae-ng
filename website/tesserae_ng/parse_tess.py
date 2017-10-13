"""Parsing code for Tesserae texts"""
import re


def _parse_tess_line(text_value):
    """
    Read and parse text from the user, must be in .tess format
    """

    if text_value is None:
        return (None, None)

    # Input files must be in .tess format
    lines = []
    rex = r'^<([^>]+)>[\t](.*)$'
    lrex = r'([0-9]+)(-([0-9]+))?$'

    for line in re.split(r'[\n\r]+', text_value):
        line = line.strip()
        if len(line) == 0:
            continue
        match = re.match(rex, line)
        if match is None:
            continue
        left = match.group(1)
        right = match.group(2)

        line_info = re.search(lrex, left)
        if line_info is not None:
            start = line_info.group(1)
            if line_info.group(3) is not None:
                end = line_info.group(3)
            else:
                end = start
        else:
            start = 0
            end = 0

        lines.append((right, start, end))

    full_text = '\n'.join([l[0] for l in lines])

    return (full_text, lines)


def _parse_tess_phrase(text_value):
    """
    Read and parse text from the user, must be in .tess format
    """

    if text_value is None:
        return (None, None)

    (full_text, lines) = _parse_tess_line(text_value)

    sentences = []
    phrase_delimiter = r'([.?!;:])'
    only_delimeter = re.compile(r'^[.?!;:]$')

    current_sentence = None
    for text, start, end in lines:
        text = text.strip()
        parts = re.split(phrase_delimiter, text)

        for part in parts:
            if only_delimeter.match(part) is not None:
                # This is a delimeter
                if current_sentence is None:
                    # Ignore it
                    pass
                else:
                    sentence = (current_sentence[0] + part).strip()
                    sent_start = current_sentence[1]
                    sent_end = end
                    if len(current_sentence[0].strip()) > 0:
                        sentences.append((sentence, sent_start, sent_end))
                    current_sentence = None
            else:
                # Not a delimeter
                if current_sentence is None:
                    current_sentence = (part, start, end)
                else:
                    current_sentence = (
                        current_sentence[0] + ' ' + part, current_sentence[1],
                        end)

    return (full_text, sentences)


TESS_MODES = {
    'line': _parse_tess_line,
    'phrase': _parse_tess_phrase,
}
