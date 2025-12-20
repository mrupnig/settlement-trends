import re


def clean_text_lines(file_path, line_beginning):
    FILE_PATH = file_path
    LINE_BEGINNING = line_beginning
    # Entfernt "... 123" bzw. "...123"
    dots_page_pattern = re.compile(r"\.\.\.\s*\d{3}")

    lines = FILE_PATH.read_text(encoding="utf-8").splitlines()

    result_lines = []
    current_line = None

    for raw in lines:
    # 1) Muster entfernen und trimmen
        line = dots_page_pattern.sub("", raw).strip()

    # 2) Leere Zeilen überspringen
        if not line:
            continue

    # 3) Neue Figure-Zeile beginnt
        if line.startswith(LINE_BEGINNING):
        # vorherige Zeile abschließen
            if current_line is not None:
                result_lines.append(current_line)
            current_line = line
        else:
        # 4) An vorherige Zeile anhängen
            if current_line is not None:
                current_line = f"{current_line} {line}"
            else:
            # Datei beginnt unerwartet ohne "Figure"
                current_line = f"{LINE_BEGINNING} {line}"

# letzte Zeile anhängen
    if current_line is not None:
        result_lines.append(current_line)

    FILE_PATH.write_text("\n".join(result_lines) + "\n", encoding="utf-8")