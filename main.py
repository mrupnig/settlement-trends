from pathlib import Path
from preprocessing.clean_raw_data import clean_text_lines

FILE_PATH = Path("data/raw/list_of_plates.txt")
LINE_BEGINNING = "Plate"

clean_text_lines(FILE_PATH, LINE_BEGINNING)
