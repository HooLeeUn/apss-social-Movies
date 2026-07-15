import csv
from contextlib import contextmanager
from pathlib import Path

from django.core.management.base import CommandError


CSV_DELIMITER_LABELS = {
    ";": "semicolon",
    ",": "comma",
    "\t": "tab",
}


def delimiter_label(delimiter):
    return CSV_DELIMITER_LABELS.get(delimiter, delimiter)


def normalize_csv_fieldnames(fieldnames):
    if fieldnames is None:
        return fieldnames
    return [fieldname.strip().lstrip("\ufeff") if fieldname is not None else fieldname for fieldname in fieldnames]


def detect_csv_delimiter(path, sample_size=4096):
    csv_path = Path(path)
    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        sample = csv_file.read(sample_size)

    if not sample:
        raise CommandError(f"CSV vacío: {csv_path}")

    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t").delimiter
    except csv.Error:
        header = sample.splitlines()[0] if sample.splitlines() else ""
        if ";" in header:
            return ";"
        if "," in header:
            return ","
        if "\t" in header:
            return "\t"
        raise CommandError(
            "No se pudo detectar el delimitador del CSV. "
            "Se esperaba coma (,), punto y coma (;) o tabulación."
        )


@contextmanager
def open_csv_dict_reader(path):
    csv_path = Path(path)
    delimiter = detect_csv_delimiter(csv_path)
    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        csv_file.seek(0)
        reader = csv.DictReader(csv_file, delimiter=delimiter)
        reader.fieldnames = normalize_csv_fieldnames(reader.fieldnames)
        yield reader, delimiter
