import csv
import datetime
import io
import zipfile
from collections import OrderedDict
from pathlib import Path

import rows
from rows.utils import load_schema
from scrapy import Request, Spider

csv.field_size_limit(1024 ** 2)


class FixCSVWrapper(io.TextIOWrapper):
    def read(self, *args, **kwargs):
        data = super().read(*args, **kwargs)
        for first, second in self.replace:
            data = data.replace(first, second)
        return data

    def readline(self, *args, **kwargs):
        data = super().readline(*args, **kwargs)
        for first, second in self.replace:
            data = data.replace(first, second)
        return data


class FixCSVWrapper2011(FixCSVWrapper):
    replace = ((';"SN;', ";SN;"),)


class FixCSVWrapper2018(FixCSVWrapper):
    replace = ((';"LUPA', ";LUPA"), (';"EMBAIXADA', ";EMBAIXADA"))


CSV_WRAPPERS = {
    2011: FixCSVWrapper2011,
    2018: FixCSVWrapper2018,
    None: io.TextIOWrapper,
}


class DocumentField(rows.fields.TextField):
    chars_to_remove = (" ", ".", "-", "/")

    @classmethod
    def deserialize(cls, value):
        for char in cls.chars_to_remove:
            value = value.replace(char, "")
        return super().deserialize(value.strip())


def get_schema():
    schema = load_schema("schema.csv")
    # `load_schema` does not support custom fields yet so we must force it
    # TODO: pass `context` to `load_schema` so we don't need to override
    # 'txtcnpjcpf' type.
    schema["txtcnpjcpf"] = DocumentField
    return schema


def open_file(year, filename, encoding="utf-8-sig"):
    zip_file = zipfile.ZipFile(filename)
    assert (
        len(zip_file.filelist) == 1
    ), f"More than one file found on ZIP archive {filename}"
    internal_name = zip_file.filelist[0].filename
    extracted_year = int(internal_name.split("-")[1].split(".")[0])
    assert (
        year == extracted_year
    ), f"Expected CSV for {year} ({extracted_year} found) on {filename}"

    if year not in CSV_WRAPPERS:
        year = None
    return CSV_WRAPPERS[year](zip_file.open(internal_name), encoding=encoding)


def dict_to_lower(dictionary):
    return {key.lower(): value for key, value in dictionary.items()}


def read_file(year, filename):
    fobj = open_file(year, filename)
    reader = csv.DictReader(
        fobj, delimiter=";"
    )  # TODO: may detect dialect automatically
    schema = get_schema()
    for row in reader:
        # TODO: may change key names in future instead of only moving to lowercase
        row = dict_to_lower(row)
        assert (
            int(row["numano"]) == year
        ), f"row has year {row['numano']} (expecting {year})"
        if not row["sgpartido"]:
            row["sgpartido"] = row["txnomeparlamentar"].replace("LIDERANÇA DO ", "")
        yield {
            field_name: Field.deserialize(row[field_name])
            for field_name, Field in schema.items()
        }


class CotaParlamentarCamaraFederalSpider(Spider):

    name = "cota-parlamentar-camara-federal"
    download_path = Path("./data/download")  # TODO: use settings?

    def __init__(self, years=None):
        super().__init__()
        if years is not None:
            self.years = [int(year) for year in years.split(",")]
        else:
            first_year, last_year = 2009, datetime.datetime.now().year
            self.years = range(last_year, first_year - 1, -1)

    def start_requests(self):
        if not self.download_path.exists():
            self.download_path.mkdir()

        for year in self.years:
            # If the file already exists, do not download it again
            filename = f"Ano-{year}.csv.zip"
            full_filename = self.download_path / filename
            if full_filename.exists():
                local_file = True
                url = "file://" + str(full_filename.absolute())
            else:
                local_file = False
                url = f"http://www.camara.leg.br/cotas/{filename}"

            yield Request(
                url=url,
                callback=self.parse_year,
                meta={
                    "filename": full_filename,
                    "year": year,
                    "local_file": local_file,
                },
            )

    def parse_year(self, response):
        meta = response.request.meta
        filename, year = meta["filename"], meta["year"]

        if not meta["local_file"]:  # Save local file if not saved yet
            with open(filename, mode="wb") as fobj:
                fobj.write(response.body)

        yield from read_file(year, filename)
