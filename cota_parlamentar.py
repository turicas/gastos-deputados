import csv
import datetime
import io
import zipfile
from collections import OrderedDict
from pathlib import Path

import rows
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


class MoneyField(rows.fields.TextField):
    @classmethod
    def deserialize(cls, value):
        value = value.replace(".", "").replace(",", ".")
        return super().deserialize(value)


class DocumentField(rows.fields.TextField):
    @classmethod
    def deserialize(cls, value):
        value = value.replace(" ", "").replace(".", "").replace("-", "").strip()
        return super().deserialize(value)


# TODO: may read the schema from a file
CSV_SCHEMA = OrderedDict(
    [
        ("codLegislatura", rows.fields.IntegerField),
        ("datEmissao", rows.fields.DatetimeField),
        ("ideDocumento", rows.fields.IntegerField),
        ("ideCadastro", rows.fields.IntegerField),
        ("indTipoDocumento", rows.fields.IntegerField),
        ("nuCarteiraParlamentar", rows.fields.IntegerField),
        ("nuDeputadoId", rows.fields.IntegerField),
        ("nuLegislatura", rows.fields.IntegerField),
        ("numAno", rows.fields.IntegerField),
        ("numEspecificacaoSubCota", rows.fields.IntegerField),
        ("numLote", rows.fields.IntegerField),
        ("numMes", rows.fields.IntegerField),
        ("numParcela", rows.fields.IntegerField),
        ("numRessarcimento", rows.fields.IntegerField),
        ("numSubCota", rows.fields.IntegerField),
        ("sgPartido", rows.fields.TextField),
        ("sgUF", rows.fields.TextField),
        ("txNomeParlamentar", rows.fields.TextField),
        ("txtCNPJCPF", DocumentField),
        ("txtDescricao", rows.fields.TextField),
        ("txtDescricaoEspecificacao", rows.fields.TextField),
        ("txtFornecedor", rows.fields.TextField),
        ("txtNumero", rows.fields.TextField),
        ("txtPassageiro", rows.fields.TextField),
        ("txtTrecho", rows.fields.TextField),
        ("vlrDocumento", MoneyField),
        ("vlrGlosa", MoneyField),
        ("vlrLiquido", MoneyField),
        ("vlrRestituicao", MoneyField),
    ]
)


def convert_field(FieldClass):
    if FieldClass is MoneyField:
        return rows.fields.DecimalField
    elif FieldClass is DocumentField:
        return rows.fields.TextField
    else:
        return FieldClass


# The current rows implementation does not know how to export `MoneyField` and
# `DocumentField` to SQLite (only knows rows.fields.*Field classes), so we need
# to have a specific schema for the `Table` object. In the future, the library
# should detect from the values produced by the class or by inspecting it.
FINAL_SCHEMA = OrderedDict(
    [(field_name, convert_field(Field)) for field_name, Field in CSV_SCHEMA.items()]
)


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


def read_file(year, filename):
    fobj = open_file(year, filename)
    reader = csv.DictReader(fobj, delimiter=";")
    for row in reader:
        assert (
            int(row["numAno"]) == year
        ), f"row has year {row['numAno']} (expecting {year})"
        if not row["sgPartido"]:
            row["sgPartido"] = row["txNomeParlamentar"].replace("LIDERANÇA DO ", "")
        yield [
            Field.deserialize(row[field_name])
            for field_name, Field in CSV_SCHEMA.items()
        ]


def parse_table(year, filename):
    table = rows.Table(fields=FINAL_SCHEMA)
    table._rows = read_file(year, filename)
    return table


class CotaParlamentarCamaraSpider(Spider):

    name = "cota-parlamentar-camara"
    download_path = Path("./data/download")

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

        table = parse_table(year, filename)
        header = table.field_names
        for row in table._rows:
            yield dict(zip(header, row))
