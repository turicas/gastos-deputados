import csv
import io
import lzma
import pathlib
import zipfile
from collections import OrderedDict

import requests
import rows
from tqdm import tqdm


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


def load_file(year, download_path, encoding="utf-8"):
    filename = download_path / f"Ano-{year}.csv.zip"
    if not filename.exists():
        url = f'http://www.camara.leg.br/cotas/{filename.name}'
        response = requests.get(url)
        with open(filename, mode='wb') as fobj:
            fobj.write(response.content)

    zip_file = zipfile.ZipFile(filename)
    if year == 2011:
        return FixCSVWrapper2011(zip_file.open(f"Ano-{year}.csv"), encoding=encoding)
    elif year == 2018:
        return FixCSVWrapper2018(zip_file.open(f"Ano-{year}.csv"), encoding=encoding)
    else:
        return io.TextIOWrapper(zip_file.open(f"Ano-{year}.csv"), encoding=encoding)


def create_table(year, download_path):

    # TODO: may read the schema from a file
    schema = OrderedDict(
        [
            ("codLegislatura", rows.fields.IntegerField),
            ("datEmissao", rows.fields.DatetimeField),
            ("ideDocumento", rows.fields.IntegerField),
            ("idecadastro", rows.fields.IntegerField),
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

    # The current rows implementation does not know how to export `MoneyField`
    # and `DocumentField` to SQLite (only knows rows.fields.*Field classes), so
    # we need to have a specific schema for the `Table` object. In the future,
    # the library should detect from the values produced by the class or by
    # inspecting it.
    schema_rows = OrderedDict(
        [(field_name, convert_field(Field)) for field_name, Field in schema.items()]
    )

    def read_file(fobj):
        year = fobj.name.split(".csv")[0].split("-")[1]
        reader = csv.DictReader(fobj, delimiter=";")
        for row in reader:
            assert row["numAno"] == year
            if not row["sgPartido"]:
                row["sgPartido"] = row["txNomeParlamentar"].replace("LIDERANÇA DO ", "")
            yield [
                Field.deserialize(row[field_name])
                for field_name, Field in schema.items()
            ]

    table = rows.Table(fields=schema_rows)
    table._rows = read_file(load_file(year, download_path))
    header = table.field_names
    return table


def main():
    data_path = pathlib.Path("data")
    output_path = data_path / "output"
    download_path = data_path / "download"
    output = output_path / "cota_parlamentar.csv.xz"
    encoding = "utf-8"
    for path in (data_path, download_path, output_path):
        if not path.exists():
            path.mkdir()

    fobj = io.TextIOWrapper(lzma.open(output, mode='w'), encoding=encoding)
    with tqdm() as progress:
        counter, writer = 0, None
        for year in sorted(years, reverse=True):
            progress.desc = str(year)
            table = create_table(year, download_path)
            header = table.field_names
            for row in table._rows:
                row = dict(zip(header, row))
                if writer is None:
                    writer = csv.DictWriter(fobj, fieldnames=header)
                    writer.writeheader()
                writer.writerow(row)
                counter += 1
                progress.n = counter
                progress.refresh()
            progress.n = counter
            progress.refresh()


if __name__ == '__main__':
    main()
