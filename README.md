# Gastos Deputados

Script que baixa os ZIPs relativos a gastos da cota parlamentar [do site da
Câmara dos
Deputados](http://www2.camara.leg.br/transparencia/cota-para-exercicio-da-atividade-parlamentar/dados-abertos-cota-parlamentar),
descompacta, extrai e limpa os registros e converte em um único CSV.


## Licença e Citação

A licença do código é [LGPL3](https://www.gnu.org/licenses/lgpl-3.0.en.html) e
dos dados convertidos [Creative Commons Attribution
ShareAlike](https://creativecommons.org/licenses/by-sa/4.0/). Caso utilize os
dados, **cite a fonte original e quem tratou os dados** e caso compartilhe os
dados, **utilize a mesma licença**. Exemplo de como os dados podem ser citados:
**Fonte: Portal da Transparência da Câmara dos Deputados, dados tratados por
[Álvaro Justen/Brasil.IO](https://brasil.io/)**


## Dados

Depois de coletados e checados os dados ficam disponíveis de 3 formas no
[Brasil.IO](https://brasil.io/):

- [Interface Web](https://brasil.io/dataset/gastos-deputados/) (feita para humanos)
- [API](https://brasil.io/api/dataset/gastos-deputados/) (feita para humanos que desenvolvem programas)
- [Download do dataset completo](https://data.brasil.io/dataset/gastos-deputados/_meta/list.html)

Se esse programa e/ou os dados resultantes foram úteis a você ou à sua empresa,
**considere [fazer uma doação ao projeto Brasil.IO](https://brasil.io/doe)**,
que é mantido voluntariamente.


## Instalando

Requer Python 3.7.

```bash
pip install -r requirements.txt
```

## Executando

Para capturar todos os anos:

```bash
./run.sh
```

Você também pode executar diretamente o spider do scrapy:

```bash
scrapy runspider camara_federal.py --loglevel=INFO -o cota-parlamentar-camara-federal.csv
```

Caso queira apenas alguns anos, passe o parâmetro `-a years=XXX`, com os
valores separados por vírgula, exemplo:

```bash
scrapy runspider camara_federal.py -a years=2015,2016,2017,2018 --loglevel=INFO -o cota-parlamentar-2015-2018.csv
```

## Trabalhando com os dados

Você pode utilizar a interface de linha de comando da
[rows](https://github.com/turicas/rows) para converter os dados gerados em CSV
para um banco de dados SQLite - dessa forma a análise dos dados pode ser feita
em cima do banco de dados gerado usando SQL.

```bash
rows csv2sqlite --schemas=schema/cota-parlamentar.csv data/output/cota-parlamentar.csv.gz data/gastos.sqlite
```
