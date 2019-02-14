# Gastos Deputados

Script que baixa os ZIPs relativos a gastos da cota parlamentar [do site da
Câmara dos
Deputados](http://www2.camara.leg.br/transparencia/cota-para-exercicio-da-atividade-parlamentar/dados-abertos-cota-parlamentar),
descompacta, extrai e limpa os registros e converte em um único CSV.

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
scrapy runspider cota_parlamentar --loglevel=INFO -o cota-parlamentar.csv
```

Caso queira apenas alguns anos, passe o parâmetro `-a years=XXX`, com os
valores separados por vírgula, exemplo:

```bash
scrapy runspider cota_parlamentar -a years=2015,2016,2017,2018 --loglevel=INFO -o cota-parlamentar-2015-2018.csv
```
