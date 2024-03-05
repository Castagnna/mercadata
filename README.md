# MercaData

PySpark jobs for Mercafacil ETL.

![Arquitetura](mercafacil.drawio.svg)

## Running
```bash
cd mercadata/etl
```

```bash
python launcher.py gold UpSellCategoria -e prd -m standalone --dry-run
```

```bash
python launcher.py bronze Vendas -e prd -m standalone -d 20220102T010203
```
