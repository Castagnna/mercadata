[![Unit tests](https://github.com/Castagnna/mercadata/actions/workflows/python-app.yml/badge.svg?branch=main)](https://github.com/Castagnna/mercadata/actions/workflows/python-app.yml)
# MercaData 

PySpark jobs for Mercafacil ETL.

![architecture](mercafacil.drawio.svg)

## Prepare your environment

Validate the Java installation on your system
```bash
java -version
```

Check if the JAVA_HOME environment variable is set on your system
```bash
echo $JAVA_HOME
```

If not, set JAVA_HOME
```bash
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64/"
```

Install venv:
```bash
sudo apt update

sudo apt upgrade

sudo apt install python3.12-venv
```

Activate the virtual environment and install requirements:
```bash
python3.12 -m venv ./venv

source venv/bin/activate

pip install -r requirements.txt
```

When you're done working with the virtual environment, you can deactivate it by running:
```bash
deactivate
```

## Running
```bash
cd mercadata/etl
```

```bash
python3 launcher.py gold UpSellCategoria -e prd -m standalone --dry-run
```

```bash
python3 launcher.py bronze Vendas -e prd -m standalone -d 20220102T010203 --noop
```

```bash
python3 launcher.py bronze Vendas -e prd -m standalone -d 20220102T010203
```
