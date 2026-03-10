![Python](https://img.shields.io/badge/Python-3.12%20%E2%80%93%203.14(t)-darkgreen?logo=python&logoColor=blue)
[![Tests](https://github.com/h5rdly/webtoken/actions/workflows/tests.yml/badge.svg)](https://github.com/h5rdly/webtoken/actions/workflows/tests.yml)

# Cudos

**PG17 server mimic / emulator for tests and local k8s / k3s setups**

##  Prerequisites

Cudos uses [sqlglot](https://github.com/tobymao/sqlglot). 

Install the fast variant via - 

`pip install sqlglotc sqlglot`

Or - 
`pip install sqlglot[c]`


##  Usage

Start the server -
```python
python3 cudos.py
```

Run supplid tests via - 
```python
python3 tests.py
```
