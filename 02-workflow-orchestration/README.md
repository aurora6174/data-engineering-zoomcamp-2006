# Module 02 – Workflow Orchestration

**Data Engineering Zoomcamp 2026**

This module covers workflow orchestration using **Kestra**. The objective is to design, run, and monitor reproducible data pipelines for ingesting and analyzing NYC Taxi data.

---

## Tools & Technologies

- Kestra (workflow orchestration)
- Docker & Docker Compose
- PostgreSQL
- Python
- SQL

---

## Project Overview

- Kestra is deployed locally using Docker Compose
- Workflows are defined using YAML
- NYC Taxi datasets are ingested into PostgreSQL and GCP
- Environment variables are managed using a `.env` file
- Pipelines are parameterized and re-runnable

---

## How to run

```bash
docker compose up -d
```

## Remove containers

```bash
docker compose down
```

## Answers to homework questions

- Question 1 
    128.3MB

- Question 2
    green_tripdata_2020-04.csv

- Question 3
    24,648,499

- Question 4
    1,734,051

- Question 5
    1,428,092

- Question 6
    Add a timezone property set to America/New_York in the Schedule trigger configuration
