# 🌍 Air Quality Intelligence Platform

An end-to-end, **Dockerized data engineering project** that ingests public
**air-quality** (OpenAQ) and **weather** (Open-Meteo) data for several cities,
stores raw data in **AWS S3**, processes it with **PySpark** (local mode,
CPU-only), loads curated daily metrics into **PostgreSQL**, serves them via a
**FastAPI** backend, and visualizes them in a **Streamlit** dashboard — all
orchestrated by **Apache Airflow** and shipped with **Docker Compose**.

> **Version 1 scope:** Spark **local mode** inside Docker (no YARN), **no AWS
> ECR**, manual EC2 provisioning, CI on GitHub Actions, and **auto-deploy to
> EC2 over SSH**. The project runs **end-to-end even without API keys or AWS
> credentials** thanks to built-in fallback sample data.

---

## 1. Project Overview

| Layer            | Technology            | Responsibility                                            |
|------------------|-----------------------|-----------------------------------------------------------|
| Orchestration    | Apache Airflow        | Runs the `air_quality_pipeline` DAG (extract→load→check)  |
| Ingestion        | Python + `requests`   | Pulls OpenAQ + Open-Meteo data (fallback sample data)     |
| Data Lake        | AWS S3                | `raw/` and `processed/` layers                            |
| Processing       | PySpark (local mode)  | Clean, join, aggregate to daily city metrics              |
| Warehouse        | PostgreSQL            | Final dashboard tables                                    |
| API              | FastAPI               | `/health`, `/trigger-pipeline`, `/latest-metrics`, `/city-trends` |
| UI               | Streamlit             | Charts + a "Trigger Latest Pipeline" button               |
| Packaging        | Docker Compose        | One command to run the whole stack                        |
| CI               | GitHub Actions        | Lint + tests + compose validation                         |
| CD               | GitHub Actions (SSH)  | Auto-deploy to EC2 on push to `main`                      |

---

## 2. Architecture (text diagram)

```
                         ┌──────────────────────────────────────────────┐
 Developer ──push──►     │                 GitHub                         │
                         │  CI (ci.yml): lint + pytest + compose config   │
                         │  CD (deploy.yml): SSH ──► EC2, docker compose  │
                         └───────────────────────┬────────────────────────┘
                                                 │ (auto-deploy on push to main)
                                                 ▼
 ┌───────────────────────────────── EC2 (Docker Compose) ──────────────────────────────────┐
 │                                                                                           │
 │   Streamlit UI  ──HTTP──►  FastAPI  ──REST──►  Airflow (webserver + scheduler)            │
 │        ▲                      │                      │                                    │
 │        │                      │ SQL                  │ runs DAG: air_quality_pipeline     │
 │        │                      ▼                      ▼                                    │
 │        │                  PostgreSQL  ◄────────  load_curated_data_to_postgres            │
 │        │              (air_quality DB)                ▲                                   │
 │        └── reads metrics ──────────────┘             │                                   │
 │                                                       │                                   │
 │   extract_air_quality_data ─┐                         │                                   │
 │   extract_weather_data     ─┴► upload_raw_data_to_s3 ─► run_pyspark_transformation        │
 │                                        │ (local[*])         │                             │
 │                                        ▼                    ▼                             │
 │                                   AWS S3 (raw/)        AWS S3 (processed/) + curated CSV   │
 │                                                              │                            │
 │                                                              ▼                            │
 │                                                    data_quality_check                     │
 └───────────────────────────────────────────────────────────────────────────────────────┘
```

**Pipeline DAG task order**

```
extract_air_quality_data ┐
                         ├─► upload_raw_data_to_s3 ─► run_pyspark_transformation
extract_weather_data    ┘                                    │
                                                             ▼
                              load_curated_data_to_postgres ─► data_quality_check
```

---

## 3. Folder Structure

```
.
├── README.md
├── docker-compose.yml
├── .env.example                # template — copy to .env (never commit .env)
├── requirements.txt            # full dev/test deps (used by CI)
├── pytest.ini / .flake8 / conftest.py
├── dags/
│   └── air_quality_pipeline.py # the Airflow DAG
├── jobs/
│   └── transform_air_quality.py# PySpark transformation (local mode)
├── app/                        # FastAPI backend
│   ├── main.py
│   ├── database.py
│   ├── schemas.py
│   └── services/
│       ├── airflow_client.py   # triggers the DAG via Airflow REST API
│       └── metrics_service.py
├── ui/
│   └── streamlit_app.py        # dashboard (talks only to FastAPI)
├── src/                        # shared library used by Airflow tasks
│   ├── extract/{air_quality_api,weather_api}.py
│   ├── load/{s3_loader,postgres_loader}.py
│   ├── transform/pollution_logic.py
│   └── utils/config.py         # env-driven config (no hardcoded secrets)
├── tests/                      # pytest suite
├── scripts/                    # SQL init (creates airflow_meta + app tables)
├── docker/                     # Dockerfiles + per-service requirements
└── .github/workflows/
    ├── ci.yml                  # lint + test + compose validate
    └── deploy.yml              # auto-deploy to EC2 over SSH
```

---

## 4. Setup Instructions

### Prerequisites
- Docker + Docker Compose v2 (`docker compose version`)
- (Optional) Python 3.11 if you want to run tests/lint locally

### Configure environment
```bash
git clone https://github.com/uddipan77/data_engg_one.git
cd data_engg_one
cp .env.example .env
# Edit .env to add AWS keys / OpenAQ key IF you have them.
# Leave them blank to run fully locally with fallback sample data.
```

> **Security:** `.env`, `*.pem`, `*.key`, and AWS credentials are git-ignored.
> Only `.env.example` is committed.

---

## 5. How to Create an AWS S3 Bucket

You said the bucket does **not** exist yet. Create it once:

**Option A — AWS Console**
1. Sign in → **S3** → **Create bucket**.
2. Bucket name: e.g. `air-quality-intelligence-<your-initials>` (globally unique).
3. Region: pick one (e.g. `eu-central-1`) and put the **same value** in `.env` `AWS_REGION`.
4. Keep "Block all public access" **ON** (this is a private data lake).
5. Create bucket. Put the name in `.env` `S3_BUCKET_NAME`.

**Option B — AWS CLI**
```bash
aws s3api create-bucket \
  --bucket air-quality-intelligence-<your-initials> \
  --region eu-central-1 \
  --create-bucket-configuration LocationConstraint=eu-central-1
```

**IAM:** create an IAM user with programmatic access and an inline policy
allowing `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` on that bucket. Put the
access key + secret into `.env` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
The platform will create `raw/...` and `processed/...` prefixes automatically.

> If you skip AWS entirely, the pipeline still runs — it just logs
> "S3 not configured" and keeps data on the local mounted `./data` volume.

---

## 6. Run Locally with Docker Compose

```bash
docker compose build           # build airflow/fastapi/streamlit images
docker compose up -d           # start everything
docker compose ps              # check health
```

First boot does a one-shot `airflow-init` (DB migration + admin user). Give it
~1–2 minutes. Then open:

| Service          | URL                          | Login                |
|------------------|------------------------------|----------------------|
| Streamlit UI     | http://localhost:8501        | —                    |
| FastAPI docs     | http://localhost:8000/docs   | —                    |
| Airflow UI       | http://localhost:8080        | `airflow` / `airflow`|
| Spark master UI  | http://localhost:8081        | —                    |

Stop / reset:
```bash
docker compose down            # stop
docker compose down -v         # stop + wipe volumes (fresh DB next time)
```

---

## 7. How to Trigger the Airflow DAG

**Three ways:**

1. **From the UI:** open Streamlit → click **🚀 Trigger Latest Pipeline**.
2. **From FastAPI:** `POST http://localhost:8000/trigger-pipeline`
   (or use the "Try it out" button in `/docs`).
3. **From Airflow UI:** http://localhost:8080 → DAG `air_quality_pipeline` →
   ▶️ Trigger.

The DAG: extracts data → uploads raw to S3 → runs the PySpark job
(`spark-submit --master local[*]`) → loads curated rows into PostgreSQL →
runs data-quality checks.

**Run the Spark job manually (optional):**
```bash
docker compose exec airflow-scheduler \
  spark-submit --master local[*] \
  /opt/airflow/project/jobs/transform_air_quality.py \
  --raw-dir /opt/airflow/data/raw \
  --out-dir /opt/airflow/data/curated \
  --run-date 2026-06-11
```

---

## 8. Open FastAPI Docs

http://localhost:8000/docs — interactive Swagger UI. Endpoints:

| Method | Path                       | Description                          |
|--------|----------------------------|--------------------------------------|
| GET    | `/health`                  | Liveness + DB status + Airflow URL   |
| POST   | `/trigger-pipeline`        | Trigger the Airflow DAG              |
| GET    | `/latest-metrics`          | Most recent day's curated metrics    |
| GET    | `/city-trends?city=Berlin` | Historical trend for one city        |
| GET    | `/pipeline-status`         | Last DAG-run state                   |

---

## 9. Open the Streamlit UI

http://localhost:8501 — shows the project explanation, a trigger button,
the latest metrics table, and charts for city-wise **PM2.5**, **PM10**,
**temperature**, and **pollution-category** distribution, plus per-city trends.

---

## 10. Running Tests & Linting Locally

```bash
python -m venv .venv && source .venv/bin/activate    # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
flake8 .
pytest -q
```

Tests cover: API `/health`, pollution-category logic, the PySpark transform
(with tiny in-memory data), and the DB connection utility. The Spark test needs
Java; it auto-skips if no JVM is available.

---

## 11. Deploying to AWS EC2 (Auto-deploy via GitHub Actions)

### 11.1 Create the EC2 instance (does not exist yet)
1. EC2 → **Launch instance**. AMI: **Ubuntu 22.04** (or Amazon Linux 2023).
2. Type: **t3.large** or bigger (Spark + Airflow + Postgres need RAM; ≥ 8 GB).
3. Create/download a **key pair** (`.pem`) — keep it safe, never commit it.
4. Security group inbound rules: `22` (SSH, your IP), `8501` (Streamlit),
   `8000` (FastAPI), `8080` (Airflow) — restrict source IPs as you prefer.
5. Launch, note the **Public IP / DNS**.

### 11.2 Prepare the instance
```bash
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>
sudo apt-get update && sudo apt-get install -y docker.io git
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/bin/docker-compose && sudo chmod +x /usr/local/bin/docker-compose
sudo usermod -aG docker $USER && newgrp docker
git clone https://github.com/uddipan77/data_engg_one.git
cd data_engg_one
cp .env.example .env   # then edit .env with your real AWS keys ON THE SERVER
```

### 11.3 Configure GitHub Actions Secrets (this is the part you asked about)
In the repo: **Settings → Secrets and variables → Actions → New repository secret**:

| Secret name       | Value                                                      |
|-------------------|------------------------------------------------------------|
| `EC2_HOST`        | EC2 public IP or DNS                                       |
| `EC2_USERNAME`    | `ubuntu` (Ubuntu AMI) or `ec2-user` (Amazon Linux)        |
| `EC2_SSH_KEY`     | **contents** of your `.pem` private key                   |
| `EC2_PROJECT_DIR` | e.g. `/home/ubuntu/data_engg_one`                         |

> ✅ Your AWS keys stay in the `.env` file **on EC2** — they are **not** stored
> in GitHub. Only the SSH key + host go into GitHub Actions Secrets.

### 11.4 Deploy
- **Automatic:** push/merge to `main` → `deploy.yml` SSHes into EC2,
  runs `git pull` + `docker compose up -d --build`.
- **Manual fallback:** SSH in yourself and run `docker compose up -d --build`.
- If secrets aren't set yet, the deploy job safely **skips** (CI still passes).

Then browse to `http://<EC2_PUBLIC_IP>:8501`.

---

## 12. Data Sources & Fallback Behaviour

- **Air quality:** OpenAQ v3 (`OPENAQ_API_KEY`, free). If absent/unreachable →
  deterministic **fallback sample data** is generated per city/date.
- **Weather:** Open-Meteo (no key). If unreachable → fallback sample data.
- Cities: **Berlin, Munich, Paris, London, Delhi, Kolkata** (override with the
  `CITIES` env var).

This guarantees the full pipeline runs for demos, CI, and first-time setup
**without any credentials**.

---

## 13. Database Tables

| Table                  | Purpose                                              |
|------------------------|------------------------------------------------------|
| `pipeline_runs`        | One row per DAG run (status, timestamps, row counts) |
| `air_quality_metrics`  | Curated daily city metrics (the dashboard table)     |
| `data_quality_results` | Per-run data-quality check outcomes                  |

`air_quality_metrics` columns: `city, date, pm25_avg, pm10_avg, no2_avg,
temperature_avg, humidity_avg, pollution_category`.

---

## 14. Future Improvements

- Move to **Spark on a real cluster** (standalone/EMR/YARN) for larger data.
- Use **AWS ECR** + image tags instead of building on the EC2 box.
- Add **incremental/partitioned** S3 layout (Hive-style `date=` partitions).
- Add **Great Expectations** for richer data-quality validation.
- Add **authentication** to FastAPI/Streamlit and HTTPS via a reverse proxy.
- Schedule the DAG (cron) and add **alerting** (Slack/email) on failures.
- Add **Terraform** to provision S3/EC2/IAM reproducibly.
- Backfill historical data and build **time-series forecasting** of PM2.5.

---

## 15. Resume Bullet Points

- Built an **end-to-end, Dockerized data engineering platform** ingesting
  public air-quality and weather APIs for 6 cities, orchestrated with **Apache
  Airflow** and deployed to **AWS EC2** via a **GitHub Actions** CI/CD pipeline.
- Designed an **S3 data lake** (raw + processed layers) and a **PySpark**
  (local-mode, CPU-only) transformation that cleans, joins, and aggregates data
  into daily city-level analytics with a derived pollution-category metric.
- Modeled and loaded curated metrics into **PostgreSQL**, exposed them through a
  **FastAPI** service, and visualized them in a **Streamlit** dashboard with
  one-click pipeline triggering.
- Implemented **data-quality checks**, **pytest** unit/integration tests, and
  **flake8** linting in CI; added graceful **fallback sample data** so the full
  system runs without credentials.
- Automated deployment to EC2 with a **GitHub Actions SSH workflow** (`docker
  compose up`), keeping all secrets out of source control.

---

## 16. Troubleshooting

- **Airflow UI 502 / not ready:** wait for `airflow-init` to finish; check
  `docker compose logs airflow-init`.
- **DAG not triggering from API:** ensure Airflow REST API basic-auth is on
  (set in compose) and creds match `.env`.
- **Spark task fails with "JAVA_HOME":** rebuild the Airflow image
  (`docker compose build airflow-scheduler`) — it installs the JDK.
- **No metrics in UI:** trigger the pipeline and wait for the DAG to finish,
  then refresh.
- **Low memory on EC2:** use an instance with ≥ 8 GB RAM.
