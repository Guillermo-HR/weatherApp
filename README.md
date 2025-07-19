# Weather Data Pipeline (V1)

![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-blue)
![Grafana](https://img.shields.io/badge/Grafana-9.0%2B-blue)
![Fedora](https://img.shields.io/badge/Fedora-42-blue)

A lightweight data engineering pipeline that collects, stores, and visualizes weather data from OpenWeatherMap API.

## 📌 Project Scope (V1)
- **Single API Source**: OpenWeatherMap Current Weather Data
- **Technology Stack**:
  - Python 3.9+ (ETL scripts)
  - PostgreSQL 14+ (Data storage)
  - Grafana 9+ (Visualization)
  - Cron (Scheduling)

## 📦 Installation
This instructions are for setting up the Weather Data Pipeline on a Fedora system.
1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/weather-data-pipeline.git
   cd weather-data-pipeline
   ```
2. **Install dependencies**:
    * **Python**:
        ```bash
        sudo dnf install python3 python3-pip
        ```
    * **PostgreSQL**:
        ```bash
        sudo dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/F-42-x86_64/pgdg-fedora-repo-latest.noarch.rpm

        sudo dnf install -y postgresql17-server
        
        sudo /usr/pgsql-17/bin/postgresql-17-setup initdb

        sudo systemctl enable postgresql-17.service

        sudo systemctl start postgresql-17.service

        sudo systemctl status postgresql-17.service
        ```
    * **Grafana**:
        ```bash
        sudo dnf install wget # If wget is not installed

        wget -q -O gpg.key https://rpm.grafana.com/gpg.key

        sudo rpm --import gpg.key

        echo "[grafana]
        name=grafana
        baseurl=https://rpm.grafana.com
        repo_gpgcheck=1
        enabled=1
        gpgcheck=1
        gpgkey=https://rpm.grafana.com/gpg.key
        sslverify=1
        sslcacert=/etc/pki/tls/certs/ca-bundle.crt" | sudo tee /etc/yum.repos.d/grafana.repo

        sudo dnf install grafana

        sudo systemctl daemon-reload

        sudo systemctl start grafana-server

        sudo systemctl enable grafana-server

        sudo systemctl status grafana-server
        ```