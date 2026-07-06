# Olist Big Data Analytics Pipeline

Olist Brazilian E-Commerce veri seti (~100,000 gerçek sipariş, 2016–2018) üzerine kurulu uçtan uca Big Data analytics pipeline'ı.

> Uçtan uca Big Data Pipeline — Ingest, Transform & Visualize.

---

## 📖 Proje Hakkında

Bu proje, Olist e-ticaret veri setini modern veri mühendisliği prensipleriyle işleyerek analitik dashboard'lar oluşturmayı amaçlar. Düz CSV → Parquet dönüşümünün ötesine geçerek **Medallion Mimari**, **Apache Iceberg**, **Kimball Star Schema** ve **Apache Airflow** orkestrasyonu ile production-grade bir data pipeline tasarlanmıştır.

### Temel Yaklaşımlar

- **Medallion Architecture (Bronze / Silver / Gold)** — Veri katmanlı olarak işlenir
- **Apache Iceberg Table Format** — ACID transactions, time travel, schema evolution
- **Kimball Star Schema** — Fact & Dimension tabloları ile analitik modelleme
- **Data Quality Engine** — Null check, duplicate detection, schema validation, DLQ
- **Apache Airflow** — Tüm pipeline'ın orkestre edilmesi ve izlenmesi

---

## 🏗️ Mimari Tasarım

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow (DAG)                        │
│                   Orkestrasyon & Pipeline Yönetimi                  │
└──────────┬──────────┬──────────┬──────────┬────────────────────────┘
           │          │          │          │
           v          v          v          v
┌────────────┐  ┌───────────┐  ┌──────────┐  ┌──────────────────────┐
│  Download  │  │  Bronze   │  │  Silver  │  │        Gold          │
│  Kaggle    │→ │  Layer    │→ │  Layer   │→ │       Layer          │
│  CSV (9)   │  │  Raw Data │  │  Clean   │  │   Star Schema        │
└────────────┘  └───────────┘  └──────────┘  └──────────────────────┘
                     │              │              │
                     v              v              v
              ┌─────────────────────────────────────────┐
              │        HDFS — Apache Iceberg Tables      │
              │        hdfs://namenode:9000               │
              └──────────────────┬────────────────────────┘
                                 │
                                 v
                     ┌───────────────────────┐
                     │   Spark ThriftServer   │
                     │   (Hive Metastore)     │
                     └───────────┬───────────┘
                                 │
                                 v
                     ┌───────────────────────┐
                     │   Apache Superset      │
                     │   Dashboard & Charts   │
                     └───────────────────────┘
```

---

## 🔶 Medallion Mimarisi

### Bronze Layer (Ham Veri)
- Kaggle'dan indirilen 9 CSV dosyası **olduğu gibi** Iceberg tablo formatında HDFS'e yazılır
- Hiçbir dönüşüm yapılmaz — kaynak verinin aslına sadık kopyası
- Amaç: Veri kaybını önlemek, her zaman ham veriye dönebilmek

### Silver Layer (Temizlenmiş Veri)
- Bronze'dan okunan veri üzerinde **data quality kontrolleri** çalıştırılır
- Null temizleme, veri tipi düzeltme, duplicate kaldırma
- Hatalı kayıtlar **Dead Letter Queue (DLQ)** klasörüne yönlendirilir
- Temiz veri Silver Iceberg tablolarına yazılır

### Gold Layer (İş Modeli — Star Schema)
- Silver katmandan okunan veri **Kimball Star Schema** modeline dönüştürülür
- Fact ve Dimension tabloları oluşturulur
- Partition stratejisi (year/month) ile sorgu performansı optimize edilir

---

## ⭐ Star Schema Tasarımı (Kimball)

```text
                          dim_customers
                        (customer_id, zip_code,
                         city, state)
                               │
                               │
dim_products ────────── fact_order_sales ────────── dim_sellers
(product_id,            (order_id,                  (seller_id,
 category,               customer_id,                zip_code,
 weight, size)           product_id,                 city, state)
                         seller_id,                         
       │                 order_purchase_timestamp,          │
       │                 price,                    dim_geolocation
       │                 freight_value,            (zip_code,
dim_dates                avg_review_score)          lat, lng,
(full_date,                                         city, state)
 year, month,           fact_order_payments
 day, quarter,          (order_id,
 day_of_week,            customer_id,
 is_weekend)             payment_type,
                         payment_installments,
                         payment_value)
```

| Tablo | Tip | Grain | Açıklama |
|-------|-----|-------|----------|
| `fact_order_sales` | Fact | 1 sipariş kalemi | Sipariş detayları, fiyatlar ve ort. yorum puanı |
| `fact_order_payments`| Fact | 1 ödeme taksiti| Sipariş ödeme tipleri ve taksit tutarları |
| `dim_customers` | Dimension | 1 müşteri | Müşteri ID ve lokasyon bilgileri |
| `dim_products` | Dimension | 1 ürün | Ürün bilgileri ve İngilizce kategori çevirisi |
| `dim_sellers` | Dimension | 1 satıcı | Satıcı ID ve lokasyon bilgileri |
| `dim_dates` | Dimension | 1 gün | Zaman boyutu (2016–2018) |
| `dim_geolocation` | Dimension | 1 zip code | Coğrafi koordinatlar (deduplicated) |

---

## 🛡️ Data Quality Framework

Pipeline, her katman geçişinde 6 boyutlu veri kalitesi kontrolü uygular:

| Boyut | Kontrol | Aksiyon |
|-------|---------|---------|
| **Completeness** | Primary key'lerde NULL var mı? | DLQ'ya yönlendir |
| **Uniqueness** | Duplicate satırlar var mı? | Deduplicate et |
| **Validity** | Veri tipleri doğru mu? | Cast et veya DLQ |
| **Consistency** | Referans bütünlüğü sağlanıyor mu? | Uyarı logla |
| **Accuracy** | Değerler mantıklı aralıkta mı? | Uyarı logla |
| **Timeliness** | Tarihler anlamlı mı? | Uyarı logla |

Hatalı kayıtlar silinmez — **Dead Letter Queue** klasörüne yazılarak incelenebilir durumda tutulur.

---

## 🛠 Kullanılan Teknolojiler

| Kategori | Teknoloji | Versiyon | Kullanım Amacı |
|----------|-----------|----------|----------------|
| **İşleme** | Apache Spark (PySpark) | 3.3.0 | Dağıtık veri işleme |
| **Tablo Formatı** | Apache Iceberg | 1.4.x | ACID, time travel, schema evolution |
| **Depolama** | HDFS (Hadoop) | 3.2.1 | Dağıtık dosya sistemi |
| **Orkestrasyon** | Apache Airflow | 2.8.x | Pipeline yönetimi & zamanlama |
| **Görselleştirme** | Apache Superset | 4.0.2 | Dashboard & analitik grafikler |
| **Dil** | Python | 3.11 | Pipeline geliştirme |
| **Konteyner** | Docker & Docker Compose | — | Servis yönetimi |

---

## 🚀 Kurulum ve Çalıştırma

### Ön Gereksinimler

- Docker & Docker Compose
- Python 3.10+
- Kaggle hesabı & API token (`~/.kaggle/kaggle.json`)

### 1. Docker Network Oluştur

```bash
bash scripts/setup_network.sh
```

### 2. Servisleri Başlat

```bash
# HDFS
docker compose -f docker/docker-compose-hdfs.yml up -d

# Spark (Iceberg destekli)
docker compose -f docker/docker-compose-spark.yml up -d

# Superset
docker compose -f docker/docker-compose-superset.yml up -d

# Airflow
docker compose -f docker/docker-compose-airflow.yml up -d
```

### 3. Pipeline'ı Çalıştır

**Yöntem A — Airflow üzerinden (önerilen):**
1. [http://localhost:8080](http://localhost:8080) → Airflow UI
2. `olist_medallion_pipeline` DAG'ını tetikle

**Yöntem B — Manuel (Makefile):**
```bash
make all        # Tüm pipeline'ı çalıştır
make bronze     # Sadece Bronze layer
make silver     # Sadece Silver layer
make gold       # Sadece Gold layer
make dashboard  # Tabloları Superset'e kaydet
```

**Yöntem C — Tek tek:**
```bash
# Geliştirme container'ına gir
docker exec -it olist-dev bash

# Sırasıyla çalıştır
python scripts/download_dataset.py
spark-submit processing/bronze_ingestion.py
spark-submit processing/silver_transformation.py
spark-submit processing/gold_modeling.py
spark-submit visualization/register_tables.py
```

### 4. Dashboard'a Eriş

| Servis | URL | Giriş |
|--------|-----|-------|
| HDFS NameNode | http://localhost:9870 | — |
| Spark Master | http://localhost:8080 | — |
| Spark UI | http://localhost:4040 | — |
| Superset | http://localhost:8088 | admin / admin |
| Airflow | http://localhost:8080 | admin / admin |

---

## 📁 Proje Yapısı

```
BigData-Pipeline-Project/
├── main.py                    # Projeyi başlatan Master Controller
├── config/
│   ├── tables.yaml                # Tablo konfigürasyonları
│   └── pipeline_config.yaml       # Pipeline ayarları
├── airflow/
│   └── dags/
│       └── olist_pipeline_dag.py  # Airflow DAG tanımı
├── docker/
│   ├── docker-compose-hdfs.yml    # HDFS (NameNode + DataNode)
│   ├── docker-compose-spark.yml   # Spark + Iceberg + ThriftServer
│   ├── docker-compose-superset.yml# Superset + PostgreSQL + Redis
│   ├── docker-compose-airflow.yml # Apache Airflow
│   ├── docker-compose-minio.yml   # MinIO (alternatif depolama)
│   └── docker-compose-dev.yml     # Geliştirme ortamı
├── processing/
│   ├── bronze_ingestion.py        # CSV → Bronze (Iceberg)
│   ├── silver_transformation.py   # Bronze → Silver (temizleme)
│   ├── gold_modeling.py           # Silver → Gold (Star Schema)
│   ├── data_quality.py            # Veri kalitesi kontrolleri
│   └── utils.py                   # Logging, config, yardımcılar
├── visualization/
│   └── register_tables.py         # Superset tablo kayıt
├── scripts/
│   ├── download_dataset.py        # Kaggle dataset indirme
│   ├── verify_data.py             # CSV doğrulama
│   └── setup_network.sh           # Docker network oluşturma
├── tests/
│   └── test_data_quality.py       # Kalite kontrol testleri
├── reports/
│   ├── REPORT.md                  # Detaylı proje raporu
│   ├── watermark.json             # Pipeline çalışma kayıtları
│   └── screenshots/               # Dashboard ekran görüntüleri
├── Makefile                       # Kısayol komutları
├── requirements.txt               # Python bağımlılıkları
└── .pre-commit-config.yaml        # Kod kalitesi araçları
```

---

## 📊 Olist Veri Seti Hakkında

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — Brezilya'nın en büyük online pazaryerinden ~100,000 gerçek sipariş (2016–2018). 9 CSV tablosu:

| # | Tablo | Açıklama |
|---|-------|----------|
| 1 | `olist_customers_dataset` | Müşteri bilgileri ve lokasyon |
| 2 | `olist_orders_dataset` | Sipariş detayları ve durumları |
| 3 | `olist_order_items_dataset` | Sipariş kalemleri ve fiyatlar |
| 4 | `olist_order_payments_dataset` | Ödeme bilgileri |
| 5 | `olist_order_reviews_dataset` | Müşteri yorumları ve puanları |
| 6 | `olist_products_dataset` | Ürün bilgileri |
| 7 | `olist_sellers_dataset` | Satıcı bilgileri |
| 8 | `olist_geolocation_dataset` | Coğrafi koordinatlar |
| 9 | `product_category_name_translation` | Kategori isim çevirileri |



## 🏗️ Mimari Kararlar

Detaylı mimari kararlar, teknoloji karşılaştırmaları (Iceberg vs Hudi vs Delta Lake), ve performans analizleri için [REPORT.md](reports/REPORT.md) dosyasına bakınız.

---

## 📝 Lisans

Bu proje eğitim amaçlıdır. Olist veri seti [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) üzerinden kamuya açık olarak sunulmaktadır.
