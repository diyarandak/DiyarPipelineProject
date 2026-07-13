# 🚀 Olist Gelişmiş Büyük Veri Boru Hattı (Modern Data Stack)

Bu proje, Brezilya E-Ticaret Platformu Olist'e ait gerçek veri seti kullanılarak geliştirilmiş, uçtan uca, kurumsal seviyede bir **Büyük Veri (Big Data) ve Veri Mühendisliği** projesidir. Projenin amacı; ham ve dağınık haldeki bir e-ticaret verisinin, en modern veri mühendisliği standartlarında (Modern Data Stack) işlenerek şirket için değerli bir "iş zekası" kararına dönüşme serüvenini sıfırdan inşa etmektir.

---

## 🧠 AI Sunum Üreticisi İçin Kılavuz (Prompt Context)
Bu döküman, yapay zeka araçlarının (ChatGPT, Claude vb.) bu projeden profesyonel bir PowerPoint (.pptx) sunumu hazırlayabilmesi için özel olarak son derece detaylı yazılmıştır. Sunum hazırlarken lütfen aşağıdaki 9 adımı slaytlara bölerek, mimari karşılaştırmaları (Avantaj/Dezavantaj) vurgulayarak ve mühendislik terimlerini (ELT, OLAP, SCD Type 2, DAG) ön plana çıkararak tasarlayın.

---

## 🏛️ Mimari Tasarım (Medallion Architecture)

Proje, veriyi ham halinden en değerli analitik haline kadar katman katman işleyen Medallion yaklaşımını kullanmaktadır:

```mermaid
graph LR
    subgraph EL ["Extract & Load (EL)"]
        CSV[("Kaggle Ham CSV\n(OLTP)")] -->|PySpark Ingestion\nWatermark| Bronze[("Bronze Katman\nIceberg / Parquet")]
    end
    
    subgraph T ["Transform (T) - dbt"]
        Bronze -->|dbt Staging\nSanal Temizlik| Silver[("Silver Katman\n(View)")]
        Silver -->|dbt Models\nStar Schema| Gold[("Gold Katman\n(Table)")]
    end
    
    subgraph Serve ["Serve & Analyze"]
        Gold -->|Apache Doris\nMPP Motoru| BI["Apache Superset\n(OLAP Dashboards)"]
    end
    
    Airflow(("Apache Airflow\n(Cosmos ile Paralel)")) -.->|PySpark Tetikler| Bronze
    Airflow -.->|dbt Orkestre Eder| Silver
```

---

## 🚀 Projenin Teknik Adımları ve Mühendislik Kararları

### 1. Verinin Çıkarılması (Bronze Katmanı ve Iceberg)
Projeye başlarken ham CSV dosyalarını klasik HDFS üzerinde tutmak yerine, üzerine **Apache Iceberg** formatını giydirerek sistemi ACID yetenekleri olan bir **Veri Gölevi (Data Lakehouse)** yapısına çevirdik.
*   **Incremental Loading (Yığın Su İşareti - Watermark):** Sistem gece 03:00'te çalıştığında tüm veriyi baştan okumaz. `watermark.json` dosyasını kontrol eder; sadece yeni gelen veya önceki gün hata almış (FAILED) dosyaları bularak göle **Append (Ekleme)** mantığıyla yazar. Bu sayede veri çiftlenmesi (duplication) önlenir.

### 2. ETL'den ELT'ye Geçiş ve Apache Doris
Faz 1 projelerinde dönüşüm işlemleri Spark üzerinde (ETL) yapılıyordu. Bu projede **ELT (Extract, Load, Transform)** yaklaşımına geçtik.
*   **Neden ELT?** ETL'de veri ağ üzerinden RAM'e çekilir (darboğaz yaratır). ELT'de ise hesaplama veritabanına itilir (Pushdown), ağ maliyeti sıfırlanır.
*   **HDFS vs Doris:** HDFS ucuz disklerde devasa ham verileri saklamak için mükemmeldir. Apache Doris ise inanılmaz hızlı, sütun bazlı ve MPP (Devasa Paralel İşleme) yeteneklerine sahip gerçek zamanlı bir analitik veritabanıdır. dbt, Doris'in "External Catalog" yeteneğini kullanarak HDFS'i okur; yani dbt sadece SQL ile Doris'e emir verir, asıl ağır yükü Doris kaldırır.

### 3. SQL vs Spark ve dbt'nin Gücü
Sadece SQL kullanmak statiktir, test edilemez. Sadece Spark kullanmak donanım canavarıdır. **dbt (Data Build Tool)** bu iki dünyayı birleştirir.
*   İçine gömülü Jinja (Python) şablonları ile saf SQL'e programlanabilir bir beyin ekler. Yazılan modelleri derleyerek (compile) Doris'in anlayacağı saf SQL'e dönüştürür.

### 4. Temizlik ve Standartlaştırma (Silver / Staging Katmanı)
Silver katmanında fiziki veri kopyalaması yapılmaz; veriler disk israfını önlemek için **Sanal Tablo (View)** olarak oluşturulur.
*   Boş verilerin silinmesi veya mükerrer (`DISTINCT`) kayıtların ayıklanması doğrudan `.sql` modellerinin içinde çözülür (DLQ yerine doğrudan kod içi filtre).
*   **Çift Dikiş (Defense in Depth):** SQL içindeki filtrelemeye ek olarak `schema.yml` dosyasına `not_null` ve `unique` testleri yazılarak bozuk verinin Gold katmana sızması kesin olarak engellenir.

### 5. İş Modelleri (Gold Katmanı) ve OLTP'den OLAP'a Geçiş
Kaynak veritabanı (Olist) siparişleri hızlı kaydetmek için tasarlanmış dağınık bir **OLTP (Online Transaction Processing)** sistemidir. İş zekası analistleri ise milyonlarca satırı saniyeler içinde okumak (**OLAP**) isterler.
*   **Star Schema (Yıldız Şema):** Karmaşık OLTP verisini alıp merkezde `fact_orders` (ciro vb.) ve etrafında onu açıklayan `dim_customers`, `dim_products` gibi tabloların olduğu denormalize bir yapıya çevirdik. 
*   **Overwrite Stratejisi:** Gold katmanında 100 bin satırlık veriyi incremental işlemek yerine kodu basit tutmak adına (Keep it simple) her gün baştan silip yaratan (Overwrite - Table) stratejisini seçtik. Çünkü Doris bu kadar küçük bir veriyi saniyeler içinde baştan yaratabilir.

### 6. dbt'nin Gelişmiş Kurumsal Özellikleri
*   **Seeds:** Eyalet kodları gibi değişmeyen statik referans verileri CSV olarak yüklenir ve otomatik tablo yapılır.
*   **Macros:** DRY (Don't Repeat Yourself) prensibiyle, uzun `CASE WHEN` blokları fonksiyonlaştırılarak tekrar tekrar kullanılır.
*   **Analyses:** Kalıcı tablo gerektirmeyen tek seferlik ad-hoc analizler için kullanılır.
*   **state:modified Komutu:** CI/CD süreçlerinde devrim yaratır. Kod güncellendiğinde 100 tablonun tamamını değil, sadece kodu değişen tabloları (ve bağımlılarını) `dbt build -s state:modified+` ile tetikleyerek devasa compute tasarrufu sağlar.

### 7. Zaman Makinesi: Müşteri Tarihçesi Takibi (SCD Type 2)
Bir müşterinin ili değiştiğinde geçmişi silip üstüne yazmak (SCD 1 / Incremental) geçmiş analizlerini bozar. Biz dbt **Snapshots** özelliğini kullanarak **SCD Type 2** metodolojisini kurduk.
*   `strategy='check'` özelliği sayesinde adres kolonu değiştiği an eski kaydın `valid_to` tarihini kapatır, altına güncel tarihi `valid_from` olarak basıp yeni satır açar. Sistem zaman makinesine dönüşür.

### 8. Orkestrasyon ve Paralel Çalışma (Airflow ve Cosmos)
Projenin kalbi Apache Airflow'dur. Her gece 03:00'da çalışır, hataları 3 kez dener, zaman aşımında (SLA) uyarı atar.
*   **Astronomer Cosmos:** Normalde dbt'yi Airflow'da çalıştırırsanız tek bir dev kara kutu görünür. Cosmos ise dbt klasörünü okur ve içindeki SQL modellerini otomatik olarak Airflow görevlerine (tasks) dönüştürür.
*   **Paralel Çalışma:** Airflow zaten doğası gereği bağımsız görevleri paralel çalıştırır. Cosmos'un bu dönüşümü yapması sayesinde Airflow, dbt'nin bağımlılık haritasına (DAG) bakar ve alakasız tabloları aynı anda (paralel) çalıştırarak sistemi muazzam hızlandırır.

### 9. Dashboard Devrimi (Spark Thrift vs Apache Doris)
Eski yapılarda BI araçları SQL konuştuğu için araya Spark Thrift Server konulurdu. Bu da SQL'i Spark işlerine çevirirken devasa JVM başlatma sürelerine (Latency) ve eşzamanlı analist girdiğinde bellek hatalarına (Concurrency çöküşü) neden olurdu.
*   **Neden Doris?** Biz Apache Superset'i doğrudan Doris'e bağladık. Doris zaten sıfırdan C++ ile SQL konuşmak için yazılmıştır. **Zone Map** (Blok Haritası) indeksleri sayesinde tüm veriyi taramaz, grafiğin noktasını saniyeden kısa sürede getirir. Çevirici köprü (Thrift) ortadan kalktığı için milisaniye hızında Dashboard'lar elde edildi.

---

## 🌟 Veri Modelleme ER Diyagramı (Star Schema)

```mermaid
erDiagram
    fact_orders {
        string order_id PK
        string customer_key FK
        date order_date_key FK
        decimal total_order_value
        string delivery_status
    }
    
    fact_order_items {
        string order_item_surrogate_key PK
        string order_id FK
        string product_key FK
        string seller_key FK
        decimal price
        decimal freight_value
    }
    
    fact_seller_performance {
        string seller_key PK
        int total_orders_fulfilled
        int total_products_sold
        decimal total_revenue
        decimal avg_review_score
    }

    dim_customers {
        string customer_key PK
        string customer_city
        string customer_region
    }
    
    dim_sellers {
        string seller_key PK
        string seller_city
        string seller_region
    }

    dim_products {
        string product_key PK
        string product_category
        string product_size_category
    }

    dim_date {
        date date_key PK
        int year
        int quarter
        string month_name_pt
    }
    
    dim_geography {
        string zip_code_prefix PK
        string city
        string state
    }
    
    dim_payment_type {
        string payment_type_key PK
        string payment_type_name
    }

    %% Relationships
    fact_orders }o--|| dim_customers : "Satın alır"
    fact_orders }o--|| dim_date : "Sipariş verilir"
    
    fact_order_items }o--|| fact_orders : "Aittir"
    fact_order_items }o--|| dim_products : "İçerir"
    fact_order_items }o--|| dim_sellers : "Satılır"
    
    fact_seller_performance ||--|| dim_sellers : "Performans"
    dim_customers }o--|| dim_geography : "Bulunur"
    dim_sellers }o--|| dim_geography : "Bulunur"
```

---

## 📸 Superset Analitik Dashboard

Projemizin çıktısı olan ve otomatik Python API scriptleri ile saniyeler içinde Superset üzerinde ayağa kalkan interaktif Olist Dashboard'undan görüntüler:

![Dashboard 1](Dashboards/Ekran%20Resmi%202026-07-13%2000.23.30.png)
![Dashboard 2](Dashboards/Ekran%20Resmi%202026-07-13%2000.23.53.png)
![Dashboard 3](Dashboards/Ekran%20Resmi%202026-07-13%2000.24.12.png)
![Dashboard 4](Dashboards/Ekran%20Resmi%202026-07-13%2000.24.26.png)
![Dashboard 5](Dashboards/Ekran%20Resmi%202026-07-13%2000.24.39.png)

---

## 📂 Dosya ve Klasör Hiyerarşisi

Aşağıda projenin temiz ve modüler dosya ağacını görebilirsiniz:

```text
AdvancedBigDataPipeline/
├── Makefile                        # Tüm kurulum ve çalıştırma komutlarının bulunduğu merkezi araç
├── README.md                       # Proje dokümantasyonu (Bu dosya)
├── Dashboards/                     # Superset üzerinden alınan rapor ekran görüntüleri
├── airflow/                        # Airflow DAG'leri ve Astronomer konfigürasyonları
│   └── dags/
├── config/                         # Veritabanı, Spark ve diğer servis bağlantı ayarları
├── data/                           # Kaggle'dan indirilen Olist CSV dosyaları
├── docker/                         # Docker Compose YAML dosyaları (Doris, Superset, Airflow vb.)
├── olist_dbt/                      # dbt veri dönüşüm projesi
│   ├── dbt_project.yml
│   ├── macros/                     # SQL makroları (örn: get_brazil_region)
│   ├── models/
│   │   ├── staging/                # Silver katman (Veri temizleme ve tekilleştirme)
│   │   └── marts/                  # Gold katman (Fact ve Dimension tabloları)
│   ├── seeds/                      # Referans verileri (brazil_states.csv)
│   └── tests/                      # Veri doğrulama testleri
├── processing/                     # PySpark veri içe aktarma scriptleri (Bronze Ingestion)
├── scripts/                        # Otomasyon scriptleri (Veri indirme, ağ kurma vb.)
└── visualization/                  # Superset API otomasyon klasörü
    ├── cleanup.py                  # Eski dashboard'ları temizler
    ├── create_dashboard.py         # Grafikleri ve Dashboard'u sıfırdan kurar
    └── register_tables.py          # Veritabanı tablolarını Superset datasetlerine çevirir
```

---

## 🛠️ Kullanılan Teknolojiler
*   **Veri Çıkarma (Ingestion):** PySpark
*   **Veritabanı / Veri Ambarı:** Apache Doris
*   **Veri Dönüştürme:** dbt (Data Build Tool)
*   **Orkestrasyon:** Apache Airflow & Astronomer Cosmos
*   **Veri Görselleştirme:** Apache Superset

---

## 🚀 Projeyi Çalıştırma (Kurulum Kılavuzu)

### 1. Servisleri Başlatma
Tüm altyapı (Hadoop, Doris, Airflow, Superset) Docker üzerinde çalışır:
```bash
make setup
```

### 2. Veri Boru Hattını (Pipeline) Tetikleme
PySpark veriyi göle indirir ve dbt tüm dönüşüm/test süreçlerini çalıştırır:
```bash
make download
make pipeline
```

### 3. Superset (Dashboardlar)
Raporları ve panelleri oluşturmak için:
```bash
make dashboard
```
Erişim: `http://localhost:8088` (admin/admin).
