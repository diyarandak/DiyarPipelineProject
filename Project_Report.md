# Olist E-Ticaret Gelişmiş Büyük Veri ve Analitik Boru Hattı (Data Pipeline) Proje Raporu

Bu rapor, Olist E-Ticaret veritabanı üzerinde kurulan, endüstri standartlarında ve üretim (production) kalitesinde tasarlanmış **Medallion Architecture (Bronz, Gümüş, Altın)** veri boru hattının teknik yapısını ve ELT (Extract, Load, Transform) süreçlerini detaylandırmaktadır.

---

## 1. Mimari Genel Bakış (Architecture Overview)

Projemiz, modern veri mühendisliği pratiklerini barındıran tam otomatik bir **Veri Gölevi (Data Lakehouse)** mimarisidir. Süreç, ham verinin diskten alınıp iş zekası panolarına (dashboard) aktarılmasına kadar olan tüm adımları kapsar.

Aşağıdaki diyagram, verinin uçtan uca yolculuğunu ve teknolojilerin birbirleriyle olan etkileşimini göstermektedir:

<div align="center">
  <img src="./diagram.png" alt="Architecture Diagram" width="800"/>
</div>

---

## 2. ELT Süreci: Verinin Yolculuğu

Sistemimiz klasik ETL (Extract, Transform, Load) yerine modern **ELT (Extract, Load, Transform)** yaklaşımını benimsemiştir.

### Adım 1: Extract & Load (Bronze Katmanı)
* **Teknoloji:** Apache Spark & Apache Iceberg (HDFS üzerinde)
* **Süreç:** `bronze_ingestion.py` kodu, klasördeki ham CSV dosyalarını okur.
* **Akıllı Özellik (Watermarking):** Spark kodu çalışırken `bronze_watermark.json` dosyasına bakarak hangi verilerin daha önce işlendiğini kontrol eder. Eğer CSV'de yeni bir satır yoksa, sistemi yormamak ve verileri çiftlememek (duplicate) için *append* işlemini es geçer.
* **Depolama:** Veriler modern bir tablo formatı olan **Apache Iceberg** formatında HDFS üzerine yazılır. Bu, devasa verilerde hızlı okuma/yazma (ACID işlemleri) sağlar.

### Adım 2: Transform (Silver & Gold Katmanı)
* **Teknoloji:** dbt (Data Build Tool) & Apache Doris
* **Süreç (Silver - Staging):** dbt, Iceberg'deki Bronz tablolara bağlanır. Sütun isimlerini düzeltir, boş (null) verileri temizler ve veri tiplerini (tarih, sayı vs.) standartlaştırır. Bu katmanda veriler fiziksel olarak kopyalanmaz, `View` (Sanal Tablo) olarak tutulur.
* **Süreç (Gold - Marts):** Temizlenen Silver tablolar birbiriyle (Join) birleştirilir. Satış Rakamları (`fct_orders`), Müşteri Boyutları (`dim_customers`) gibi analitik iş tabloları oluşturulur. Bu veriler süper hızlı sorgu performansı için **Apache Doris** veritabanına fiziksel olarak (`table`) yazılır.
* **Tarihsel İzleme (SCD Type 2):** Müşteri adres değişiklikleri gibi geçmiş verilerin kaybolmaması için dbt'nin **Snapshot** özelliği kullanılarak Geçerlilik Tarihi (Valid From/To) sütunlarıyla versiyonlama yapılır.

---

## 3. Otomasyon ve Orkestrasyon (Apache Airflow)

Tüm bu devasa sürecin manuel olarak yönetilmesi imkansızdır. Bu nedenle bir **Orkestra Şefi** olarak Apache Airflow devrededir.

* **Zamanlama:** Her gece saat 03:00'te uyanır.
* **Bağımlılık Yönetimi:** Önce Spark'ı tetikler. Spark ancak başarıyla ("Yeşil") bittikten sonra dbt dönüşümlerini başlatır.
* **Astronomer Cosmos Entegrasyonu:** dbt projesini dümdüz bir komut olarak çalıştırmaz. dbt'nin içindeki tablo bağımlılıklarını tek tek okur ve bunları Airflow arayüzünde görsel bir haritaya (Task Group) dönüştürür. Böylece Staging tabloları **paralel olarak** işlenerek ciddi bir zaman tasarrufu sağlanır.
* **Hata Yönetimi (SLA & Retry):** Herhangi bir görev çökerse, sistem akıllıca 2-4-8 dakika bekleyerek tekrar dener (Exponential Backoff). Eğer tüm süreç 2 saati aşarsa (SLA Miss), ilgili uyarı mekanizmalarını (Callback) tetikler.

---

## 4. Görselleştirme (Apache Superset)

Veriler Doris'te (Gold) mükemmel bir şekilde hazırlandıktan sonra, son kullanıcıya (Patronlar/Yöneticiler) sunulması gerekir.

* **Süreç:** Python ile yazılmış özel scriptler (`create_dashboard.py`, `register_tables.py`), Superset'in API'sine doğrudan bağlanır.
* **Otomasyon:** Yöneticilerin fareyle saatlerce tıklamasına gerek kalmadan, tek bir script ile veritabanı bağlantısı kurulur, metrikler (SQL) gönderilir ve Bar, Çizgi, Pasta gibi grafikler saniyeler içinde programatik olarak oluşturulup panoya (Dashboard) eklenir.

---

## 5. Projenin Sağladığı Katma Değerler (Executive Summary)
1. **Ölçeklenebilirlik:** Spark ve Doris kullanımı sayesinde veri boyutu terabaytlara çıksa bile sistem aynı performansta çalışır.
2. **Dayanıklılık:** Airflow otomasyonu, hata durumunda kendi kendini kurtaracak (Retry) mekanizmalara sahiptir.
3. **Veri Tutarlılığı:** Watermark mantığı veri çiftlenmesini (duplication) engellerken, dbt içindeki testler (`not_null`, `unique`, `test_payment_not_exceeds_order`) veri kalitesini makineden çıkmadan önce garanti altına alır.
4. **Hız:** Astronomer Cosmos ile bağımsız görevler paralel işlenirken, Apache Doris'in sütun odaklı (columnar) yapısı Dashboard ekranlarının saliseler içinde yüklenmesini sağlar.
