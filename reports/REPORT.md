# Olist Big Data Pipeline - Proje Mimari ve Sonuç Raporu

## 1. Proje Özeti
Bu proje, Brezilya merkezli Olist e-ticaret platformunun ~100.000 gerçek sipariş verisini modern veri mühendisliği teknikleriyle işleyen uçtan uca bir Büyük Veri (Big Data) hattı projesidir. Veriler "Medallion Mimarisi" (Bronze -> Silver -> Gold) prensiplerine sadık kalınarak işlenmiş, temizlenmiş ve İş Zekası (BI) raporlamalarına hazır hale getirilmiştir.

## 2. Kullanılan Teknolojiler ve Mimari
- **Veri Kaynağı:** Kaggle API (Olist Dataset - 9 Adet CSV)
- **Veri İşleme Motoru:** Apache Spark (PySpark)
- **Tablo Formatı ve Depolama:** Apache Iceberg (HDFS/Docker üzerinde)
- **Veri Kalitesi (Data Quality):** PySpark (Tekrar eden veri ve Null kontrolü) ve Pytest
- **Orkestrasyon:** Apache Airflow
- **İş Zekası (BI) ve Görselleştirme:** Apache Superset

## 3. Veri Katmanları (Medallion Mimarisi)
- **Bronze Katman (Ham Veri):** Kaggle üzerinden indirilen CSV dosyaları, hiçbir manipülasyona veya veri kaybına uğramadan orijinal formatında (raw) Iceberg tabloları olarak depolanmıştır.
- **Silver Katman (Temizlenmiş Veri):** Bronze katmandan alınan veriler üzerinde deduplication (tekrar eden verilerin silinmesi), veri tipi dönüşümleri (örneğin string tarihlerin timestamp'e çevrilmesi) ve null değer temizliği yapılmıştır. İhtiyaç duyulmayan sütunlar bu aşamada elenmiştir.
- **Gold Katman (İş Modeli - Star Schema):** Temizlenmiş Silver veriler, analitik sorgular için en verimli yapı olan Kimball Star Schema modeline dönüştürülmüştür. 
  - **Fact Tabloları:** `fact_order_sales` ve `fact_order_payments` adında 2 ana süreç tablosu oluşturulmuştur.
  - **Dimension Tabloları:** `dim_customers`, `dim_sellers`, `dim_products`, `dim_geolocation`, `dim_dates` boyut tabloları ile model desteklenmiştir.

## 4. Pipeline Orkestrasyonu (Airflow)
Tüm süreç Apache Airflow kullanılarak otomatikleştirilmiştir. Yazılan Airflow DAG (Yönlü Döngüsüz Grafik) sayesinde Bronze, Silver ve Gold süreçleri sırasıyla ve birbirine bağımlı olarak çalıştırılmış, oluşabilecek hatalara karşı retry (yeniden deneme) mekanizmaları kurgulanmıştır.

## 5. Veri Kalitesi ve Standartlar
Kod tabanında tutarlılığı sağlamak amacıyla **Pre-commit hook** (Black, Flake8) yapıları kurulmuştur. Veri hatlarındaki (pipeline) hatalı, eksik (Null) veya tekrar eden kayıtları saptayıp DLQ (Dead Letter Queue) tablolarına yönlendiren PySpark fonksiyonları yazılmış, bunların doğruluğu **Pytest** üzerinden otomatize test edilmiştir. Ayrıca, projenin lokal ortamda baştan uca tek tuşla test edilebilmesi için `main.py` (Master Controller) mekanizması eklenmiştir.