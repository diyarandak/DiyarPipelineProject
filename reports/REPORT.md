# 📋 Olist Big Data Pipeline — Proje Raporu

## 1. Proje Özeti

Bu rapor, Olist Brazilian E-Commerce veri seti üzerinde kurulan uçtan uca Big Data analytics pipeline'ının tasarım kararlarını, uygulama detaylarını ve sonuçlarını içermektedir.

**Temel hedefler:**
- 9 CSV tablosunun Medallion Mimarisi (Bronze/Silver/Gold) ile işlenmesi
- Apache Iceberg table format ile modern data lakehouse yaklaşımının uygulanması
- Kimball Star Schema ile analitik modelleme
- Apache Airflow ile pipeline orkestrasyonu
- Apache Superset ile görselleştirme

---

## 2. Mimari Tasarım

### 2.1 Medallion Mimarisi (Bronze / Silver / Gold)

_Bu bölüm pipeline geliştirildikçe güncellenecektir._

### 2.2 Apache Iceberg Tercih Gerekçesi

_Iceberg vs Hudi vs Delta Lake karşılaştırması eklenecektir._

### 2.3 Star Schema (Kimball) Tasarımı

_Fact ve Dimension tablo tasarımları eklenecektir._

---

## 3. Veri Kalitesi

### 3.1 Kalite Kontrol Sonuçları

_Her tablo için null oranı, duplicate sayısı ve kalite metrikleri eklenecektir._

### 3.2 Dead Letter Queue (DLQ)

_Hatalı kayıtların analizi eklenecektir._

---

## 4. Pipeline Orkestrasyonu (Airflow)

_Airflow DAG yapısı ve çalışma detayları eklenecektir._

---

## 5. Dashboard & Görselleştirme

_Superset dashboard ekran görüntüleri ve grafik açıklamaları eklenecektir._

---

## 6. Performans Metrikleri

| Metrik | Değer |
|--------|-------|
| Toplam CSV satır sayısı | _TBD_ |
| Bronze ingestion süresi | _TBD_ |
| Silver transformation süresi | _TBD_ |
| Gold modeling süresi | _TBD_ |
| Toplam pipeline süresi | _TBD_ |

---

## 7. Karşılaşılan Sorunlar ve Çözümler

_Geliştirme sürecinde karşılaşılan sorunlar ve çözümleri eklenecektir._

---

## 8. Gelecek Fazlar İçin Öneriler

- **Phase 2:** Apache Kafka ile real-time event streaming, Debezium CDC
- **Phase 3:** Apache Doris/Starrocks OLAP engine entegrasyonu
- **Phase 4:** Tam Lakehouse mimarisi — Iceberg + Kafka + Doris + dbt

---

## 9. Sonuç

_Proje tamamlandığında genel değerlendirme eklenecektir._