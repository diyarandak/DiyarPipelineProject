# 📊 Olist Veri Boru Hattı - Detaylı Analiz ve Proje Raporu

## 1. Proje Amacı ve Kapsamı
Bu projenin temel amacı, karmaşık, dağınık ve kirli olabilen gerçek dünya verilerini (Olist E-Ticaret Veriseti) alıp, büyük veri teknolojileri (Big Data Technologies) kullanarak ölçeklenebilir, güvenilir ve raporlanabilir bir yapıya kavuşturmaktır. 
Geleneksel veritabanları yerine **Apache Iceberg** kullanılarak modern Data Lakehouse mimarisi benimsenmiştir.

## 2. Mimari Kararlar ve Kullanılan Teknolojiler

- **Apache Spark (PySpark):** Verinin boyutu büyüdükçe Pandas gibi kütüphanelerin yetersiz kalacağı öngörülerek, dağıtık veri işleme motoru olarak PySpark tercih edilmiştir.
- **Apache Iceberg:** Veri gölünde ACID transaction (güvenli okuma/yazma) desteği sağlamak, zamanda yolculuk (time-travel) özelliklerinden faydalanmak ve şema evrimini (schema evolution) kolay yönetmek için kullanılmıştır.
- **Apache Airflow:** Görevleri otomatize etmek ve bağımlılıkları yönetmek (önce Bronze, sonra Silver, en son Gold katmanın sırasıyla çalışması) amacıyla güçlü bir orkestrasyon aracı olarak sürece dahil edilmiştir.
- **Docker Compose:** Ortam bağımsızlığı sağlamak ve tüm altyapının (HDFS, Spark, Superset, Airflow) tek bir `make setup` komutuyla her bilgisayarda aynı şekilde çalışmasını garantilemek için izole konteyner mimarisi kullanılmıştır.

## 3. Medallion Mimarisi ile Veri İşleme Aşamaları

### 🥉 Bronze Katmanı (Ingestion - Veri Alma)
- Tüm ham CSV dosyaları Kaggle üzerinden okunur.
- Hiçbir filtreleme veya tip dönüşümü yapılmadan `.writeTo("iceberg_catalog.bronze.table_name")` komutuyla doğrudan Data Lake'e (Veri Gölüne) yazılır.
- **Amaç:** Verinin orijinal halini kaybetmeden güvenli ve ucuz bir depolama alanına yedeklemek.

### 🥈 Silver Katmanı (Transformation & Data Quality - Temizleme)
- Ham veriler Bronze katmanından okunur. Sipariş tarihleri (timestamp) doğru standart formata çevrilir.
- Null (boş) veya eksik olan ürün boyutları, ağırlıkları ve analizde anlamsız sonuçlar doğuracak satırlar (data quality checks) temizlenir.
- Özellikle `product_category_name_translation` tablosu kullanılarak, Portekizce olan kategori isimleri (örneğin *beleza_saude*) İngilizce karşılıklarına (*health_beauty*) çevrilir. Böylece küresel bir analistin veriyi okuması ve anlaması sağlanır.

### 🥇 Gold Katmanı (Dimensional Modeling - Modelleme)
- Temizlenmiş (Silver) veriler, iş zekası (BI) araçlarının ve yöneticilerin en kolay anlayacağı **Yıldız Şema (Star Schema)** yapısına getirilir.
- **Fact Tabloları:** `Master_Sales_Auto` ve `Master_Payments_Auto` tabloları oluşturulur. Siparişler, sipariş edilen ürünler, fiyatlar, kargo ücretleri ve teslimat süreleri bu tablolar içinde iş zekasına hazır halde harmanlanır.
- **Dimension (Boyut) Tabloları:** Müşteri boyutları (Customer Dim) ve Ürün boyutları (Product Dim) ayrı ayrı normalize edilir.
- Bu katman sayesinde Superset gibi BI araçları, arka planda tablolar arası karmaşık JOIN işlemleri yapmak zorunda kalmaz ve devasa grafikler saniyeler içinde yüklenir.

## 4. İş Zekası (BI) Çıktıları ve Şirket İçin Karar Alma (Decision Making)

Superset üzerinde oluşturulan "Olist E-Ticaret Analitik Portfolyosu", şirket yöneticilerine şu kararları veri odaklı (data-driven) alma yeteneği sunar:

1. **Bölgesel Lojistik Optimizasyonu:** "Şehirlere Göre Kargo Maliyeti" haritası sayesinde kargo masraflarının en yüksek olduğu eyaletler (örneğin Kuzey Brezilya) tespit edilebilir. Bu eyaletlere yeni depolar (hub) kurularak teslimat süreleri ve kargo maliyetleri düşürülebilir.
2. **Pazarlama ve Kampanya Stratejileri:** "Kategori Kralları" (Health_Beauty, Watches vs.) çubuk grafiği üzerinden, şirketin cirosunu sırtlayan ana kategoriler net bir şekilde görülür. Buna özel indirim kampanyaları ve dijital reklam bütçeleri ayrılabilir.
3. **Taksit ve Finansal Yönetim:** Müşterilerin yoğunlukla kaç taksit tercih ettiği pasta grafiği ile analiz edilerek, kredi kartı bankalarıyla yapılacak komisyon ve taksit anlaşmaları şirket lehine optimize edilebilir.
4. **Operasyonel Başarı Oranları:** Teslimat başarı grafiği incelenerek, yolda iptal olan veya geciken siparişlerin yüzdesi görülür; buna göre kargo şirketleriyle olan sözleşmeler gözden geçirilebilir.

## 5. Sonuç
Bu proje; modern bir Veri Mühendisinin baştan sona veri çıkarma (Extract), dönüştürme (Transform), yükleme (Load) ve görselleştirme (Visualize) adımlarına tamamen hakim olduğunu kanıtlayan, üretime hazır (production-ready) güçlü bir analitik platformdur.