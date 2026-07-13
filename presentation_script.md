# Olist Data Pipeline - Sunum Metni

## 1. Giriş (Introduction)

"Değerli hocalarım ve jüri üyeleri, merhaba. Bugün sizlere Olist E-Ticaret veri seti üzerinde geliştirdiğimiz Büyük Veri Analitik projemizi sunacağım. 

Bu projedeki temel amacımız; ham ve dağınık haldeki bir verinin, modern mühendislik standartlarında işlenerek şirket için değerli bir 'iş zekası' kararına dönüşme serüvenini sıfırdan inşa etmekti.

Şimdi hazırsanız, verinin bu yolculuğunun nasıl başladığına, yani ilk durağımız olan veri çekme aşamasına geçelim."

---

## 2. Medallion Mimarisi ve Adım 1: Verinin Çıkarılması (Bronze Katmanı)

"Projeye başlarken elimizdeki ham CSV dosyalarını doğrudan raporlara yansıtmak yerine, endüstri standardı olan **'Medallion (Madalyon) Mimarisi'ni** benimsedik. Bu mimari veriyi; ham halinden (Bronze), temizlenmiş haline (Silver) ve en son analize hazır iş modeline (Gold) doğru 3 aşamalı bir saflaştırma işleminden geçirir.

Sistemimizin ilk adımı olan **'Bronze'** katmanında, Apache Spark kullanarak ham veriyi okuduk. Ancak veriyi klasik bir formatta saklamak yerine, sütun bazlı (columnar) 'Parquet' formatını ve onun üzerine modern 'Apache Iceberg' tablo formatını giydirdik. Geleneksel HDFS sadece hantal bir 'Veri Gölü' iken; biz Iceberg sayesinde bu yapıyı ACID işlemlerini, yani CRUD (Kayıt Ekleme/Silme/Güncelleme) yeteneklerini destekleyen modern bir **'Veri Gölevi (Data Lakehouse)'** mimarisine dönüştürdük. Ayrıca veriyi göle atarken `sanitize_columns` fonksiyonumuzla boşluklu veya hatalı karakterli tüm düzensiz sütun isimlerini anında standartlaştırdık.

Burada jürinin dikkatini çekmek istediğim en kritik mühendislik detayı ise koda eklediğimiz **'Watermark'** sistemidir. Büyük veri dünyasında 'Watermark' kavramı iki farklı şekilde kullanılır ve biz bu projede mimarimize en uygun olanı seçtik.

Genel kültür olarak bahsetmek gerekirse; birinci tür olan **Streaming (Akan Veri) Watermark'ı**, saniye saniye akan verilerde (örneğin anlık web sitesi tıklamaları) "geç gelen veriyi (late data)" yönetmek için kullanılır. Örneğin sistem *"5 dakikaya kadar geciken tıklamaları kabul et, daha eski veriler gelirse hafızayı yormamak için reddet"* der.

Ancak bizim projemiz anlık akan bir yapı değil, gece 03:00'te otomatik çalışan bir **Batch (Yığın)** projesidir. Bu yüzden biz sektördeki ikinci tür olan **'ETL High-Water Mark (Incremental Loading)'** sistemini kullandık. 

Bizim yazdığımız bu akıllı sistem tam olarak şöyle çalışıyor: Spark, klasördeki CSV dosyalarını okuyup Iceberg'e aktardığında, her tablonun başarı durumunu küçük bir hafıza dosyasına (`watermark.json`) kaydeder. Ertesi gün sistem tekrar tetiklendiğinde körü körüne tüm CSV'leri baştan okumaz; önce gidip bu deftere bakar. Eğer bir dosyanın karşısında "SUCCESS" (Başarılı) yazıyorsa, *"Bu tabloyu zaten önceki günlerde işledim, zaman kaybetmeye gerek yok"* diyerek o tabloyu tamamen es geçer (Skip). Ancak işlem yarım kalmışsa ("FAILED") veya klasöre yepyeni bir tablo eklenmişse, sadece o yeni dosyayı okur ve Iceberg tablomuza **'Append' (Ekleme)** yöntemiyle yazar. *(Bu sırada audit amacıyla eklediğimiz `_ingested_at` sütunuyla da o verinin ne zaman sisteme girdiğini mühürleriz).*

Özetle, projemize eklediğimiz bu Incremental Loading (Tablo Bazlı Watermark) mantığı sayesinde işlenmiş dosyaları atlayarak verilerin çiftlenmesini (duplication) %100 engelledik, hem de sistemi yormadan saniyeler içinde sadece gerekli verileri işleyebilir hale getirdik."

---

## 3. Adım 2: ETL'den ELT'ye Geçiş ve Apache Doris (Silver & Gold Katmanı)

"Bronze katmanında veriyi Iceberg formatında başarıyla sakladıktan sonra, Medallion mimarisinin 'Silver' (Temizleme) ve 'Gold' (Analitik) katmanlarına geçiş yapıyoruz. 

Değerli hocalarım, bildiğiniz gibi projenin ilk fazında bu dönüşümlerin tamamını yine Spark kodlarıyla (ETL mantığıyla) yapıyorduk. Ancak bu gelişmiş (Advanced) mimaride geleneksel ETL (Extract, Transform, Load) yapısını terk edip, modern **ELT (Extract, Load, Transform)** yaklaşımına geçtik. Peki neden?

ETL mantığında veri önce hafızaya (RAM) alınır, Spark gibi araçlarla dönüştürülür, en son veritabanına yazılır. Bu durum veriler büyüdükçe çok ciddi bir sistem darboğazı (bottleneck) yaratır. Biz ise ELT mantığına geçerek, "veriyi işlemek için dışarı çıkarma, işleme (Transform) görevini güçlü veritabanının devasa işlemcilerine bırak (Pushdown)" dedik. ELT'nin en büyük avantajı, verinin ağ üzerinde taşınma maliyetini sıfırlaması ve dönüşümleri MPP hızında çok daha ucuz donanımlarla yapmasıdır. 

Bu noktada eski hantal yapı olan HDFS'i analitik sorgular için geride bırakıp sistemin merkezine **Apache Doris**'i yerleştirdik. HDFS ucuz disklerde devasa ham verileri saklamak için mükemmeldir, ancak sorgu hızı ve performansı olarak Doris ile uzaktan yakından kapışamaz. Apache Doris; inanılmaz hızlı, sütun bazlı (columnar) ve MPP (Devasa Paralel İşleme) mimarisine sahip gerçek zamanlı bir analitik veritabanıdır.

Doris, mimarimizde **iki temel yeteneği** ile öne çıkıyor:
1. **Doris'in HDFS'i Doğrudan Okuması (External Catalog ve sources.yml):** "Peki dbt, HDFS içindeki Iceberg verilerini nasıl okuyabiliyor?" diye sorabilirsiniz. Burada çok kritik bir mimari kuralı aydınlatmak isterim: **dbt HDFS'e bağlanmaz, dbt HDFS'in ne olduğunu bile bilmez!** dbt sadece `profiles.yml` üzerinden Apache Doris'e bağlanır ve SQL konuşur.
Bunu Apache Doris'in efsanevi **'External Catalog'** (Dış Katalog) özelliği sayesinde başardık. dbt içindeki `sources.yml` dosyamıza Doris'in dış kataloğunu (örneğin `hdfs_catalog`) kaynak olarak gösterdik. dbt kodu derleyip SQL'i Doris'e yolladığında; Doris *"Benim HDFS'teki Iceberg'e gitmem lazım"* der ve devasa MPP işlem gücüyle gidip HDFS'i okur. 
Yani özetle; dbt sadece emir veren bir beyin, komutu alıp HDFS'teki yükü sırtlayan ve işleyen kas gücü ise Apache Doris'tir.
2. **Internal (İç) Depolama ve Zone Map:** Doris, HDFS'ten okuyup temizlediği (Silver) ve iş modellerine dönüştürdüğü (Gold) verileri kendi 'Internal' (iç) disklerine kaydeder. Bunu yaparken **'Zone Map'** ve **'Key Column'** (Sıralama Anahtarı) indeksleme teknolojilerini kullanır. Bu eşsiz yetenek sayesinde panolardan (Dashboard) bir sorgu geldiğinde milyarlarca satırı tek tek taramak yerine, verinin bloğunu haritadan bulup nokta atışıyla milisaniyeler içinde getirir.

İşte tam bu noktada, HDFS'ten okuyup Doris'e yazdığımız bu dönüşüm orkestrasyonunu yönetmek için devreye **dbt (Data Build Tool)** giriyor. Neden Doris'in içine girip dümdüz SQL scriptleri yazmadık da dbt kullandık? 

Çünkü sadece SQL kullanmak veri mühendisliğinde ilkel kalır; düz SQL statiktir, test edilemez ve kod tekrarı yaratır. Sadece Spark kullanmak ise donanım canavarıdır ve karmaşık kod yazımı gerektirir. dbt ise bu iki dünyanın avantajlarını birleştirir: İçine gömülü Jinja (Python) şablonları sayesinde saf SQL'e tam anlamıyla programlanabilir bir 'Beyin' ekler. 
- Siz dbt'de kodunuzu değişkenler, `For` döngüleri ve `If` bloklarıyla dinamik bir yazılım dili gibi yazarsınız. 
- dbt arka planda bunu Doris'in anlayacağı kusursuz, optimize edilmiş saf SQL kodlarına **dönüştürür (compile eder)** ve çalışması için doğrudan Doris'e gönderir. Yani aslında çalışan şey yine SQL'dir ama onu yazan dbt'dir!
- Ayrıca `ref()` fonksiyonuyla hangi tablonun kime bağlı olduğunu kendi kendine çözüp (DAG) otomatik sıraya koyar.
- SQL'de sayfalar sürecek veri kalitesi testlerini (bu alan boş olamaz, benzersiz olmalı vb.) sadece tek satır YAML koduyla saf SQL sorgularına dönüştürür.

Projemizdeki dbt yapısını açtığımızda karşımıza temelde şu çok düzenli klasör mimarisi çıkıyor:
1. **models:** Tüm veri dönüşüm kodlarımızın, Staging (Silver) ve Marts (Gold) olarak klasörlenerek tutulduğu kalptir.
2. **tests:** Verinin doğruluğunu ve tutarlılığını ölçtüğümüz kalite testlerinin bulunduğu yerdir.
3. **seeds:** Ülke kodları gibi değişmeyen küçük verileri (CSV) sisteme veritabanı tablosu olarak hızlıca gömdüğümüz klasördür.
4. **macros:** Tıpkı yazılımdaki fonksiyonlar gibi, tekrar eden karmaşık SQL parçalarını tek bir yerde yazıp her yerde çağırmamızı sağlayan klasördür.

Şimdi dilerseniz bu klasörlerin içine girerek veriyi nasıl temizleyip altın (Gold) değerinde analitik tablolara dönüştürdüğümüzü tek tek inceleyelim.

---

## 4. Adım 3: Temizlik ve Standartlaştırma (Silver / Staging Katmanı)

"dbt hiyerarşimizde ilk durağımız olan `models/staging` klasörüne, yani Medallion mimarisinin **'Silver (Gümüş)'** katmanına geliyoruz.

Bu katmanın temel vizyonu şudur: **Fiziksel kopyalama yapma, sadece sanal olarak temizle.** 
Bronze katmanında tuttuğumuz ham verileri alıp Doris'in içine tekrar kocaman fiziksel tablolar olarak kaydetmiyoruz. dbt'nin gücü sayesinde (materialization ayarıyla) Staging modellerimizi sadece birer **'View' (Sanal Tablo)** olarak oluşturuyoruz. Böylece devasa boyutlardaki ham verileri boş yere kopyalayarak disk (storage) israfı yapmaktan kurtuluyoruz.

Peki bu sanal temizlik aşamasında (Staging'de) tam olarak ne yapıyoruz?
Ham veri her zaman kirli, düzensiz ve analize uygunsuz gelir. Biz Staging katmanında ağırlıklı olarak şu operasyonları gerçekleştiriyoruz:
1. **İsimlendirme Standartları:** Sütun isimlerini, analistlerin ve iş birimlerinin (Business) anlayacağı şekilde temizliyor ve İngilizce/Türkçe standartlarına oturtuyoruz.
2. **Veri Tipi (Casting) Dönüşümleri:** Kaynak sistemden metin (String) olarak gelen tarih sütunlarını gerçek `TIMESTAMP` tipine, metin olarak gelen finansal değerleri hesaplama yapılabilmesi için `DECIMAL` veya `FLOAT` tiplerine dönüştürüyoruz (Casting).
3. **Filtreleme ve Temizlik (DLQ Farkı):** Faz 1'deki Spark projemizde, bozuk (null) veya tekrarlayan verileri yakalamak için `.dropDuplicates()` veya `.dropna()` yazar, sorunlu verileri kaybetmemek için de onları uzun kodlarla yakalayıp ayrı bir klasöre **(Dead Letter Queue - DLQ)** yazardık. Yeni dbt mimarimizde ise bu temizliği doğrudan Staging `.sql` modellerimizin içine çok daha zarif bir şekilde gömdük. Örneğin kodumuzun içine `SELECT DISTINCT` yazarak tekrarları (duplicate) uçuruyor, `WHERE customer_id IS NOT NULL` diyerek boş satırları siliyoruz.

4. **Çift Dikiş Veri Kalitesi (Data Quality) Testleri (`schema.yml`):** "Peki madem `.sql` dosyasının içinde `IS NOT NULL` veya `DISTINCT` yazıp veriyi zaten siliyoruz, neden dbt klasöründeki `schema.yml` dosyasına gidip bir daha `not_null` ve `unique` testleri tanımlıyoruz?" diye düşünebilirsiniz. İşte büyük verideki yazılım mühendisliği vizyonu tam olarak budur: **Çift Dikiş Güvenlik (Defense in Depth).**
Biz bu YAML testlerini yazıyoruz çünkü; yarın bir gün takımdan biri yanlışlıkla `.sql` dosyasındaki filtreleme satırını silerse veya sisteme daha önce hiç görmediğimiz garip bir bozuk veri gelirse, bu durum Gold katmanına asla sızmamalı! 
Eğer yazdığımız SQL temizliği yeterli gelmezse, `schema.yml` devreye girer. dbt'nin felsefesi *"Bozuk veri düzeltilmez, kaynağında durdurulur"* şeklindedir. Test patladığında dbt boru hattını (pipeline) kırmızıya boyayıp anında durdurur ve yöneticilerin hatalı panolara (Dashboard) bakmasını %100 engeller.

Özetle Silver katmanı; kaba kirin SQL kodlarıyla atıldığı, `schema.yml` testleriyle de verinin üzerine 'Güven' damgasının vurulduğu yerdir. Veri burada diske yazılmaz, sadece sorgu anında anlık olarak temizlenerek asıl şovun yapılacağı 'Gold' katmanına servis edilir.

---

## 5. Adım 4: İş Modelleri ve Star Schema (Gold / Marts Katmanı)

"Staging katmanında verimizi sanal olarak temizleyip güvenilir hale getirdikten sonra, dbt hiyerarşisinde `models/marts` klasörüne, yani Medallion mimarisinin son ve en değerli adımı olan **'Gold (Altın)'** katmanına ulaşıyoruz.

Staging'deki sanal (View) tablolar teknik olarak temizdir ancak kaynak sistemden (Olist) geldiği gibi dağınıktır (Normalize). Çünkü Olist'in orijinal veritabanı, müşterilerin siparişlerini sisteme en hızlı şekilde kaydetmek için tasarlanmış bir **OLTP (Online Transaction Processing)** sistemidir. Ancak iş zekası uzmanları saniyede binlerce satış kaydetmek değil, milyonlarca geçmiş satışı saniyeler içinde topluca okumak, yani **OLAP (Online Analytical Processing)** yapmak isterler. İşte bu yüzden Gold katmanında ünlü veri ambarı gurusu Ralph Kimball'ın metodolojisini uygulayarak bu dağınık 'OLTP' verisini, analize hazır **'Star Schema (Yıldız Şema)'** adlı 'OLAP' mimarisine dönüştürüyoruz.

Peki Yıldız Şema nedir? 
Yıldız şemada merkezde milyonlarca satırlık sayısal ölçümleri (ciro, maliyet) ve hareketleri (siparişler) tutan devasa bir **Fact (Gerçek)** tablosu bulunur (Örneğin projemizdeki: `fact_orders`). Bu merkez tablonun etrafında ise, o hareketin *Kime, Ne Zaman, Hangi Ürünle* yapıldığını açıklayan, metinsel (açıklayıcı) detayların bulunduğu **Dimension (Boyut)** tabloları bir yıldızın köşeleri gibi etrafa dizilir (Örn: `dim_customers`, `dim_products`, `dim_date`). 

Neden üniversitelerde çok öğretilen 'Snowflake (Kar Tanesi)' şemasını değil de Star Schema'yı seçtik? 
Çünkü Snowflake şemasında boyutlar kendi içlerinde parçalara bölünür (örneğin ürünler tablosu ayrı, ürünlerin kategorileri ayrı tablo olur). Bu durum, analistlerin basit bir rapor çekerken bile zincirleme (iç içe) onlarca JOIN atmasını gerektirir, bu da devasa verilerde (Big Data) okuma performansını yerle bir eder. Biz okuma performansının ve sadeliğin zirvesi olan Star Schema'yı (Denormalize yapı) seçerek, iş birimlerinin sadece tek bir basit JOIN ile tüm detaylara saniyeler içinde ulaşmasını sağladık.

Bu katmandaki en önemli mühendislik farkımız ise depolama (yazma) stratejimizdir: 
Hatırlarsanız **Bronze** katmanında PySpark ile veriyi göle kaydederken **'Append' (Üstüne Ekleme)** modunu kullanıyorduk. Çünkü Bronze'a her gün yeni ham sipariş dosyaları akıyordu ve biz geçmişe dokunmadan sadece yeni gelenleri ekliyorduk.
Ancak **Gold** katmanında iş zekası tablolarımızı oluştururken dbt'de `materialized='table'` ayarını kullandık. Bu ayar, tablonun her dbt çalışmasında tamamen silinip baştan yaratılması, yani **'Overwrite' (Üzerine Yazma - Full Refresh)** anlamına gelir. 

Peki neden Gold katmanında sadece yeni gelenleri ekleyen (Incremental) bir yapı kurmadık da Overwrite yaptık? 
Çünkü Olist e-ticaret veri setimiz (yaklaşık 100 bin sipariş) Big Data dünyası için oldukça 'küçük' bir veri setidir. Apache Doris gibi muazzam işlem gücüne (MPP) sahip bir veri ambarı, 100 bin satırlık bir tabloyu baştan yaratmayı saniyeler bile sürmeden tamamlar. Sadece yeni gelenleri bulup birleştirmeye çalışmak (Incremental mantığı) bu boyuttaki verilerde sistemi hızlandırmaz, tam aksine kod karmaşıklığını (complexity) artırır ve gereksiz bir mühendislik yükü (Over-engineering) yaratır. Bu yüzden "Keep it simple" (Basit tut) vizyonuyla hareket edip 'Overwrite' (Table Materialization) modunu kullanarak tablolarımızı her seferinde hatasız ve taptaze baştan yaratmayı en verimli çözüm olarak gördük.

Süreç tam olarak şöyle son bulur: dbt yazdığımız SQL'leri derleyip Doris'e yollar. Doris, devasa işlem gücüyle (MPP) temiz sanal verileri birleştirip Yıldız Şemayı kurar ve nihai Gold tabloları kendi içine Overwrite ile yazar. 
Artık Tableau, PowerBI, Apache Superset gibi iş zekası araçları karmaşık dosyalara veya HDFS'e değil; doğrudan Doris'in içindeki bu tertemiz, analize hazır Gold tablolara bağlanarak yöneticilere milisaniyeler içinde rapor üretir."

---

## 6. Adım 5: dbt'nin Gücünü Artıran Klasörler (Seeds, Macros, Analyses ve Target)

"Gold katmanında Yıldız Şemamızı oluşturduk ancak dbt'nin projemize kattığı yazılım mühendisliği vizyonu sadece modellerden ibaret değil. Gelin projemizi devasa bir kurumsal yapıya dönüştüren diğer klasör yeteneklerimize bakalım:

**1. Seeds (Statik Veri Gömme):**
Raporlamalarda bazen dışarıdan, kolay kolay değişmeyen küçük referans verilerine ihtiyaç duyarız. Örneğin Brezilya'daki 'SP' veya 'RJ' gibi eyalet kodlarını uzun isimleri ve bölgeleriyle eşleştirmek istiyorduk. Bunun için karmaşık veri çekme (Ingestion) kodları yazmak yerine o dosyayı (`brazil_states.csv`) alıp doğrudan `seeds` klasörüne koyduk. Sadece `dbt seed` komutunu çalıştırdığımızda, dbt bu basit CSV dosyasını Doris veritabanımızın içine saniyeler içinde **gerçek bir tablo** olarak kaydetti.

**2. Macros (Yeniden Kullanılabilir Kodlar):**
Standart SQL'in en büyük zayıflığı 'kod tekrarıdır'. Örneğin, bir siparişin boyutunu sınıflandırmak veya eyaletlere göre bölge (Kuzey, Güney vb.) atamak için yazdığınız uzun bir `CASE WHEN` bloğunu düşünün. Bunu 10 farklı tabloda kullanacaksanız, 10 kere kopyala-yapıştır yapmanız gerekir. Biz bu hamallığı bitirmek için dbt'nin `macros` klasörünü kullandık. İçine `get_brazil_region` adında bir Jinja (Python) fonksiyonu yazdık. Artık herhangi bir .sql dosyasında sadece `{{ get_brazil_region('customer_state') }}` diyerek o yüzlerce satırlık kodu tek satırda çağırıp mükemmel bir kod standartizasyonu (DRY - Don't Repeat Yourself) sağladık.

**3. Analyses (Analitik Sorgular):**
Bazen iş birimlerinden 'Müşteri RFM Segmentasyonu' veya 'Aylık Gelir Trendi' gibi ad-hoc (tek seferlik) derin analiz talepleri gelir. Bu karmaşık sorguları veritabanında gereksiz yere kalıcı bir tablo (Table) veya sanal tablo (View) olarak kaydetmek sistemi kirletir. Bu yüzden biz bu tür sorguları `analyses` klasörüne yazdık. dbt bu dosyaların içindeki Jinja fonksiyonlarını ve referansları okur, saf SQL'e dönüştürür (compile eder) ancak **veritabanına tablo olarak kaydetmez**. Yalnızca bizim kullanmamız için hazır bir analiz kodu olarak bize sunar.

**4. Tests (Veri Kalitesi Güvencesi - PySpark vs dbt):**
Hatırlarsanız Faz 1'deki PySpark projemizde veri kalitesini ölçmek ve iş kurallarını test etmek için DataFrame'ler üzerinde uzun uzun manuel Python kodları ve if-else blokları yazıyorduk. dbt'de ise bu süreç `tests` klasörüyle inanılmaz bir seviyeye ulaştı. Projemizde iki farklı test türü yönetiyoruz:
- **Generic (Genel) Testler:** Bunlar modellerin yanındaki `schema.yml` dosyalarında yaşarlar. Örneğin bir sütunun altına sadece `not_null` veya `unique` yazarak dbt'nin bizim yerimize devasa test sorguları üretmesini sağlarız. Ayrıca `tests/generic/test_payment_not_exceeds_order.sql` dosyasında yazdığımız gibi kendi özel fonksiyonel testlerimizi yaratıp, bunları da YAML dosyasında kolayca çağırabiliyoruz.
- **Singular (Özel İş Kuralları) Testleri:** Bir de `schema.yml` ile çözülemeyecek kadar karmaşık iş kurallarımız vardır. Örneğin; *"Durumu teslim edildi (delivered) olan siparişlerin, teslimat tarihi boş olamaz!"*. İşte bu tarz çok spesifik testleri `tests/singular/assert_delivered_orders_have_delivery_date.sql` olarak saf SQL ile yazıyoruz. dbt bu dosyayı çalıştırır; eğer sonuç olarak geriye **0'dan fazla satır** (hatalı kayıt) dönerse, anında boru hattını (pipeline) kırmızıya boyayıp durdurur.

**5. Target Klasörü (Perde Arkasındaki Sihir):**
Değerli jüri üyeleri, dbt'yi dbt yapan asıl sihirbazlık projemizin kök dizinindeki `target` klasörüdür. 
Biz `models` klasöründe `{{ ref(...) }}` içeren, değişkenler atadığımız "Yarı-SQL" (Jinja) kodları yazarız. İşin gerçeği şudur ki; Apache Doris bu süslü kodları **anlayamaz**.
İşte biz terminale `dbt run` komutunu yazdığımızda sihir başlar:
- dbt önce bizim yazdığımız süslü kodları alır ve `target/compiled` klasörünün içinde Apache Doris'in anlayacağı, tüm makroların açıldığı **'Saf SQL'e** dönüştürür.
- Ardından bu saf SQL'leri alır, `target/run` klasörüne koyar ve Doris'e gönderip çalıştırır. 
- Ayrıca dbt'nin süreçlerimizde devrim yaratan **`state:modified`** komutu sayesinde çok büyük bir avantaja sahibiz. Projeye sadece bir tablo eklediğimizde 100 tablonun tamamını baştan çalıştırmak zorunda kalmayız. `dbt build -s state:modified+` komutunu kullanarak dbt'ye *"Sadece kodunu değiştirdiğim modeli ve ondan etkilenenleri çalıştır"* diyerek saatler sürecek işlem (compute) maliyetinden ve zamandan müthiş bir tasarruf sağlarız.
Yani kısacası; biz modern bir yazılım dili gibi kod yazarız, `target` klasörü bunu arka planda ilkel ve güçlü saf SQL'e çevirir.

**Veritabanı (Doris) ile Etkileşim Özeti:**
Burada mimari açıdan çok kritik bir detayın altını çizmek istiyorum: *"Hangi klasörler veritabanına (Doris) gider, hangileri dbt'nin bilgisayarında kalır?"*
- **Doris'e Gidenler (Fizikselleşenler):** Sadece `models` (Staging/Marts), `snapshots` ve `seeds` klasöründeki yapılar Doris'e gönderilir ve orada gerçek bir Tablo veya Sanal Tablo (View) olarak kaydedilir.
- **Doris'e Gitmeyenler (Mutfakta Kalanlar):** `macros`, `analyses` ve `target` klasörleri asla Doris'e kaydedilmez. Doris makronun veya analizin ne olduğunu bilmez. Bunlar sadece dbt'nin bilgisayarımızda saf SQL üretmek için kullandığı "mutfak" araçlarıdır. Doris klasörleri görmez, sadece dbt mutfağından kendisine fırlatılan son yemeği (saf SQL kodunu) görür ve çalıştırır."

---

## 7. Adım 6: Zaman Makinesi ile Tarihçe Takibi (SCD Type 2 ve Snapshots)

"Projemizin mühendislik açısından en havalı ve çözülmesi en zor problemlerinden biri olan 'Müşteri Tarihçesi Takibi' konusuna, yani **`snapshots`** klasörümüze geliyoruz. 

Normalde bilişim dünyasında 'Snapshot' demek, veritabanının o anki fotoğrafını (yedeklemesini) almak demektir. Ancak dbt terminolojisinde Snapshot'ın anlamı çok daha derindir; dbt'de Snapshot demek **'SCD (Slowly Changing Dimensions)'** yani 'Yavaş Değişen Boyutlar' problemini çözmek demektir.

Peki SCD nedir?
Bir müşterinin adresi veya medeni hali her saniye değişmez (yavaş değişir). Ancak değiştiğinde eski veriye ne olacağı veri ambarı mimarisinin en temel problemidir. Literatürde bu problemi çözmek için farklı yöntemler (SCD 0'dan 6'ya kadar) vardır. Örneğin:
- **SCD Type 0:** Değişikliği reddeder, ilk veriyi sonsuza dek korur.
- **SCD Type 1:** Eski verinin üstünü çizer (Overwrite), sadece son durumu tutar. Geçmiş kaybolur.
- **SCD Type 2:** Eski veriyi arşive kaldırır, yanına yeni satır açarak tüm tarihçeyi korur.
- **SCD Type 3:** Tabloya 'Önceki Şehir', 'Şimdiki Şehir' diye sütunlar ekleyerek sadece 1 adım geriyi tutar.
- **SCD 4, 5, 6:** Tarihçe tutan mini tablolarla güncel durumu tutan dev tabloların kompleks hibrit birleşimleridir.

Biz projemizde, geçmişteki siparişlerin hangi ilden verildiği bilgisi analiz için çok kritik olduğundan, tarihi asla silmeyen **SCD Type 2** metodolojisini seçtik. 

**SCD Type 2 (Snapshot) ile Incremental Yükleme Arasındaki Fark:**
Burada çok ince ama kritik bir mühendislik farkı var. Hem Incremental yükleme hem de Snapshot, arka planda veritabanına `MERGE` (Güncelle ve Ekle) işlemi yollar. Ancak iş mantıkları zıttır: Incremental yapı SCD Type 1 gibi çalışır; hedef tablodaki müşterinin adresini ezer (Overwrite) ve geçmişi yırtıp atar. Snapshot (SCD Type 2) ise geçmişe dokunmaz, yeni bir satır olarak ekler.

**Faz 1 (Spark) vs dbt (Snapshot):**
Faz 1 projemizde SCD Type 2 kurmak bir kabustu. Spark'ta `valid_from` (başlangıç), `valid_to` (bitiş) ve `is_active` (aktif mi?) sütunlarını manuel yaratıp, çok karmaşık Join'lerle eski kayıtların tarihlerini kendi ellerimizle kapatmak zorundaydık. 
Ancak dbt'de işler çok daha zarif çözüldü. dbt'de tarihçeyi takip etmenin iki temel stratejisi vardır:
1. **Timestamp Stratejisi:** Kaynak veride güvenilir bir `updated_at` (son güncellenme tarihi) sütunu varsa, dbt sadece bu tarihe bakarak satırın değişip değişmediğini anlar.
2. **Check Stratejisi:** Eğer sistemde güvenilir bir tarih sütunu yoksa, dbt sizin verdiğiniz belirli sütunları mercek altına alır; içindeki metin/sayı değiştiği an değişikliği yakalar.

Biz kodumuzda **`strategy='check'`** yöntemini kullandık ve `check_cols` ayarına `['customer_zip_code_prefix', 'customer_city', 'customer_state']` yazdık. Yani dbt'ye şunu dedik: *"Sadece müşterinin şehri, eyaleti veya posta kodu değişirse bu müşteriyi takibe al ve tarihçesini tut!"*
Ayrıca config içine `target_schema='gold'` yazarak, oluşan bu tarihçe yapısını doğrudan Doris'in içindeki Gold katmanına kalıcı bir **Fiziksel Tablo (Table)** olarak kaydettik. (Snapshot'lar sanal View olamaz, her zaman fiziksel tablodur).

Sonuç olarak müşteri Ankara'dan İstanbul'a taşındığında dbt kendi kendine şunu yaptı:
1. Ankara satırının `dbt_valid_to` sütununa bugünün tarihini basıp onu arşive gömdü.
2. Altına yepyeni bir İstanbul satırı açıp `dbt_valid_from` sütununa bugünü yazdı ve aktifleştirdi.

Böylece analistler *"Bu müşteri geçen sene 15 Kasım'da sipariş verdiğinde nerede yaşıyordu?"* diye sorduklarında, sistemimiz bir zaman makinesi gibi tam o günkü şehri onlara anında sunabilir hale geldi."

---

## 8. Adım 7: Orkestrasyon ve Otomasyon (Apache Airflow & Cosmos)

"Projemizdeki ham verileri PySpark ile okuduk, HDFS'e yazdık, dbt ve Doris ile modelledik, temizledik ve test ettik. Peki tüm bu işlemleri her gece sırasıyla kim çalıştıracak? Eğer ortada bir maestro (orkestra şefi) yoksa, enstrümanların ne kadar iyi olduğunun bir önemi kalmaz. İşte projemizin beyni ve orkestra şefi **Apache Airflow'dur.**

Airflow klasörümüzdeki DAG (Yönlendirilmiş Döngüsüz Grafik) dosyamıza (`olist_pipeline_dag.py`) baktığımızda, sadece kodu çalıştıran değil, kurumsal seviyede (Enterprise-grade) kontroller yapan bir otomasyon inşa ettiğimizi görüyoruz:

**1. Zamanlama, Retry ve SLA (Hata Yönetimi):**
Boru hattımızın (pipeline) her gece saat 03:00'da (`0 3 * * *`) çalışmasını planladık. Olası anlık bağlantı kopmalarına karşı `retries: 3` (3 kere tekrar dene) ve `retry_exponential_backoff` ayarı verdik. Ayrıca sisteme bir SLA (Hizmet Seviyesi Taahhüdü) ekleyerek, *"Tüm süreç 2 saat içinde bitmezse bana alarm yolla"* komutunu (sla_miss_callback) tanımladık.

**2. Bronze Katmanı Tetikleyicisi:**
Airflow'daki ilk görevimiz (Task 1), `BashOperator` kullanarak yazdığımız PySpark kodunu (`bronze_ingestion.py`) çalıştırmaktır. Bu komutla Spark tetiklenir ve ham veriler HDFS'e çekilir.

**3. dbt ve Astronomer Cosmos Sihri:**
PySpark bitince dbt'yi başlatmamız gerekiyordu. Çoğu standart projede dbt, `BashOperator` içine `dbt run` yazılarak çalıştırılır. Bunun en büyük eksisi; Airflow ekranında tüm dbt projesinin tek bir kutu olarak görünmesidir. İçerideki hangi SQL'in patladığını Airflow UI'dan (arayüzünden) göremezsiniz.
Biz bu problemi çözmek için **'Astronomer Cosmos'** kütüphanesini kullandık! Aslında Apache Airflow zaten doğası gereği birbirine bağımlı olmayan görevleri paralel çalıştırma yeteneğine sahiptir. Ancak Cosmos'un asıl sihri şudur: Airflow gidip dbt klasörümüzü okudu ve içindeki her bir '.sql' modelini otomatik olarak tek tek ayrı Airflow görevlerine dönüştürdü. Biz 50 modeli elimizle Airflow'a (Python koduyla) yazmak zorunda kalmadık! Bu otomatik dönüşüm sayesinde Airflow, dbt'nin bağımlılık ağacını (DAG) anladı ve **birbiriyle alakasız/bağımsız olan SQL modellerini kendi yeteneğiyle aynı anda (Paralel) çalıştırdı**. Böylece Airflow ekranına baktığımızda dbt içindeki onlarca modelin paralel olarak hızla yeşil yandığını devasa ve harika bir harita olarak izleyebiliyoruz.

**4. Bağımlılık Akışı (Dependencies):**
Kodun en altına `bronze_task >> dbt_tg` yazarak zinciri kurduk: *"PySpark ile veri çekme (Bronze) işlemi başarıyla bitmeden, asla dbt (Silver/Gold) analizlerini başlatma!"*

**5. Sektörde Airflow'un Hayat Kurtaran Diğer Özellikleri:**
Projemizde temel özelliklerini kullansak da, Airflow'un büyük veri dünyasında neden alternatifsiz bir standart olduğuna dair şu hayat kurtaran özelliklerini de belirtmek isterim:
- **Backfilling (Geçmişi Yeniden İşleme):** Sisteme yepyeni bir iş kuralı veya tablo eklediğimizde, *"Geriye dönük son 3 ayın verisini bu yeni kuralla tekrar işle"* demek Airflow'da sadece bir komuttur (`catchup=True`). Manuel olarak gün gün kod çalıştırma derdini bitirir.
- **Sensors (Akıllı Sensörler):** Airflow sadece saate bakarak körü körüne çalışmaz. Örneğin `FileSensor` kullanarak *"S3'e veya HDFS'e beklediğim dosya gelmediyse kodu başlatma, gelene kadar bekle"* diyerek boşuna hata alınmasını engeller.
- **Alerting (Otomatik Slack/Teams Bildirimi):** Bir görev (Task) çöktüğünde veya veride bir bozukluk olduğunda, Airflow `on_failure_callback` özelliği sayesinde anında veri mühendislerinin Slack kanalına hatanın loglarıyla beraber kırmızı bir alarm mesajı yollar.
- **Güvenlik (Connections & Secrets):** Veritabanı (Doris vs.) şifrelerini asla kodun içine (hardcoded) yazmayız. Airflow'un arayüzündeki şifreli kasa yapısında tutarız. Kod Github'da herkese açık olsa bile şifrelerimiz güvendedir.

---

## 9. Adım 8: Görselleştirme ve İş Zekası (Apache Doris vs Spark Thrift)

"Verimizi altın (Gold) standartlarında işledik ve orkestrasyonumuzu kurduk. Artık yöneticilerin, pazarlama ve finans ekiplerinin bu veriyi anlamlı grafikler (Dashboard) üzerinden tüketme zamanı geldi. Biz görselleştirme katmanımızda Apache Superset (veya Tableau/PowerBI) kullanarak doğrudan **Apache Doris**'e bağlandık.

**Neden Apache Doris Görselleştirmede (BI) Efsanedir?**
Burada Faz 1 (Spark) projemizi hatırlayalım. Tableau, PowerBI veya Superset gibi iş zekası (BI) araçları arka planda PySpark veya Spark kodlarını (Dataframe) anlayamazlar; onlar **sadece klasik SQL** diliyle konuşabilirler. Bu yüzden Faz 1'de araya bir köprü, yani **Spark Thrift Server** kurmak zorundaydık. Pano SQL yolluyor, Thrift bunu Spark'ın anlayacağı görevlere (Jobs) çevirip Spark kümesine (Cluster) iletiyordu. Ancak bu köprünün sektörde bilinen çok büyük mimari sakıncaları vardır:
1. **Yüksek Gecikme (Çeviri ve JVM Maliyeti):** Panodan gelen bir SQL, Thrift tarafından Spark'a çevrilip çalıştırılana kadar arka planda ağır JVM (Java) süreçleri ayağa kalkar. Bu "köprüdeki çeviri" ve başlatma süresi nedeniyle, bir yönetici panoyu açtığında basit bir grafiğin bile yüklenmesi saniyeler, bazen dakikalar sürer.
2. **Eşzamanlılık (Concurrency) Çöküşü:** Spark Thrift, aynı anda 50 analist panolara girip filtreleri değiştirdiğinde hızlıca boğulur ve hafıza (Memory/OOM) hataları verip çökmeye çok müsaittir. Yüksek trafikli anlık BI sorguları için optimize edilmemiştir.

**Doris ile Gelen Dashboard Devrimi:**
Biz panolarımızı doğrudan **Apache Doris**'e bağlayarak bu "Tercüman (Thrift)" mantığını tamamen yıktık. Çünkü Doris, zaten doğası gereği SQL konuşan ve tamamen 'Gerçek Zamanlı Analitik (Real-Time OLAP)' için sıfırdan C++ ile yazılmış devasa paralel (MPP) bir veritabanıdır. Arada hiçbir çevirici (köprü) yoktur!
- Panolardan gelen saf SQL sorgusu Doris'e doğrudan ulaştığında, Doris daha önce kurduğumuz **Zone Map** (Blok Haritası) indeksleri sayesinde tüm veriyi okumakla vakit kaybetmez; sadece grafiğin ihtiyaç duyduğu spesifik veri bloğunu bulup getirir.
- Eşzamanlı yüzlerce analist aynı anda filtre değiştirse bile çökmeden, milisaniyeler (sub-second) içinde cevap verir.

Sonuç olarak; Spark Thrift Server'ın o hantal, yüklenme tekerleği (loading) döndüren yavaş yapısından kurtulduk. İş birimlerine, tıkladıkları anda tepki veren, milisaniye hızında çalışan interaktif ve modern yönetim panoları (Dashboards) sunduk."

---

**Sonuç ve Teşekkür**
Sayın jüri üyeleri, Faz 1'deki hantal, teste kapalı ve yönetimi zor Spark ETL yapısını tamamen değiştirerek; veri kalitesini dbt testleriyle garanti altına aldığımız, sorgu hızını Apache Doris'in MPP gücüyle şaha kaldırdığımız ve orkestrasyonunu Airflow Cosmos ile ilmek ilmek işlediğimiz bu modern **ELT Medallion** mimarisini gururla inşa ettik. 
Bizi dinlediğiniz için teşekkür ederiz, varsa sorularınızı yanıtlamaktan memnuniyet duyarız."
