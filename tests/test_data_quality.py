import pytest
import os
import sys
import typing
sys.modules['typing.io'] = typing

from pyspark.sql import SparkSession

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processing.data_quality import check_nulls, check_duplicates

# PySpark 3.3.x uses an older cloudpickle version that fails on Python 3.11+
# We skip the test gracefully locally if Python version is >= 3.11
pytestmark = pytest.mark.skipif(
    sys.version_info >= (3, 11),
    reason="PySpark 3.3.x cloudpickle is incompatible with Python 3.11+. Run tests via 'make test' inside Docker."
)

# 1. BÖLÜM: SPARK SESSION (TEST MOTORU) KURULUMU
@pytest.fixture(scope="session")
def spark():
    """Testler için geçici bir lokal Spark motoru başlatır."""
    return SparkSession.builder.master("local[1]").appName("pytest-data-quality").getOrCreate()


# 2. BÖLÜM: BOŞ DEĞER (NULL) KONTROL TESTİ
def test_check_nulls(spark):
    """Kritik sütunlarda boş (Null) değer olduğunda sistemin yakalayıp yakalamadığını test eder."""
    # Sahte verimizi yaratıyoruz (Ortadaki müşterinin ID'si yok, yani Null)
    test_data = [
        ("MUSTERI_1", "İstanbul"),
        (None, "Ankara"), 
        ("MUSTERI_3", "İzmir")
    ]
    df = spark.createDataFrame(test_data, ["customer_id", "city"])
    
    # Asıl kodumuzu çalıştırıyoruz
    valid_df, invalid_df = check_nulls(df, critical_columns=["customer_id"])
    
    # Beklentilerimiz (Assert)
    assert valid_df.count() == 2   # 2 tane sağlam müşteri geçmeli
    assert invalid_df.count() == 1 # 1 tane hatalı müşteri yakalanmalı (DLQ'ya gitmek üzere)


# 3. BÖLÜM: TEKRAR EDEN (DUPLICATE) KONTROL TESTİ
def test_check_duplicates(spark):
    """Aynı siparişin yanlışlıkla 2 kere gelmesi durumunda sistemin yakalayıp yakalamadığını test eder."""
    # Sahte verimizi yaratıyoruz (A siparişi 2 kere girilmiş)
    test_data = [
        ("SIPARIS_A", 100),
        ("SIPARIS_A", 100),
        ("SIPARIS_B", 200)
    ]
    df = spark.createDataFrame(test_data, ["order_id", "price"])
    
    # Asıl kodumuzu çalıştırıyoruz
    valid_df, invalid_df = check_duplicates(df, key_columns=["order_id"])
    
    # Beklentilerimiz (Assert)
    assert valid_df.count() == 2   # A'nın teki ve B olmak üzere 2 sağlam sipariş kalmalı
    assert invalid_df.count() == 1 # A'nın kopya olan ikincisi hatalı (invalid) olarak yakalanmalı
