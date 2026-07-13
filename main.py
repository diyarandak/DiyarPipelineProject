import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PipelineController")

def run_script(script_path):
    """
    Belirtilen Python dosyasını çalıştırır ve hataları kontrol eder.
    """
    logger.info(f"[{script_path}] BAŞLATILIYOR...")
    
    # Subprocess ile dosyayı ayrı bir süreçte çalıştırıyoruz
    result = subprocess.run([sys.executable, script_path])
    
    if result.returncode != 0:
        logger.error(f"❌ HATA: [{script_path}] çalışırken çöktü. Tüm süreç durduruldu!")
        sys.exit(1)
        
    logger.info(f"✅ BAŞARILI: [{script_path}] tamamlandı.\n" + "-"*50)

def main():
    logger.info("🚀 BÜYÜK VERİ (BIG DATA) PLATFORMU OTOMATİK SÜRECİ BAŞLIYOR 🚀")
    
    # Sırayla çalıştırılacak tüm sistemlerin listesi
    pipeline_scripts = [
        "processing/bronze_ingestion.py",      # 1. Aşama: Veriyi Oku ve HDFS'e at (Bronze/Lake)
        # 2. and 3. aşamalar (Silver and Gold) will now be handled by dbt via Airflow.
        "visualization/register_tables.py"     # 4. Aşama: Olan biteni otomatik olarak Superset'e bağla!
    ]
    
    for script in pipeline_scripts:
        run_script(script)
        
    logger.info("🎉🎉 MÜKEMMEL! TÜM VERİ BORU HATTI (PIPELINE) KUSURSUZ ÇALIŞTI! 🎉🎉")

if __name__ == "__main__":
    main()
