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
        "processing/bronze_ingestion.py",      # 1. Aşama: Veriyi Raw'dan Bronze'a al (Ham Veri)
        "processing/silver_transformation.py", # 2. Aşama: Veriyi temizle ve Silver'a at (Kalite)
        "processing/gold_modeling.py",         # 3. Aşama: Star Schema ile Gold tabloyu oluştur (Analiz)
        "visualization/register_tables.py"     # 4. Aşama: Olan biteni otomatik olarak Superset'e bağla!
    ]
    
    for script in pipeline_scripts:
        run_script(script)
        
    logger.info("🎉🎉 MÜKEMMEL! TÜM VERİ BORU HATTI (PIPELINE) KUSURSUZ ÇALIŞTI! 🎉🎉")

if __name__ == "__main__":
    main()
