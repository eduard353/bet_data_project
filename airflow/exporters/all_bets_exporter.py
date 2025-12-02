import os
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, Column, String, Numeric, TIMESTAMP, Boolean, Integer
from sqlalchemy.dialects.postgresql import UUID 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import uuid

# --- Конфиги PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# --- Конфиги MinIO (S3) ---
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

# --- ORM-модель (соответствует bets из консьюмера) ---
class Bet(Base):
    __tablename__ = "bets"
    bet_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    # Основные поля для экспорта
    amount = Column(Numeric)
    currency = Column(String)
    status = Column(String)
    payout = Column(Numeric)
    bet_timestamp = Column(TIMESTAMP)
    # Метаданные/атрибуты пользователя и события (могут быть NULL)
    country = Column(String)
    age = Column(Integer)
    device = Column(String)
    ip_address = Column(String)
    sport = Column(String)
    league = Column(String)
    match = Column(String)
    selection = Column(String)
    odds = Column(Numeric)
    source = Column(String)
    session_id = Column(UUID(as_uuid=True))
    created_at = Column(TIMESTAMP)
    # Техническое поле
    exported = Column(Boolean, default=False, nullable=False)

def run_export():
    """Функция для Airflow PythonOperator"""
    # Валидация и подготовка настроек S3/MinIO
    # Примечание: таблица bets создаётся консьюмером (all_bets_consumer.py),
    # экспортер только читает данные из существующей таблицы
    endpoint = S3_ENDPOINT or "http://minio:9000"
    if not S3_ACCESS_KEY or not S3_SECRET_KEY:
        print("[EXPORT][ERROR] S3_ACCESS_KEY/S3_SECRET_KEY не заданы. Проверьте переменные окружения.")
        return
    if not S3_BUCKET:
        print("[EXPORT][ERROR] S3_BUCKET не задан. Укажите имя бакета в переменной окружения S3_BUCKET.")
        return

    session = Session()

    # 1. Забираем новые записи
    new_bets = session.query(Bet).filter(Bet.exported == False).all()  # noqa: E712

    if not new_bets:
        print("[EXPORT] Нет новых записей для экспорта.")
        session.close()
        return

    # 2. В DataFrame (только существующие в bets поля)
    rows = []
    for b in new_bets:
        rows.append({
            "bet_id": str(b.bet_id),
            "user_id": str(b.user_id),
            "amount": float(b.amount) if b.amount is not None else None,
            "currency": b.currency,
            "status": b.status,
            "payout": float(b.payout) if b.payout is not None else None,
            "bet_timestamp": b.bet_timestamp,
            "country": b.country,
            "age": b.age,
            "device": b.device,
            "ip_address": b.ip_address,
            "sport": b.sport,
            "league": b.league,
            "match": b.match,
            "selection": b.selection,
            "odds": float(b.odds) if b.odds is not None else None,
            "source": b.source,
            "session_id": str(b.session_id) if b.session_id is not None else None,
            "created_at": b.created_at,
            "exported": b.exported,
        })
    df = pd.DataFrame(rows)

    # 3. Конвертируем в Parquet
    today = datetime.utcnow().strftime("%Y-%m-%d")
    part_name = datetime.utcnow().strftime("part-%H%M%S.parquet")
    s3_key = f"raw/{today}/{part_name}"

    table = pa.Table.from_pandas(df)
    pq.write_table(table, "/tmp/export.parquet")

    # 4. Заливаем в MinIO
    from botocore.exceptions import ClientError
    
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",  # MinIO требует указания региона
    )

    # Создаём бакет, если его нет
    bucket_exists = False
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        bucket_exists = True
        print(f"[EXPORT] Бакет '{S3_BUCKET}' уже существует.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "404" or error_code == "NoSuchBucket":
            bucket_exists = False
        else:
            print(f"[EXPORT][ERROR] Ошибка при проверке бакета '{S3_BUCKET}': {e}")
            print(f"[EXPORT][ERROR] Код ошибки: {error_code}")
            session.close()
            return
    except Exception as e:
        print(f"[EXPORT][ERROR] Неожиданная ошибка при проверке бакета '{S3_BUCKET}': {e}")
        session.close()
        return

    if not bucket_exists:
        try:
            # Для MinIO можно создавать бакет без дополнительных параметров
            s3.create_bucket(Bucket=S3_BUCKET)
            print(f"[EXPORT] Создан бакет '{S3_BUCKET}'.")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "BucketAlreadyExists" or error_code == "BucketAlreadyOwnedByYou":
                print(f"[EXPORT] Бакет '{S3_BUCKET}' уже существует.")
            else:
                print(f"[EXPORT][ERROR] Не удалось создать бакет '{S3_BUCKET}': {e}")
                print(f"[EXPORT][ERROR] Код ошибки: {error_code}")
                print(f"[EXPORT][ERROR] Endpoint: {endpoint}")
                print(f"[EXPORT][ERROR] Access Key: {S3_ACCESS_KEY[:10]}..." if S3_ACCESS_KEY else "[EXPORT][ERROR] Access Key: НЕ ЗАДАН")
                session.close()
                return
        except Exception as e:
            print(f"[EXPORT][ERROR] Неожиданная ошибка при создании бакета '{S3_BUCKET}': {e}")
            session.close()
            return
    
    # Загружаем файл
    try:
        s3.upload_file("/tmp/export.parquet", S3_BUCKET, s3_key)
    except Exception as e:
        print(f"[EXPORT][ERROR] Не удалось загрузить файл в бакет '{S3_BUCKET}': {e}")
        session.close()
        return

    print(f"[EXPORT] Загружено {len(df)} записей в s3://{S3_BUCKET}/{s3_key}")

    # 5. Отмечаем записи как exported
    for bet in new_bets:
        bet.exported = True
    session.commit()
    session.close()
    print("[EXPORT] Отмечены записи как exported.")
