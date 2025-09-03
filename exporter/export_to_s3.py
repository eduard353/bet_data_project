import os
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
import uuid 
from sqlalchemy import create_engine, Column, String, Numeric, TIMESTAMP, Boolean
from sqlalchemy.dialects.postgresql import UUID 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# --- Конфиги PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bets")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# --- Конфиги MinIO (S3) ---
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "bets")

# --- Путь выгрузки ---
today = datetime.utcnow().strftime("%Y-%m-%d")
part_name = datetime.utcnow().strftime("part-%H%M%S.parquet")
s3_key = f"raw/{today}/{part_name}"


class Bet(Base):
    __tablename__ = "bets"
    bet_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    game = Column(String)
    amount = Column(Numeric)
    status = Column(String)
    timestamp = Column(TIMESTAMP)
    exported = Column(Boolean, default=False, nullable=False)



def main():
    session = Session()

    # 1. Забираем новые записи ORM
    new_bets = session.query(Bet).filter(Bet.exported == False).all()  # noqa: E712

    if not new_bets:
        print("[EXPORT] Нет новых записей для экспорта.")
        return

    # 2. В DataFrame
    df = pd.DataFrame([{
        "bet_id": str(b.bet_id),
        "user_id": str(b.user_id),
        "game": b.game,
        "amount": float(b.amount),
        "status": b.status,
        "timestamp": b.timestamp,
        "exported": b.exported
    } for b in new_bets])

    # 3. Конвертируем в Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "/tmp/export.parquet")

    # 4. Заливаем в MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )
    s3.upload_file("/tmp/export.parquet", S3_BUCKET, s3_key)

    print(f"[EXPORT] Загружено {len(df)} записей в s3://{S3_BUCKET}/{s3_key}")

    # 5. Отмечаем записи как exported
    for bet in new_bets:
        bet.exported = True
    session.commit()

    print("[EXPORT] Отмечены записи как exported.")
    session.close()


if __name__ == "__main__":
    main()
