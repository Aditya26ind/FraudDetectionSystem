from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap: str = "localhost:9092"
    kafka_fraud_topic: str = "transactions"
    
    # S3
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "fraud-detection"
    
    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    
    # Database 
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "fraud_detection"
    db_user: str = "postgres"
    db_password: str = "password"
    
    class Config:
        env_file = ".env"
        env_prefix = ""
        fields = {
            "kafka_bootstrap": {"env": ["KAFKA_BOOTSTRAP", "KAFKA_BOOTSTRAP_SERVERS"]},
            "kafka_fraud_topic": {"env": ["KAFKA_TOPIC", "KAFKA_FRAUD_TOPIC"]},
        }


def get_settings():
    return Settings()
