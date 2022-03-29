from src.infi.clickhouse_orm import Database


db = Database("test")
db.drop_database()