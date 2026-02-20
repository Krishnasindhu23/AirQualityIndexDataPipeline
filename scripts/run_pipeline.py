import os

os.system("python scripts/data_split.py")
os.system("python scripts/ingest.py")
os.system("mysql -h localhost -P 3307 -u root -ppassword < sql/transform_aqi.sql")
os.system("python scripts/alert.py")
os.system("python scripts/dashbaord.py")
