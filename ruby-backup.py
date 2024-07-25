"""
Скрипт отправки бэкапов в объектное хранилище (Selectel, к примеру).

BACKUP_DIR указывает на директорию, содержащую файлы архивов. Указывать полный путь.
BACKUP_EXT содержит список расширений файлов для передачи (с точкой в начале, например, .7z, .zip)
DAY_DELTA количество дней для отправки (позволяет переотправить "вчерашние" данные в случае возникновения ошибок)
"""

import logging
import pathlib
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import urllib3

DAY_DELTA = 3
BACKUP_DIR = r"/home/backup"
BACKUP_EXT = (".7z", ".gz", )
S3_URL = "https://storage-backup-test.s3.storage.selcloud.ru"
S3_BUCKET = "BUCKET-NAME"
S3_ACCESS_KEY = "XXXXXXX"
S3_SECRET_KEY = "XXXXXXX"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

current_date = datetime.now()

logger.info("Запущен процесс бэкапа на S3")

# Получить список файлов директории бэкапа BACKUP_DIR с выбранными расширениями
path = pathlib.Path(BACKUP_DIR)
files_for_backup = [x for x in path.iterdir() if not x.is_dir() and x.suffix in BACKUP_EXT]

# Собрать файлы для передачи, находящиеся в рамках DAY_DELTA дней.
# Мы обрабатываем не только сегодняшние файлы, но и DAY_DELTA дней назад, вдруг вчера не отправили что-то
file_for_transfers = []
for file in files_for_backup:
    create_date = datetime.fromtimestamp(file.stat().st_ctime)
    days = (current_date - create_date).days
    if days <= DAY_DELTA:
        file_for_transfers.append(file)

if not file_for_transfers:
    exit(0)

logger.info(f"Планируем отправить следующие файлы: {[str(f) for f in file_for_transfers]}")

# Подключение и отправка в Object storage файлов
s3_session = boto3.Session(aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)
s3_client = s3_session.client("s3", endpoint_url=S3_URL, verify=False)

logger.info(f"Подключены к s3: {S3_URL}")

for file in file_for_transfers:
    try:
        try:
            # Проверка файла на наличие в хранилище
            s3_client.head_object(Bucket=S3_BUCKET, Key=file.name)
            logger.info(f"Файл пропущен {file.name}, присутствует в хранилище")
        except ClientError as client_exception:
            # Файла в хранилище нет, загружаем
            if client_exception.response["Error"]["Code"] == "404":
                response = s3_client.upload_file(file, S3_BUCKET, file.name)
                logger.info(f"Файл {file.name} успешно загружен в хранилище")
    except ClientError as global_client_exception:
        logger.error(f"Error uploading file {file.name}. Message: {str(global_client_exception)}")
    except Exception as process_exception:
        logger.error(f"Error processing {str(process_exception)}")

logger.info(f"Процесс завершён")
