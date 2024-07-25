# ruby-backup-to-s3

## Описание
Скрипт сохранения файлов архивных копий в холодном S3 хранилище.

Для примера используется хранилище Selectel. [Документация](https://docs.selectel.ru/cloud/object-storage/) по работе с хранилищем.

## Процесс

1. Поиск файлов в выбранной директории по типу файлов и дате создания. Отправляются файлы текущей даты и ранее, не более `DAY_DELTA` дней
2. Подключение к хранилищу
3. Попытка загрузить подготовленные файлы в хранилище. Если файл присутствует в хранилище, то пропускается. 
