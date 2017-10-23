apt-get update
apt-get install php -y
apt-get install php-fpm -y
apt-get install php-redis -y
apt-get install php-mysql -y
apt-get install nginx -y
apt-get install redis-server -y
apt-get install subversion -y
apt-get install python-redis -y
apt-get install python-mysqldb -y
pip install requests

mkdir -p /data/www
mkdir -p /data/www/upload
mkdir -p /data/logs
mkdir -p /data/logs/api
mkdir -p /data/logs/manage
mkdir -p /data/logs/atomanger
mkdir -p /data/logs/nginx
mkdir -p /data/logs/php

cp -r ./crontab /data/crontab
cp -r ./config /data/config
