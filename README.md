# climate_data_exchange
python script to push aggregated data from Nepali Calendar to ISO Calendar 

Python Script to Auto sync Climet data from nepalhmis to dhis2 instance

# add flask for create web-app for DHIS2
## install

sudo apt install python3-pip

pip install flask requests python-dotenv

pip install --upgrade certifi

pip install --upgrade requests certifi urllib3

pip install flask-cors

pip install python-dotenv

pip install psycopg2-binary

pip install clickhouse-connect

pip install nepali-date_converter

pip install npdatetime

pip install datetime

#https://pypi.org/project/nepali-calendar-utils/

pip install nepali-calendar-utils

pip install nepali

pip install nepali-datetime


-- 
sudo apt update

sudo apt install python3-full python3-venv -y

-- Create virtual environment

cd /home/mithilesh/climet_data_exchange

python3 -m venv venv

-- Activate it

source venv/bin/activate

then

pip install nepali-datetime

pip install --upgrade requests certifi urllib3

pip install python-dotenv


-- now add cron inside that

0 2 * * * /home/mithilesh/climet_data_exchange/venv/bin/python /home/mithilesh/climet_data_exchange/run_exchange.py >> /home/mithilesh/climet_data_exchange/cron.log 2>&1


0 2 * * * /home/mithilesh/climet_data_exchange/venv/bin/python /home/mithilesh/climet_data_exchange/main.py >> /home/mithilesh/climet_data_exchange/cronlogs_aggregatedDataValue.log 2>&1

chmod +x /home/mithilesh/climet_data_exchange/pythonAggregatedDataValue.sh
chmod +x /home/mithilesh/climet_data_exchange/main.py

chmod 755 /home/mithilesh/climet_data_exchange/logs

0 2 * * * cd /home/mithilesh/climet_data_exchange && /home/mithilesh/climet_data_exchange/venv/bin/python main.py >> /home/mithilesh/climet_data_exchange/cronlogs_aggregatedDataValue.log 2>&1

-- final schedular
0 2 * * * /home/mithilesh/climet_data_exchange/pythonAggregatedDataValue.sh >> /home/mithilesh/climet_data_exchange/cronlogs_aggregatedDataValue.log 2>&1


00 22 * * *  /home/mithilesh/climet_data_exchange/pythonAggregatedDataValue.sh >> /home/mithilesh/climet_data_exchange/cronlogs_aggregatedDataValue.log 2>&1

