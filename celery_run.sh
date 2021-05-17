# Kill all celery workers/processes
pkill -9 -f 'celery worker'
pkill -f "celery"

# Run conventionally
celery -A celery_main worker --loglevel=INFO --logfile=./logs/celery/celery_main.log
celery -A celery_main worker --loglevel=INFO
celery -A celery_main flower --port=5555

# Run with task route
# Currently the --detach option is not working: the worker does not connect to the correct broker url
# celery -A celery_main worker -Q bittrex -c 10 -n bittrexWorker@h -l INFO --detach --logfile="./logs/celery/celery_main_%n.log" --pidfile="./run/celery/%n.pid"

celery -A celery_main worker -Q bittrex -c 10 -n bittrexWorker@h -l INFO --logfile="./logs/celery/celery_main_%n.log"

celery -A celery_main worker -Q bitfinex -c 10 -n bitfinexWorker@h -l INFO --logfile="./logs/celery/celery_main_%n.log"