# Kill all celery workers/processes
# Kill all processes related to the main app
pkill -9 -f 'celery_app.celery_main'

# Run with task route
# Currently the --detach option is not working: the worker does not connect to the correct broker url:
# celery -A celery_main worker -Q bittrex -c 10 -n bittrexWorker@h -l INFO --detach --logfile="./logs/celery/celery_main_%n.log" --pidfile="./run/celery/%n.pid"

celery -A celery_app.celery_main worker -Q bitfinex -c 4 -n bitfinexWorker@h -l INFO --logfile="./logs/celery/celery_main_%n.log"

celery -A celery_app.celery_main worker -Q bittrex -c 4 -n bittrexWorker@h -l INFO --logfile="./logs/celery/celery_main_%n.log"

celery -A celery_app.celery_main worker -Q binance -c 4 -n binanceWorker@h -l INFO --logfile="./logs/celery/celery_main_%n.log"

celery -A celery_app.celery_main worker -Q update_all -c 4 -n updateWorker@h -l INFO --logfile="./logs/celery/celery_main_%n.log"