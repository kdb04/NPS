
redis_cache: redis-server config/redis_cache.conf
redis_queue: redis-server config/redis_queue.conf


web: bench serve  --port 8000


socketio: /home/nikhilsrivatsa/.nvm/versions/node/v19.9.0/bin/node apps/frappe/socketio.js


watch: bench watch


schedule: bench schedule

worker:  bench worker 1>> logs/worker.log 2>> logs/worker.error.log

