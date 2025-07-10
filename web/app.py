from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import redis
import json
import sys
import os

sys.path.append('../app')
from tasks import run_interactive_pipeline

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

# Redis для хранения пользовательского ввода
redis_client = redis.Redis.from_url(os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"))

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_pipeline', methods=['POST'])
def start_pipeline():
    """Запускает интерактивный пайплайн обработки данных"""
    try:
        data = request.json
        data_size = int(data.get('data_size', 100))
        
        task_id = run_interactive_pipeline.delay(data_size)
        
        return jsonify({
            'success': True,
            'task_id': str(task_id),
            'message': 'Интерактивный пайплайн запущен'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/task_result', methods=['POST'])
def receive_task_result():
    """Получает результаты от Celery задач и отправляет через WebSocket"""
    try:
        data = request.json
        
        # Отправляем результат всем подключенным клиентам
        socketio.emit('task_update', {
            'task_name': data.get('task_name'),
            'result': data.get('result'),
            'status': data.get('status'),
            'timestamp': data.get('timestamp'),
            'error': data.get('error'),
            'request_input': data.get('request_input')
        })
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/submit_input', methods=['POST'])
def submit_input():
    """Принимает пользовательский ввод и передает его задаче"""
    try:
        data = request.json
        task_id = data.get('task_id')
        user_input = data.get('input')
        
        # Сохраняем ввод пользователя в Redis
        input_key = f"user_input:{task_id}"
        redis_client.set(input_key, json.dumps(user_input), ex=300)  # 5 минут TTL
        
        return jsonify({'success': True, 'message': 'Ввод принят'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@socketio.on('connect')
def handle_connect():
    print('Клиент подключился')
    emit('connected', {'message': 'Подключение установлено'})

@socketio.on('disconnect')
def handle_disconnect():
    print('Клиент отключился')

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=8000, debug=True, allow_unsafe_werkzeug=True)
