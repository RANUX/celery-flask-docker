from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import sys
import os

sys.path.append('../app')
from tasks import run_data_pipeline

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_pipeline', methods=['POST'])
def start_pipeline():
    """Запускает пайплайн обработки данных"""
    try:
        data = request.json
        data_size = int(data.get('data_size', 100))
        
        task_id = run_data_pipeline.delay(data_size)
        
        return jsonify({
            'success': True,
            'task_id': str(task_id),
            'message': 'Пайплайн запущен'
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
            'error': data.get('error')
        })
        
        return jsonify({'success': True})
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
