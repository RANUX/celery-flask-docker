from flask import Flask, render_template, request, jsonify
import sys

sys.path.append('../app')

from tasks_simple import add, sleep, echo, error

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/execute_task', methods=['POST'])
def execute_task():
    data = request.json
    task_name = data.get('task')
    
    try:
        if task_name == 'add':
            x = int(data.get('x', 0))
            y = int(data.get('y', 0))
            result = add.delay(x, y)
            task_result = result.get(timeout=30)
            return jsonify({
                'success': True, 
                'result': task_result,
                'task_id': result.id
            })
            
        elif task_name == 'sleep':
            seconds = int(data.get('seconds', 1))
            result = sleep.delay(seconds)
            task_result = result.get(timeout=60)
            return jsonify({
                'success': True, 
                'result': f'Задача завершена после {seconds} секунд',
                'task_id': result.id
            })
            
        elif task_name == 'echo':
            msg = data.get('message', 'Hello World')
            timestamp = data.get('timestamp', False)
            result = echo.delay(msg, timestamp)
            task_result = result.get(timeout=30)
            return jsonify({
                'success': True, 
                'result': task_result,
                'task_id': result.id
            })
            
        elif task_name == 'error':
            msg = data.get('error_message', 'Test error')
            result = error.delay(msg)
            task_result = result.get(timeout=30)
            return jsonify({
                'success': True, 
                'result': task_result,
                'task_id': result.id
            })
            
        else:
            return jsonify({'success': False, 'error': 'Неизвестная задача'})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
