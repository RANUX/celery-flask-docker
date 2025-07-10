from celery import Celery, chain
import os
import time
import random
import requests
from datetime import datetime

broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
backend_url = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

app = Celery('tasks', broker=broker_url, backend=backend_url)

# URL веб-приложения для отправки уведомлений
WEB_APP_URL = os.environ.get("WEB_APP_URL", "http://web:8000")

def send_notification(task_name, result, status="success", error=None):
    """Отправляет уведомление веб-приложению о результате задачи"""
    try:
        data = {
            "task_name": task_name,
            "result": result,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "error": error
        }
        requests.post(f"{WEB_APP_URL}/task_result", json=data, timeout=5)
    except Exception as e:
        print(f"Ошибка отправки уведомления: {e}")

@app.task(bind=True)
def step1_data_preparation(self, data_size=100):
    """Шаг 1: Подготовка данных"""
    task_name = "Подготовка данных"
    try:
        # Симулируем подготовку данных
        for i in range(5):
            time.sleep(1)
            progress = (i + 1) * 20
            send_notification(task_name, f"Прогресс: {progress}%", "progress")
        
        result = f"Подготовлено {data_size} записей"
        send_notification(task_name, result, "success")
        return {"data_size": data_size, "status": "prepared"}
    except Exception as e:
        send_notification(task_name, str(e), "error", str(e))
        raise

@app.task(bind=True)
def step2_data_processing(self, prev_result):
    """Шаг 2: Обработка данных"""
    task_name = "Обработка данных"
    try:
        data_size = prev_result.get("data_size", 0)
        
        # Симулируем обработку
        for i in range(4):
            time.sleep(1.5)
            progress = (i + 1) * 25
            send_notification(task_name, f"Обработано {progress}% данных", "progress")
        
        # Случайно генерируем количество обработанных записей
        processed = int(data_size * random.uniform(0.8, 1.0))
        result = f"Обработано {processed} из {data_size} записей"
        send_notification(task_name, result, "success")
        return {"processed": processed, "original": data_size}
    except Exception as e:
        send_notification(task_name, str(e), "error", str(e))
        raise

@app.task(bind=True)
def step3_data_analysis(self, prev_result):
    """Шаг 3: Анализ данных"""
    task_name = "Анализ данных"
    try:
        processed = prev_result.get("processed", 0)
        
        # Симулируем анализ
        for i in range(3):
            time.sleep(2)
            stage = ["Статистический анализ", "Поиск паттернов", "Генерация отчета"][i]
            send_notification(task_name, f"Выполняется: {stage}", "progress")
        
        # Генерируем результаты анализа
        insights = random.randint(5, 15)
        anomalies = random.randint(0, 3)
        result = f"Найдено {insights} инсайтов и {anomalies} аномалий в {processed} записях"
        send_notification(task_name, result, "success")
        return {"insights": insights, "anomalies": anomalies, "processed": processed}
    except Exception as e:
        send_notification(task_name, str(e), "error", str(e))
        raise

@app.task(bind=True)
def step4_generate_report(self, prev_result):
    """Шаг 4: Генерация отчета"""
    task_name = "Генерация отчета"
    try:
        insights = prev_result.get("insights", 0)
        anomalies = prev_result.get("anomalies", 0)
        processed = prev_result.get("processed", 0)
        
        # Симулируем генерацию отчета
        for i in range(3):
            time.sleep(1)
            stage = ["Создание графиков", "Формирование таблиц", "Финализация отчета"][i]
            send_notification(task_name, f"{stage}...", "progress")
        
        result = {
            "report_id": f"RPT-{random.randint(1000, 9999)}",
            "total_records": processed,
            "insights_found": insights,
            "anomalies_detected": anomalies,
            "completion_time": datetime.now().isoformat()
        }
        
        send_notification(task_name, f"Отчет {result['report_id']} успешно создан", "success")
        return result
    except Exception as e:
        send_notification(task_name, str(e), "error", str(e))
        raise

@app.task
def run_data_pipeline(data_size=100):
    """Запускает полный пайплайн обработки данных"""
    # Создаем цепочку задач
    pipeline = chain(
        step1_data_preparation.s(data_size),
        step2_data_processing.s(),
        step3_data_analysis.s(),
        step4_generate_report.s()
    )
    
    # Отправляем уведомление о начале
    send_notification("Пайплайн данных", "Запуск пайплайна обработки данных", "start")
    
    # Запускаем цепочку
    result = pipeline.apply_async()
    return result.id

if __name__ == "__main__":
    app.start()
