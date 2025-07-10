from celery import Celery, chain
import os
import time
import random
import requests
import json
from datetime import datetime

broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
backend_url = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

app = Celery('tasks', broker=broker_url, backend=backend_url)

# URL веб-приложения для отправки уведомлений
WEB_APP_URL = os.environ.get("WEB_APP_URL", "http://web:8000")

def send_notification(task_name, result, status="success", error=None, request_input=None):
    """Отправляет уведомление веб-приложению о результате задачи"""
    try:
        data = {
            "task_name": task_name,
            "result": result,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "error": error,
            "request_input": request_input
        }
        requests.post(f"{WEB_APP_URL}/task_result", json=data, timeout=5)
    except Exception as e:
        print(f"Ошибка отправки уведомления: {e}")

def wait_for_user_input(task_id, prompt, input_type="text", options=None):
    """Ждет пользовательского ввода через Redis"""
    import redis
    
    redis_client = redis.Redis.from_url(backend_url)
    input_key = f"user_input:{task_id}"
    
    # Отправляем запрос на ввод
    input_request = {
        "prompt": prompt,
        "input_type": input_type,
        "options": options or [],
        "task_id": task_id
    }
    
    send_notification("Требуется ввод", prompt, "input_required", request_input=input_request)
    
    # Ждем ввода пользователя
    while True:
        user_input = redis_client.get(input_key)
        if user_input:
            # Удаляем ключ после получения
            redis_client.delete(input_key)
            return json.loads(user_input.decode())
        time.sleep(1)

@app.task(bind=True)
def step1_data_preparation(self, data_size=100):
    """Шаг 1: Подготовка данных с пользовательским вводом"""
    task_name = "Подготовка данных"
    task_id = self.request.id
    
    try:
        # Начинаем подготовку
        send_notification(task_name, "Начинаем подготовку данных...", "progress")
        time.sleep(2)
        
        # Запрашиваем у пользователя тип обработки
        processing_type = wait_for_user_input(
            task_id, 
            "Выберите тип обработки данных:",
            "select",
            ["Быстрая обработка", "Детальная обработка", "Экспериментальная обработка"]
        )
        
        send_notification(task_name, f"Выбран тип обработки: {processing_type}", "progress")
        
        # Обрабатываем данные согласно выбору
        processing_time = {"Быстрая обработка": 2, "Детальная обработка": 4, "Экспериментальная обработка": 6}
        duration = processing_time.get(processing_type, 3)
        
        for i in range(duration):
            time.sleep(1)
            progress = ((i + 1) / duration) * 100
            send_notification(task_name, f"Прогресс: {progress:.0f}%", "progress")
        
        result = f"Подготовлено {data_size} записей с типом '{processing_type}'"
        send_notification(task_name, result, "success")
        
        return {
            "data_size": data_size, 
            "processing_type": processing_type,
            "status": "prepared"
        }
        
    except Exception as e:
        send_notification(task_name, str(e), "error", str(e))
        raise

@app.task(bind=True)
def step2_data_processing(self, prev_result):
    """Шаг 2: Обработка данных с настройкой параметров"""
    task_name = "Обработка данных"
    task_id = self.request.id
    
    try:
        data_size = prev_result.get("data_size", 0)
        processing_type = prev_result.get("processing_type", "Быстрая обработка")
        
        send_notification(task_name, f"Начинаем обработку {data_size} записей", "progress")
        time.sleep(1)
        
        # Запрашиваем коэффициент обработки
        quality_factor = wait_for_user_input(
            task_id,
            "Введите коэффициент качества обработки (0.1-1.0):",
            "number"
        )
        
        try:
            quality = float(quality_factor)
            if quality < 0.1 or quality > 1.0:
                quality = 0.8
        except:
            quality = 0.8
            
        send_notification(task_name, f"Установлен коэффициент качества: {quality}", "progress")
        
        # Обработка с учетом коэффициента
        steps = int(4 * quality) + 1
        for i in range(steps):
            time.sleep(1.5)
            progress = ((i + 1) / steps) * 100
            send_notification(task_name, f"Обработано {progress:.0f}% данных", "progress")
        
        # Результат зависит от коэффициента качества
        processed = int(data_size * quality * random.uniform(0.9, 1.0))
        result = f"Обработано {processed} из {data_size} записей (качество: {quality})"
        send_notification(task_name, result, "success")
        
        return {
            "processed": processed, 
            "original": data_size,
            "quality_factor": quality,
            "processing_type": processing_type
        }
        
    except Exception as e:
        send_notification(task_name, str(e), "error", str(e))
        raise

@app.task(bind=True)
def step3_data_analysis(self, prev_result):
    """Шаг 3: Анализ данных с выбором метода"""
    task_name = "Анализ данных"
    task_id = self.request.id
    
    try:
        processed = prev_result.get("processed", 0)
        quality_factor = prev_result.get("quality_factor", 0.8)
        
        send_notification(task_name, f"Начинаем анализ {processed} записей", "progress")
        time.sleep(1)
        
        # Запрашиваем метод анализа
        analysis_method = wait_for_user_input(
            task_id,
            "Выберите метод анализа данных:",
            "select",
            ["Статистический анализ", "Машинное обучение", "Глубокий анализ", "Комбинированный подход"]
        )
        
        send_notification(task_name, f"Выбран метод: {analysis_method}", "progress")
        
        # Если выбран продвинутый метод, запрашиваем дополнительные параметры
        if analysis_method in ["Машинное обучение", "Глубокий анализ"]:
            complexity = wait_for_user_input(
                task_id,
                "Введите уровень сложности анализа (1-10):",
                "number"
            )
            
            try:
                complexity_level = int(complexity)
                if complexity_level < 1 or complexity_level > 10:
                    complexity_level = 5
            except:
                complexity_level = 5
                
            send_notification(task_name, f"Уровень сложности: {complexity_level}", "progress")
        else:
            complexity_level = 3
        
        # Анализ с учетом метода и сложности
        analysis_time = complexity_level // 2 + 2
        for i in range(analysis_time):
            time.sleep(1.5)
            stage = f"Этап {i+1}/{analysis_time} анализа методом '{analysis_method}'"
            send_notification(task_name, stage, "progress")
        
        # Результаты зависят от метода и сложности
        base_insights = int(processed * 0.1)
        insights = base_insights + complexity_level
        anomalies = random.randint(0, max(1, complexity_level // 2))
        
        result = f"Метод '{analysis_method}': найдено {insights} инсайтов и {anomalies} аномалий"
        send_notification(task_name, result, "success")
        
        return {
            "insights": insights,
            "anomalies": anomalies,
            "processed": processed,
            "analysis_method": analysis_method,
            "complexity_level": complexity_level
        }
        
    except Exception as e:
        send_notification(task_name, str(e), "error", str(e))
        raise

@app.task(bind=True)
def step4_generate_report(self, prev_result):
    """Шаг 4: Генерация отчета с настройкой формата"""
    task_name = "Генерация отчета"
    task_id = self.request.id
    
    try:
        insights = prev_result.get("insights", 0)
        anomalies = prev_result.get("anomalies", 0)
        processed = prev_result.get("processed", 0)
        analysis_method = prev_result.get("analysis_method", "Не указан")
        
        send_notification(task_name, "Подготовка к генерации отчета", "progress")
        time.sleep(1)
        
        # Запрашиваем формат отчета
        report_format = wait_for_user_input(
            task_id,
            "Выберите формат отчета:",
            "select",
            ["Краткий отчет", "Детальный отчет", "Презентация", "Технический отчет"]
        )
        
        send_notification(task_name, f"Создаем отчет в формате: {report_format}", "progress")
        
        # Запрашиваем включение графиков
        include_charts = wait_for_user_input(
            task_id,
            "Включить графики и диаграммы в отчет?",
            "select",
            ["Да, включить", "Нет, только текст"]
        )
        
        # Генерация отчета
        generation_steps = 4 if include_charts == "Да, включить" else 3
        steps = ["Создание структуры", "Формирование данных", "Создание графиков", "Финализация"]
        
        for i in range(generation_steps):
            time.sleep(1.2)
            send_notification(task_name, f"{steps[i]}...", "progress")
        
        # Финальный результат
        report_id = f"RPT-{random.randint(1000, 9999)}"
        report_details = {
            "report_id": report_id,
            "format": report_format,
            "includes_charts": include_charts == "Да, включить",
            "total_records": processed,
            "insights_found": insights,
            "anomalies_detected": anomalies,
            "analysis_method": analysis_method,
            "completion_time": datetime.now().isoformat()
        }
        
        result_text = f"Отчет {report_id} создан в формате '{report_format}'"
        if include_charts == "Да, включить":
            result_text += " с графиками"
        
        send_notification(task_name, result_text, "success")
        
        return report_details
        
    except Exception as e:
        send_notification(task_name, str(e), "error", str(e))
        raise

@app.task
def run_interactive_pipeline(data_size=100):
    """Запускает интерактивный пайплайн обработки данных"""
    # Создаем цепочку задач
    pipeline = chain(
        step1_data_preparation.s(data_size),
        step2_data_processing.s(),
        step3_data_analysis.s(),
        step4_generate_report.s()
    )
    
    # Отправляем уведомление о начале
    send_notification("Интерактивный пайплайн", "Запуск интерактивного пайплайна", "start")
    
    # Запускаем цепочку
    result = pipeline.apply_async()
    return result.id

if __name__ == "__main__":
    app.start()
