# advanced_scheduler.py
import schedule
import time
import threading
import datetime
import functools
import inspect
from typing import Callable, List, Dict, Any, Optional, Union
from enum import Enum
import re

class ScheduleType(Enum):
    """定时任务类型"""
    DAILY = "daily"           # 每天固定时间
    WEEKLY = "weekly"         # 每周特定天
    MONTHLY = "monthly"       # 每月特定天
    INTERVAL = "interval"     # 间隔时间
    CRON = "cron"            # Cron表达式
    BUSINESS_DAY = "business_day"  # 工作日
    WEEKEND = "weekend"      # 周末
    HOURLY = "hourly"        # 每小时

class TaskPriority(Enum):
    """任务优先级"""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4

class AdvancedScheduler:
    """
    多功能定时任务调度器
    支持多种定时类型：每天、每周、每月、间隔、Cron表达式等
    """
    
    _instance = None
    _running = False
    _thread = None
    _tasks_registry = {}  # 任务注册表
    _lock = threading.Lock()
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.tasks = []
            self._job_count = 0
            self._task_history = []
            self._max_history = 100
            self._initialized = True
            print("🚀 高级定时任务调度器初始化完成")
    
    # ============== 装饰器方法 ==============
    
    @classmethod
    def daily(cls, time_str: str, description: str = None):
        """每天固定时间执行"""
        def decorator(func: Callable) -> Callable:
            cls._add_task(func, ScheduleType.DAILY, time=time_str, description=description)
            return func
        return decorator
    
    @classmethod
    def weekly(cls, day_of_week: Union[str, int], time_str: str, description: str = None):
        """
        每周特定天执行
        day_of_week: 0-6（0=周一） 或 'monday', 'tuesday'等
        """
        def decorator(func: Callable) -> Callable:
            cls._add_task(func, ScheduleType.WEEKLY, day=day_of_week, time=time_str, description=description)
            return func
        return decorator
    
    @classmethod
    def monthly(cls, day_of_month: int, time_str: str, description: str = None):
        """每月特定天执行"""
        def decorator(func: Callable) -> Callable:
            cls._add_task(func, ScheduleType.MONTHLY, day=day_of_month, time=time_str, description=description)
            return func
        return decorator
    
    @classmethod
    def interval(cls, **kwargs):
        """
        间隔时间执行
        支持: seconds, minutes, hours, days, weeks
        """
        def decorator(func: Callable) -> Callable:
            cls._add_task(func, ScheduleType.INTERVAL, **kwargs, description=func.__name__)
            return func
        return decorator
    
    @classmethod
    def cron(cls, cron_expression: str, description: str = None):
        """Cron表达式执行"""
        def decorator(func: Callable) -> Callable:
            cls._add_task(func, ScheduleType.CRON, cron=cron_expression, description=description)
            return func
        return decorator
    
    @classmethod
    def business_day(cls, time_str: str, description: str = None):
        """工作日执行（周一到周五）"""
        def decorator(func: Callable) -> Callable:
            cls._add_task(func, ScheduleType.BUSINESS_DAY, time=time_str, description=description)
            return func
        return decorator
    
    @classmethod
    def weekend(cls, time_str: str, description: str = None):
        """周末执行（周六、周日）"""
        def decorator(func: Callable) -> Callable:
            cls._add_task(func, ScheduleType.WEEKEND, time=time_str, description=description)
            return func
        return decorator
    
    @classmethod
    def hourly(cls, minute: int = 0, description: str = None):
        """每小时执行（在指定分钟）"""
        def decorator(func: Callable) -> Callable:
            cls._add_task(func, ScheduleType.HOURLY, minute=minute, description=description)
            return func
        return decorator
    
    # ============== 手动添加任务方法 ==============
    
    @classmethod
    def add_task(cls, func: Callable, schedule_type: ScheduleType, 
                 priority: TaskPriority = TaskPriority.MEDIUM, **kwargs):
        """手动添加任务"""
        return cls._add_task(func, schedule_type, priority=priority, **kwargs)
    
    @classmethod
    def _add_task(cls, func: Callable, schedule_type: ScheduleType, **kwargs):
        """内部方法：添加任务到调度器"""
        with cls._lock:
            task_id = f"{func.__name__}_{cls._instance._job_count}"
            cls._instance._job_count += 1
            
            # 创建任务包装器
            wrapped_func = cls._wrap_task(func, task_id, kwargs.get('description'))
            
            # 根据调度类型安排任务
            job = cls._schedule_job(wrapped_func, schedule_type, **kwargs)
            
            if not job:
                print(f"❌ 任务安排失败: {kwargs.get('description', func.__name__)}")
                return func
            
            # 保存任务信息
            task_info = {
                'id': task_id,
                'func': func,
                'wrapped_func': wrapped_func,
                'job': job,
                'schedule_type': schedule_type,
                'description': kwargs.get('description', func.__name__),
                'priority': kwargs.get('priority', TaskPriority.MEDIUM),
                'kwargs': kwargs,
                'created_at': datetime.datetime.now(),
                'last_run': None,
                'next_run': job.next_run,
                'run_count': 0,
                'error_count': 0
            }
            
            cls._instance.tasks.append(task_info)
            cls._tasks_registry[task_id] = task_info
            
            print(f"✅ 注册任务: {task_info['description']} [{schedule_type.value}]")
            return func
    
    @classmethod
    def _schedule_job(cls, func: Callable, schedule_type: ScheduleType, **kwargs):
        """根据类型安排任务"""
        try:
            if schedule_type == ScheduleType.DAILY:
                return schedule.every().day.at(kwargs['time']).do(func)
            
            elif schedule_type == ScheduleType.WEEKLY:
                day = kwargs['day']
                if isinstance(day, str):
                    # 字符串转数字
                    day_map = {
                        'monday': 0, 'tuesday': 1, 'wednesday': 2,
                        'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6
                    }
                    day = day_map.get(day.lower(), day)
                
                weekly_scheduler = schedule.every()
                if day == 0:
                    weekly_scheduler.monday
                elif day == 1:
                    weekly_scheduler.tuesday
                elif day == 2:
                    weekly_scheduler.wednesday
                elif day == 3:
                    weekly_scheduler.thursday
                elif day == 4:
                    weekly_scheduler.friday
                elif day == 5:
                    weekly_scheduler.saturday
                elif day == 6:
                    weekly_scheduler.sunday
                
                return weekly_scheduler.at(kwargs['time']).do(func)
            
            elif schedule_type == ScheduleType.MONTHLY:
                # schedule库不支持每月，使用cron表达式模拟
                day = kwargs['day']
                time_str = kwargs['time']
                hour, minute = map(int, time_str.split(':'))
                cron_expr = f"{minute} {hour} {day} * *"
                return schedule.every().cron(cron_expr).do(func)
            
            elif schedule_type == ScheduleType.INTERVAL:
                scheduler = schedule.every()
                if 'seconds' in kwargs:
                    scheduler = scheduler.seconds
                elif 'minutes' in kwargs:
                    scheduler = scheduler.minutes
                elif 'hours' in kwargs:
                    scheduler = scheduler.hours
                elif 'days' in kwargs:
                    scheduler = scheduler.days
                elif 'weeks' in kwargs:
                    scheduler = scheduler.weeks
                
                interval = kwargs.get('seconds') or kwargs.get('minutes') or \
                          kwargs.get('hours') or kwargs.get('days') or kwargs.get('weeks') or 1
                
                return scheduler.do(func)
            
            elif schedule_type == ScheduleType.CRON:
                return schedule.every().cron(kwargs['cron']).do(func)
            
            elif schedule_type == ScheduleType.BUSINESS_DAY:
                # 周一到周五执行
                def business_day_wrapper():
                    if datetime.datetime.now().weekday() < 5:  # 0-4是周一到周五
                        return func()
                return schedule.every().day.at(kwargs['time']).do(business_day_wrapper)
            
            elif schedule_type == ScheduleType.WEEKEND:
                # 周六周日执行
                def weekend_wrapper():
                    if datetime.datetime.now().weekday() >= 5:  # 5-6是周六周日
                        return func()
                return schedule.every().day.at(kwargs['time']).do(weekend_wrapper)
            
            elif schedule_type == ScheduleType.HOURLY:
                minute = kwargs.get('minute', 0)
                time_str = f"00:{minute:02d}" if minute < 10 else f"00:{minute}"
                return schedule.every().hour.at(time_str).do(func)
            
        except Exception as e:
            print(f"安排任务失败: {e}")
            return None
    
    @staticmethod
    def _wrap_task(func: Callable, task_id: str, description: str = None) -> Callable:
        """包装任务函数，添加日志和异常处理"""
        task_name = description or func.__name__
        
        @functools.wraps(func)
        def wrapper():
            start_time = datetime.datetime.now()
            task_info = AdvancedScheduler._instance._tasks_registry.get(task_id)
            
            print(f"🔄 [{start_time.strftime('%H:%M:%S')}] 开始执行: {task_name}")
            
            try:
                result = func()
                end_time = datetime.datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # 更新任务信息
                if task_info:
                    task_info['last_run'] = end_time
                    task_info['next_run'] = task_info['job'].next_run
                    task_info['run_count'] += 1
                    task_info['last_duration'] = duration
                    task_info['last_result'] = result
                
                # 记录历史
                AdvancedScheduler._instance._record_history(
                    task_id, task_name, start_time, end_time, 
                    "success", result, duration
                )
                
                print(f"✅ [{end_time.strftime('%H:%M:%S')}] 任务完成: {task_name} (耗时: {duration:.2f}s)")
                return result
                
            except Exception as e:
                end_time = datetime.datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # 更新任务信息
                if task_info:
                    task_info['last_run'] = end_time
                    task_info['error_count'] += 1
                    task_info['last_error'] = str(e)
                
                # 记录历史
                AdvancedScheduler._instance._record_history(
                    task_id, task_name, start_time, end_time, 
                    "error", str(e), duration
                )
                
                print(f"❌ [{end_time.strftime('%H:%M:%S')}] 任务失败: {task_name} - {e}")
                raise
        
        wrapper.task_id = task_id
        return wrapper
    
    def _record_history(self, task_id: str, task_name: str, start_time: datetime.datetime,
                       end_time: datetime.datetime, status: str, result: Any, duration: float):
        """记录任务执行历史"""
        history_entry = {
            'task_id': task_id,
            'task_name': task_name,
            'start_time': start_time,
            'end_time': end_time,
            'status': status,
            'result': result,
            'duration': duration
        }
        
        self._task_history.append(history_entry)
        
        # 限制历史记录数量
        if len(self._task_history) > self._max_history:
            self._task_history.pop(0)
    
    # ============== 管理方法 ==============
    
    @classmethod
    def list_tasks(cls, filter_type: ScheduleType = None):
        """列出所有任务"""
        if not cls._instance.tasks:
            print("📝 暂无注册任务")
            return
        
        print("\n" + "="*80)
        print("📋 任务列表:")
        print("="*80)
        
        filtered_tasks = cls._instance.tasks
        if filter_type:
            filtered_tasks = [t for t in cls._instance.tasks if t['schedule_type'] == filter_type]
            print(f"过滤类型: {filter_type.value}")
        
        for i, task in enumerate(filtered_tasks, 1):
            print(f"{i}. {task['description']}")
            print(f"   类型: {task['schedule_type'].value}")
            print(f"   优先级: {task['priority'].name}")
            print(f"   任务ID: {task['id']}")
            print(f"   创建时间: {task['created_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            if task['last_run']:
                print(f"   上次执行: {task['last_run'].strftime('%Y-%m-%d %H:%M:%S')}")
                if 'last_duration' in task:
                    print(f"   执行耗时: {task['last_duration']:.2f}s")
            
            if task['next_run']:
                print(f"   下次执行: {task['next_run'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            print(f"   执行次数: {task['run_count']}")
            if task['error_count'] > 0:
                print(f"   错误次数: {task['error_count']}")
            
            print()
    
    @classmethod
    def get_task_history(cls, task_id: str = None, limit: int = 10):
        """获取任务执行历史"""
        if not cls._instance._task_history:
            print("📊 暂无执行历史")
            return
        
        print("\n" + "="*80)
        print("📊 任务执行历史:")
        print("="*80)
        
        history = cls._instance._task_history
        if task_id:
            history = [h for h in history if h['task_id'] == task_id]
        
        history = history[-limit:]  # 获取最近的记录
        
        for i, entry in enumerate(reversed(history), 1):
            print(f"{i}. {entry['task_name']}")
            print(f"   时间: {entry['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   状态: {entry['status'].upper()}")
            print(f"   耗时: {entry['duration']:.2f}s")
            
            if entry['status'] == 'error':
                print(f"   错误: {entry['result']}")
            
            print()
    
    @classmethod
    def pause_task(cls, task_id: str):
        """暂停任务"""
        task = cls._tasks_registry.get(task_id)
        if task and task['job']:
            schedule.cancel_job(task['job'])
            task['paused'] = True
            print(f"⏸️  任务已暂停: {task['description']}")
    
    @classmethod
    def resume_task(cls, task_id: str):
        """恢复任务"""
        task = cls._tasks_registry.get(task_id)
        if task and task.get('paused'):
            # 重新安排任务
            job = cls._schedule_job(task['wrapped_func'], task['schedule_type'], **task['kwargs'])
            if job:
                task['job'] = job
                task['paused'] = False
                task['next_run'] = job.next_run
                print(f"▶️  任务已恢复: {task['description']}")
    
    @classmethod
    def remove_task(cls, task_id: str):
        """移除任务"""
        with cls._lock:
            task = cls._tasks_registry.get(task_id)
            if task:
                if task['job']:
                    schedule.cancel_job(task['job'])
                
                cls._instance.tasks = [t for t in cls._instance.tasks if t['id'] != task_id]
                cls._tasks_registry.pop(task_id, None)
                print(f"🗑️  任务已移除: {task['description']}")
    
    @classmethod
    def run_task_now(cls, task_id: str):
        """立即执行任务"""
        task = cls._tasks_registry.get(task_id)
        if task:
            print(f"⚡ 立即执行任务: {task['description']}")
            task['wrapped_func']()
    
    @classmethod
    def _run_scheduler(cls):
        """运行调度器的后台线程"""
        print("🚀 定时任务调度器启动")
        print(f"⏰ 当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        while cls._running:
            try:
                schedule.run_pending()
                
                # 更新所有任务的下次执行时间
                for task in cls._instance.tasks:
                    if task.get('job') and not task.get('paused'):
                        task['next_run'] = task['job'].next_run
                
                time.sleep(1)
            except Exception as e:
                print(f"调度器错误: {e}")
                time.sleep(10)
    
    @classmethod
    def start(cls):
        """启动调度器"""
        if not cls._running:
            cls._running = True
            cls._thread = threading.Thread(
                target=cls._run_scheduler,
                daemon=True,
                name="AdvancedScheduler"
            )
            cls._thread.start()
            print("🎯 调度器已启动")
    
    @classmethod
    def stop(cls):
        """停止调度器"""
        cls._running = False
        if cls._thread:
            cls._thread.join(timeout=10)
        print("🛑 调度器已停止")

# 创建单例实例并自动启动
_scheduler = AdvancedScheduler()
AdvancedScheduler.start()