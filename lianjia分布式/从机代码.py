import socket
import json
import time
import random
import logging
import requests
from parsel import Selector
import threading
import multiprocessing
import hashlib
import os
from urllib.parse import urlparse, urljoin
import queue

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='worker.log',
    filemode='a'
)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger('').addHandler(console)

# 全局变量
MASTER_HOST = '10.34.85.41'  # 主机IP
MASTER_PORT = 9000  # 主机端口
WORKER_ID = f"worker-{random.randint(1000, 9999)}"  # 工作节点ID
MAX_RETRIES = 3  # 最大重试次数
MAX_TASKS = 5  # 最大并行任务数

# 请求头列表
HEADERS_LIST = [
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2946.89 Safari/537.36'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2945.74 Safari/537.36'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0'
    }
]

# 城市配置
CITY_CONFIG = {
    'sh': "上海",
    'jx': '嘉兴',
    'fs': '佛山',
    'cd': '成都',
    'cq': '重庆'
}


class TaskManager:
    """任务管理器，负责管理工作节点的任务队列"""

    def __init__(self, max_tasks=MAX_TASKS):
        self.task_queue = queue.Queue()  # 任务队列
        self.active_tasks = {}  # 活跃任务，键为任务ID，值为任务信息
        self.completed_tasks = []  # 已完成任务列表
        self.failed_tasks = []  # 失败任务列表
        self.max_tasks = max_tasks  # 最大并行任务数
        self.lock = threading.Lock()  # 线程锁，保护数据结构

    def add_task(self, task):
        """添加任务到队列"""
        self.task_queue.put(task)

    def get_next_task(self):
        """获取下一个任务"""
        if self.get_active_task_count() >= self.max_tasks:
            return None

        try:
            task = self.task_queue.get_nowait()
            with self.lock:
                self.active_tasks[task["task_id"]] = task
            return task
        except queue.Empty:
            return None

    def mark_task_completed(self, task_id, result):
        """标记任务为已完成"""
        with self.lock:
            if task_id in self.active_tasks:
                task = self.active_tasks.pop(task_id)
                task["result"] = result
                task["end_time"] = time.time()
                self.completed_tasks.append(task)

    def mark_task_failed(self, task_id, error):
        """标记任务为失败"""
        with self.lock:
            if task_id in self.active_tasks:
                task = self.active_tasks.pop(task_id)
                task["error"] = error
                task["end_time"] = time.time()
                self.failed_tasks.append(task)

    def get_active_task_count(self):
        """获取活跃任务数"""
        with self.lock:
            return len(self.active_tasks)

    def get_stats(self):
        """获取任务统计信息"""
        with self.lock:
            return {
                "pending_tasks": self.task_queue.qsize(),
                "active_tasks": len(self.active_tasks),
                "completed_tasks": len(self.completed_tasks),
                "failed_tasks": len(self.failed_tasks)
            }


class Worker:
    """工作节点，负责爬取网页数据"""

    def __init__(self, master_host, master_port, worker_id):
        self.master_host = master_host
        self.master_port = master_port
        self.worker_id = worker_id
        self.task_manager = TaskManager()
        self.running = False
        self.socket = None
        self.stats = {
            "start_time": time.time(),
            "tasks_completed": 0,
            "tasks_failed": 0,
            "bytes_downloaded": 0
        }

    def start(self):
        """启动工作节点"""
        self.running = True

        # 创建数据目录
        os.makedirs('../data', exist_ok=True)

        # 连接到主节点
        self._connect_to_master()

        # 注册工作节点
        self._register()

        # 启动心跳线程
        heartbeat_thread = threading.Thread(target=self._heartbeat_thread)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        # 启动任务请求线程
        task_request_thread = threading.Thread(target=self._task_request_thread)
        task_request_thread.daemon = True
        task_request_thread.start()

        # 启动工作线程池
        self._start_worker_processes()

        # 启动结果提交线程
        result_submit_thread = threading.Thread(target=self._result_submit_thread)
        result_submit_thread.daemon = True
        result_submit_thread.start()

        # 主线程继续运行，直到收到中断信号
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("接收到中断信号，正在关闭工作节点...")
        finally:
            self.stop()

    def stop(self):
        """停止工作节点"""
        self.running = False
        if self.socket:
            self.socket.close()
        logging.info("工作节点已关闭")

    def _connect_to_master(self):
        """连接到主节点"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.master_host, self.master_port))
            logging.info(f"已连接到主节点 {self.master_host}:{self.master_port}")
        except Exception as e:
            logging.error(f"连接主节点失败: {e}")
            self.running = False

    def _register(self):
        """向主节点注册"""
        message = {
            "type": "register",
            "worker_id": self.worker_id,
            "capabilities": {
                "max_tasks": MAX_TASKS,
                "supports_javascript": False
            }
        }

        response = self._send_message(message)
        if response and response.get("status") == "ok":
            logging.info("工作节点注册成功")
        else:
            logging.error(f"工作节点注册失败: {response}")
            self.running = False

    def _send_message(self, message):
        """向主节点发送消息并接收响应"""
        try:
            # 发送消息
            message_str = json.dumps(message) + "\n"
            self.socket.sendall(message_str.encode('utf-8'))

            # 接收响应
            data = b""
            while True:
                chunk = self.socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\n" in chunk:
                    break

                    # 解析响应
            if data:
                return json.loads(data.decode('utf-8'))
            return None
        except Exception as e:
            logging.error(f"发送消息出错: {e}")
            # 尝试重新连接
            self._reconnect()
            return None

    def _reconnect(self):
        """重新连接到主节点"""
        try:
            if self.socket:
                self.socket.close()

            time.sleep(5)  # 等待一段时间再重连

            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.master_host, self.master_port))
            logging.info(f"已重新连接到主节点 {self.master_host}:{self.master_port}")

            # 重新注册
            self._register()
        except Exception as e:
            logging.error(f"重新连接主节点失败: {e}")

    def _heartbeat_thread(self):
        """心跳线程，定期向主节点发送心跳消息"""
        while self.running:
            time.sleep(20)  # 每20秒发送一次心跳

            # 获取任务统计信息
            task_stats = self.task_manager.get_stats()

            # 构建心跳消息
            message = {
                "type": "heartbeat",
                "worker_id": self.worker_id,
                "timestamp": time.time(),
                "data": {
                    "status": "active",
                    "tasks_completed": self.stats["tasks_completed"],
                    "tasks_failed": self.stats["tasks_failed"],
                    "bytes_downloaded": self.stats["bytes_downloaded"],
                    "load": task_stats["active_tasks"] / MAX_TASKS if MAX_TASKS > 0 else 0,
                    "memory_usage": os.getloadavg()[0] if hasattr(os, 'getloadavg') else 0
                }
            }

            # 发送心跳消息
            response = self._send_message(message)
            if not response or response.get("status") != "ok":
                logging.warning(f"心跳消息发送失败: {response}")

    def _task_request_thread(self):
        """任务请求线程，定期向主节点请求任务"""
        while self.running:
            # 检查是否需要请求新任务
            if self.task_manager.get_active_task_count() < MAX_TASKS:
                # 请求新任务
                message = {
                    "type": "request_task",
                    "worker_id": self.worker_id
                }

                response = self._send_message(message)
                if response and response.get("status") == "ok" and "task" in response:
                    task = response["task"]
                    logging.info(f"收到新任务: {task['url']}")
                    self.task_manager.add_task(task)
                elif response and response.get("status") == "no_task":
                    logging.debug("主节点没有可用任务")
                    time.sleep(5)  # 等待较长时间再次请求
                else:
                    logging.warning(f"请求任务失败: {response}")

            time.sleep(2)  # 等待一段时间再次请求任务

    def _start_worker_processes(self):
        """启动工作线程池"""
        for _ in range(MAX_TASKS):
            worker_thread = threading.Thread(target=self._worker_thread)
            worker_thread.daemon = True
            worker_thread.start()

    def _worker_thread(self):
        """工作线程，执行爬取任务"""
        while self.running:
            # 获取下一个任务
            task = self.task_manager.get_next_task()
            if not task:
                time.sleep(1)
                continue

                # 执行任务
            try:
                url = task["url"]
                logging.info(f"开始执行任务: {url}")

                # 爬取页面
                html_content = self._fetch_page(url, task.get("proxy"))
                if not html_content:
                    self.task_manager.mark_task_failed(task["task_id"], "获取页面失败")
                    self.stats["tasks_failed"] += 1
                    continue

                    # 解析页面
                data = self._parse_page(url, html_content)

                # 提取新URL
                new_urls = self._extract_new_urls(url, html_content)

                # 标记任务完成
                result = {
                    "url": url,
                    "success": True,
                    "data": data,
                    "new_urls": new_urls
                }
                self.task_manager.mark_task_completed(task["task_id"], result)
                self.stats["tasks_completed"] += 1

                logging.info(f"任务完成: {url}, 获取数据 {len(data)} 条, 新URL {len(new_urls)} 条")

            except Exception as e:
                logging.error(f"执行任务出错: {e}")
                self.task_manager.mark_task_failed(task["task_id"], str(e))
                self.stats["tasks_failed"] += 1

    def _fetch_page(self, url, proxy=None, timeout=30, retries=MAX_RETRIES):
        """获取页面内容"""
        for attempt in range(retries):
            try:
                headers = random.choice(HEADERS_LIST)  # 选择随机请求头
                time.sleep(random.uniform(2, 3))  # 增加随机延时

                # 使用代理（如果有）
                proxies = proxy if proxy else None

                response = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
                response.raise_for_status()

                # 更新统计信息
                self.stats["bytes_downloaded"] += len(response.content)

                return response.text
            except Exception as e:
                logging.warning(f"获取页面失败 (尝试 {attempt + 1}/{retries}): {url}, 错误: {e}")
                time.sleep(2 ** attempt)  # 指数退避

        return None

    def _parse_page(self, url, html_content):
        """解析页面内容"""
        # 判断URL格式，确定解析方式
        if "ershoufang" in url:
            return self._parse_house_list(url, html_content)
        else:
            # 其他页面类型的解析
            return []

    def _parse_house_list(self, url, html_content):
        """解析房屋列表页面"""
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            city_code = domain.split('.')[0]
            city_name = CITY_CONFIG.get(city_code, "未知城市")

            selector = Selector(html_content)
            infos = selector.css('.sellListContent li .info')
            imgs = selector.css('.lj-lazy::attr(data-original)').getall()

            all_data = []
            for index, info in enumerate(infos):
                try:
                    title = info.css('.title a::text').get()
                    house_info = info.css('.address .houseInfo::text').get()

                    # 如果图片索引超出范围，记录警告，并使用默认图片
                    if index < len(imgs):
                        img_url = imgs[index]
                        response = requests.get(img_url)
                        img_na = "".join(str(random.randint(0, 9)) for _ in range(6))
                        img_name = img_na + ".jpg"

                        if response.status_code == 200:
                            # 保存图片
                            with open(os.path.join('../data', img_name), "wb") as f:
                                f.write(response.content)
                        else:
                            logging.warning(f"无法获取图片，状态码：{response.status_code}")
                            img_name = "default.jpg"
                    else:
                        logging.warning(f"第{index}个房源对应的图片不存在")
                        img_name = "default.jpg"

                    house_location = info.css('.flood .positionInfo a::text').get()
                    unitPrice_text = info.css('div.unitPrice span::text').get()

                    # 加入容错处理
                    if unitPrice_text:
                        house_unitPrice = int(unitPrice_text.split('元/平')[0].replace(',', ''))
                    else:
                        house_unitPrice = 0

                    price_text = info.css('div.totalPrice span::text').get()
                    price = float(price_text.replace('万', '')) if price_text else 0.0

                    followers_count = random.randint(5, 140)
                    community_name = info.css('.positionInfo a:first-of-type::text').get()
                    town_name = info.css('.positionInfo a:last-of-type::text').get()

                    all_data.append({
                        '标题': title,
                        '镇/街道': town_name,
                        '小区': community_name,
                        '位置': house_location,
                        '单价（元/平米）': house_unitPrice,
                        '总价（万）': price,
                        '房屋信息': house_info,
                        '网址': url,
                        '图片': 'upload/' + img_name,
                        '关注数': followers_count,
                        '城市': city_name
                    })
                except Exception as e:
                    logging.error(f"解析房屋信息出错: {e}")

            return all_data
        except Exception as e:
            logging.error(f"解析页面出错: {e}")
            return []

    def _extract_new_urls(self, base_url, html_content):
        """从页面中提取新URL"""
        try:
            selector = Selector(html_content)

            # 提取所有链接
            all_links = selector.css('a::attr(href)').getall()

            # 过滤并规范化链接
            new_urls = []
            for link in all_links:
                # 规范化相对URL
                if link.startswith('/'):
                    parsed_url = urlparse(base_url)
                    abs_link = f"{parsed_url.scheme}://{parsed_url.netloc}{link}"
                else:
                    abs_link = urljoin(base_url, link)

                    # 过滤链接（只保留同域名的房源链接）
                if self._should_crawl_url(base_url, abs_link):
                    new_urls.append(abs_link)

            return list(set(new_urls))  # 去重
        except Exception as e:
            logging.error(f"提取新URL出错: {e}")
            return []

    def _should_crawl_url(self, base_url, url):
        """判断URL是否应该爬取"""
        try:
            # 只爬取同域名的URL
            base_domain = urlparse(base_url).netloc
            url_domain = urlparse(url).netloc

            if base_domain != url_domain:
                return False

                # 只爬取房源列表页面
            if "ershoufang" not in url:
                return False

                # 规范化URL，去除无用参数
            url_path = urlparse(url).path

            # 过滤重复页面（如分页链接）
            if "pg" in url and int(url.split("pg")[1].split("/")[0]) > 100:
                return False  # 限制爬取页数

            return True
        except:
            return False

    def _result_submit_thread(self):
        """结果提交线程，定期提交已完成任务的结果"""
        while self.running:
            time.sleep(5)  # 每5秒检查一次是否有结果需要提交

            # 复制已完成任务列表，避免并发修改
            with self.task_manager.lock:
                completed_tasks = self.task_manager.completed_tasks.copy()
                self.task_manager.completed_tasks = []

                failed_tasks = self.task_manager.failed_tasks.copy()
                self.task_manager.failed_tasks = []

                # 提交已完成任务结果
            for task in completed_tasks:
                result = task.get("result", {})

                # 提交结果
                message = {
                    "type": "submit_result",
                    "worker_id": self.worker_id,
                    "task_id": task["task_id"],
                    "data": {
                        "url": result.get("url"),
                        "success": result.get("success", False),
                        "data": result.get("data", [])
                    }
                }

                response = self._send_message(message)
                if response and response.get("status") == "ok":
                    logging.debug(f"结果提交成功: {task['task_id']}")
                else:
                    logging.warning(f"结果提交失败: {task['task_id']}, {response}")

                    # 提交新发现的URL
                new_urls = result.get("new_urls", [])
                if new_urls:
                    message = {
                        "type": "submit_new_urls",
                        "worker_id": self.worker_id,
                        "urls": new_urls
                    }

                    response = self._send_message(message)
                    if response and response.get("status") == "ok":
                        logging.debug(f"新URL提交成功: {len(new_urls)} 条")
                    else:
                        logging.warning(f"新URL提交失败: {response}")

                        # 提交失败任务结果
            for task in failed_tasks:
                message = {
                    "type": "submit_result",
                    "worker_id": self.worker_id,
                    "task_id": task["task_id"],
                    "data": {
                        "url": task.get("url"),
                        "success": False,
                        "error": task.get("error", "未知错误")
                    }
                }

                response = self._send_message(message)
                if not response or response.get("status") != "ok":
                    logging.warning(f"失败结果提交失败: {task['task_id']}, {response}")


def start_worker():
    """启动工作节点"""
    worker = Worker(MASTER_HOST, MASTER_PORT, WORKER_ID)
    worker.start()


if __name__ == "__main__":
    # 启动工作节点
    start_worker()