import socket
import json
import threading
import queue
import time
import random
import logging
import pymysql
import urllib.robotparser
import os
from datetime import datetime
import hashlib
import multiprocessing
from urllib.parse import urlparse

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='master.log',
    filemode='a'
)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger('').addHandler(console)

# 全局变量
MASTER_HOST = '10.34.85.41'
MASTER_PORT = 9000
MAX_WORKERS = 5
DISTRIBUTION_STRATEGY = "ROUND_ROBIN"  # 可选: "ROUND_ROBIN", "LOAD_BALANCE"

# 数据库配置
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '123456',
    'database': 'lianjia'
}

# 城市配置
CITY_CONFIG = {
    'sh': "上海",
    'jx': '嘉兴',
    'fs': '佛山',
    'cd': '成都',
    'cq': '重庆'
}


class URLManager:
    """URL管理器，负责管理种子URL和待爬取URL"""

    def __init__(self):
        self.seed_urls = set()  # 种子URL集合
        self.pending_urls = queue.Queue()  # 待爬取URL队列
        self.crawled_urls = set()  # 已爬取URL集合
        self.failed_urls = set()  # 爬取失败的URL集合
        self.url_filters = []  # URL过滤规则
        self.robots_parsers = {}  # robots.txt解析器，以域名为键
        self.lock = threading.Lock()  # 线程锁，保护数据结构

    def add_seed_url(self, url):
        """添加种子URL"""
        with self.lock:
            if self._check_robots(url) and self._filter_url(url):
                self.seed_urls.add(url)
                self.pending_urls.put(url)
                logging.info(f"添加种子URL: {url}")

    def add_pending_url(self, url):
        """添加待爬取URL"""
        with self.lock:
            if url not in self.crawled_urls and url not in self.failed_urls:
                if self._check_robots(url) and self._filter_url(url):
                    self.pending_urls.put(url)
                    logging.debug(f"添加待爬取URL: {url}")
                    return True
            return False

    def get_pending_url(self):
        """获取一个待爬取URL"""
        try:
            url = self.pending_urls.get_nowait()
            return url
        except queue.Empty:
            return None

    def mark_as_crawled(self, url):
        """标记URL为已爬取"""
        with self.lock:
            self.crawled_urls.add(url)

    def mark_as_failed(self, url):
        """标记URL为爬取失败"""
        with self.lock:
            self.failed_urls.add(url)

    def has_pending_urls(self):
        """检查是否有待爬取URL"""
        return not self.pending_urls.empty()

    def add_url_filter(self, filter_func):
        """添加URL过滤规则"""
        self.url_filters.append(filter_func)

    def _filter_url(self, url):
        """过滤URL，返回True表示URL符合过滤规则"""
        for filter_func in self.url_filters:
            if not filter_func(url):
                return False
        return True

    def _check_robots(self, url):
        """检查URL是否符合robots.txt规则"""
        try:
            parsed_url = urlparse(url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"

            # 如果robots解析器不存在，则创建一个
            if domain not in self.robots_parsers:
                rp = urllib.robotparser.RobotFileParser()
                rp.set_url(f"{domain}/robots.txt")
                rp.read()
                self.robots_parsers[domain] = rp

                # 检查URL是否被允许爬取
            return self.robots_parsers[domain].can_fetch("*", url)
        except Exception as e:
            logging.error(f"检查robots.txt时出错: {e}")
            return True  # 出错时假设允许爬取

    def get_stats(self):
        """获取URL统计信息"""
        return {
            "seed_urls": len(self.seed_urls),
            "pending_urls": self.pending_urls.qsize(),
            "crawled_urls": len(self.crawled_urls),
            "failed_urls": len(self.failed_urls)
        }


class WorkerManager:
    """工作节点管理器，负责管理所有工作节点的状态"""

    def __init__(self):
        self.workers = {}  # 工作节点字典，键为地址，值为状态信息
        self.last_assigned = -1  # 轮询算法的指针
        self.lock = threading.Lock()  # 线程锁，保护数据结构

    def register_worker(self, addr, info=None):
        """注册工作节点"""
        with self.lock:
            if info is None:
                info = {
                    "status": "idle",
                    "tasks_completed": 0,
                    "tasks_failed": 0,
                    "load": 0,
                    "last_heartbeat": time.time()
                }
            self.workers[addr] = info
            logging.info(f"工作节点注册: {addr}")

    def update_worker(self, addr, info):
        """更新工作节点状态"""
        with self.lock:
            if addr in self.workers:
                self.workers[addr].update(info)
                self.workers[addr]["last_heartbeat"] = time.time()
            else:
                self.register_worker(addr, info)

    def get_worker_for_task(self, strategy="ROUND_ROBIN"):
        """根据策略选择一个工作节点处理任务"""
        with self.lock:
            if not self.workers:
                return None

            active_workers = {addr: info for addr, info in self.workers.items()
                              if time.time() - info["last_heartbeat"] < 30}

            if not active_workers:
                return None

            worker_addrs = list(active_workers.keys())

            if strategy == "ROUND_ROBIN":
                # 轮询策略
                self.last_assigned = (self.last_assigned + 1) % len(worker_addrs)
                return worker_addrs[self.last_assigned]

            elif strategy == "LOAD_BALANCE":
                # 负载均衡策略
                return min(active_workers.items(), key=lambda x: x[1]["load"])[0]

            else:
                # 默认轮询
                self.last_assigned = (self.last_assigned + 1) % len(worker_addrs)
                return worker_addrs[self.last_assigned]

    def check_workers_health(self):
        """检查工作节点健康状态，移除不活跃的节点"""
        with self.lock:
            current_time = time.time()
            inactive_workers = [addr for addr, info in self.workers.items()
                                if current_time - info["last_heartbeat"] > 60]

            for addr in inactive_workers:
                logging.warning(f"移除不活跃的工作节点: {addr}")
                del self.workers[addr]

    def get_stats(self):
        """获取工作节点统计信息"""
        with self.lock:
            total_completed = sum(info["tasks_completed"] for info in self.workers.values())
            total_failed = sum(info["tasks_failed"] for info in self.workers.values())

            return {
                "active_workers": len(self.workers),
                "total_completed": total_completed,
                "total_failed": total_failed,
                "workers": {addr: {k: v for k, v in info.items() if k != "last_heartbeat"}
                            for addr, info in self.workers.items()}
            }


class DataStorage:
    """数据存储模块，负责存储爬取的数据"""

    def __init__(self, db_config):
        self.db_config = db_config
        self.lock = threading.Lock()
        self.create_tables_if_not_exist()

    def create_tables_if_not_exist(self):
        """创建数据库表（如果不存在）"""
        try:
            connection = pymysql.connect(**self.db_config)
            cursor = connection.cursor()

            # 创建URL表
            cursor.execute("""  
            CREATE TABLE IF NOT EXISTS crawl_urls (  
                id INT AUTO_INCREMENT PRIMARY KEY,  
                url VARCHAR(255) UNIQUE,  
                status ENUM('pending', 'crawled', 'failed') DEFAULT 'pending',  
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  
            )  
            """)

            # 创建索引表（仅示例，实际应根据需求设计）
            cursor.execute("""  
            CREATE TABLE IF NOT EXISTS url_index (  
                id INT AUTO_INCREMENT PRIMARY KEY,  
                url_id INT,  
                keyword VARCHAR(50),  
                weight FLOAT,  
                FOREIGN KEY (url_id) REFERENCES crawl_urls(id)  
            )  
            """)

            connection.commit()
            cursor.close()
            connection.close()
            logging.info("数据库表已创建或已存在")
        except Exception as e:
            logging.error(f"创建数据库表时出错: {e}")

    def store_house_data(self, data):
        """存储房屋数据"""
        with self.lock:
            try:
                connection = pymysql.connect(**self.db_config)
                cursor = connection.cursor()

                for house in data:
                    fangwuleixing = ['普通商品房', '经济适用房', '小产权房', '房改房', '廉租房', '公租房']
                    leixing = random.choice(fangwuleixing)
                    bianhao = "".join(str(random.randint(0, 9)) for _ in range(10))
                    current_time = datetime.now()
                    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')

                    datadetail = house['房屋信息'].split('|') if house['房屋信息'] else ["", "", "", ""]

                    zulinfangshi = ['整租', '合租', '押一付三', '包租', '短租']
                    fangshi = random.choice(zulinfangshi)

                    fangzhu = {'0011': ["李星云", '13823888881'], '0012': ["萧炎", '13823888882'],
                               '0013': ["陈平安", '13823888883']}
                    fangzhuzhanghao = ['0011', '0012', '0013']
                    zhanghao = random.choice(fangzhuzhanghao)

                    # 确保datadetail长度足够
                    if len(datadetail) < 4:
                        datadetail += [""] * (4 - len(datadetail))

                    query = """  
                    INSERT INTO fangwuxinxi (addtime, biaoti, fangwubianhao, fangwuleixing, huxing, zulinfangshi,   
                    yuyuekanfang, chengshi, dizhi, tupian, mianji, zujin, fangyuansheshi, hexinmaidian, faburiqi,   
                    fangyuanxiangqing, fangzhuzhanghao, fangzhuxingming, lianxidianhua, sfsh, shhf, clicktime, clicknum)  
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)  
                    """

                    values = (
                        formatted_time,
                        house['标题'],
                        bianhao,
                        leixing,
                        datadetail[0],
                        fangshi,
                        "接受",
                        house.get('城市', "未知城市"),
                        house['位置'] + " " + house['镇/街道'] + " " + house['小区'],
                        house['图片'],
                        datadetail[1],
                        house['总价（万）'],
                        datadetail[3],
                        house['标题'],
                        formatted_time,
                        house['房屋信息'] + " " + house['位置'] + " " + house['镇/街道'],
                        zhanghao,
                        fangzhu[zhanghao][0],
                        fangzhu[zhanghao][1],
                        '是',
                        '爬虫所得',
                        formatted_time,
                        house['关注数']
                    )

                    cursor.execute(query, values)

                connection.commit()
                cursor.close()
                connection.close()
                logging.info(f"成功存储 {len(data)} 条房屋数据")
                return True
            except Exception as e:
                logging.error(f"存储房屋数据时出错: {e}")
                return False

    def store_url(self, url, status="pending"):
        """存储URL记录"""
        with self.lock:
            try:
                connection = pymysql.connect(**self.db_config)
                cursor = connection.cursor()

                # 使用ON DUPLICATE KEY UPDATE语法来处理重复URL
                query = """  
                INSERT INTO crawl_urls (url, status) VALUES (%s, %s)  
                ON DUPLICATE KEY UPDATE status = %s, updated_at = CURRENT_TIMESTAMP  
                """

                cursor.execute(query, (url, status, status))
                connection.commit()
                cursor.close()
                connection.close()
                return True
            except Exception as e:
                logging.error(f"存储URL记录时出错: {e}")
                return False

    def update_url_status(self, url, status):
        """更新URL状态"""
        with self.lock:
            try:
                connection = pymysql.connect(**self.db_config)
                cursor = connection.cursor()

                query = """  
                UPDATE crawl_urls SET status = %s WHERE url = %s  
                """

                cursor.execute(query, (status, url))
                connection.commit()
                cursor.close()
                connection.close()
                return True
            except Exception as e:
                logging.error(f"更新URL状态时出错: {e}")
                return False

    def search(self, keyword, limit=10):
        """根据关键词搜索房屋数据"""
        try:
            connection = pymysql.connect(**self.db_config)
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            query = """  
            SELECT * FROM fangwuxinxi   
            WHERE biaoti LIKE %s OR fangyuanxiangqing LIKE %s OR dizhi LIKE %s  
            LIMIT %s  
            """

            keyword_pattern = f"%{keyword}%"
            cursor.execute(query, (keyword_pattern, keyword_pattern, keyword_pattern, limit))
            results = cursor.fetchall()

            cursor.close()
            connection.close()
            return results
        except Exception as e:
            logging.error(f"搜索数据时出错: {e}")
            return []


class DNSResolver:
    """DNS解析模块，负责将域名解析为IP地址"""

    def __init__(self):
        self.cache = {}  # DNS缓存，键为域名，值为(IP地址, 过期时间)
        self.lock = threading.Lock()  # 线程锁，保护缓存

    def resolve(self, url):
        """解析URL对应的IP地址"""
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc

            with self.lock:
                # 检查缓存
                if domain in self.cache:
                    ip, expire_time = self.cache[domain]
                    if time.time() < expire_time:
                        return ip

                        # 缓存未命中或已过期，进行DNS解析
            addr_info = socket.getaddrinfo(domain, 80)
            ip = addr_info[0][4][0]

            # 缓存结果，设置TTL为1小时
            with self.lock:
                self.cache[domain] = (ip, time.time() + 3600)

            return ip
        except Exception as e:
            logging.error(f"DNS解析出错: {e}")
            return None


class ProxyPool:
    """IP代理池，提供随机代理IP"""

    def __init__(self):
        self.proxies = []  # 代理列表
        self.lock = threading.Lock()  # 线程锁，保护代理列表
        self._init_proxies()  # 初始化代理列表

    def _init_proxies(self):
        """初始化代理列表（示例代理，实际应从代理服务获取）"""
        # 这里只是示例，实际应用中应从代理服务获取真实代理
        sample_proxies = [
            None,
            #{'http': 'http://127.0.0.1:8080', 'https': 'https://127.0.0.1:8080'},
            #{'http': 'http://127.0.0.1:8081', 'https': 'https://127.0.0.1:8081'},
            #{'http': 'http://127.0.0.1:8082', 'https': 'https://127.0.0.1:8082'}
        ]

        with self.lock:
            self.proxies = sample_proxies

    def get_random_proxy(self):
        """获取随机代理"""
        with self.lock:
            if not self.proxies:
                return None
            return random.choice(self.proxies)

    def report_proxy_status(self, proxy, success):
        """报告代理使用状态，成功或失败"""
        # 实际应用中，可以根据成功/失败次数调整代理权重或移除失效代理
        pass


class Master:
    """主节点，负责管理URL和分发任务"""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.url_manager = URLManager()
        self.worker_manager = WorkerManager()
        self.data_storage = DataStorage(DB_CONFIG)
        self.dns_resolver = DNSResolver()
        self.proxy_pool = ProxyPool()
        self.server_socket = None
        self.running = False
        self.stats = {
            "start_time": time.time(),
            "total_crawled": 0,
            "total_failed": 0,
            "urls_per_second": 0
        }

    def start(self):
        """启动主节点"""
        self.running = True

        # 初始化种子URL
        self._init_seed_urls()

        # 启动服务器套接字
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)

        logging.info(f"主节点启动，监听 {self.host}:{self.port}")

        # 启动监控线程
        monitor_thread = threading.Thread(target=self._monitor_thread)
        monitor_thread.daemon = True
        monitor_thread.start()

        # 启动任务分发线程
        dispatcher_thread = threading.Thread(target=self._dispatcher_thread)
        dispatcher_thread.daemon = True
        dispatcher_thread.start()

        # 接受连接
        try:
            while self.running:
                client_socket, client_addr = self.server_socket.accept()
                client_thread = threading.Thread(target=self._handle_client, args=(client_socket, client_addr))
                client_thread.daemon = True
                client_thread.start()
        except KeyboardInterrupt:
            logging.info("接收到中断信号，正在关闭主节点...")
        finally:
            self.stop()

    def stop(self):
        """停止主节点"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        logging.info("主节点已关闭")

    def _init_seed_urls(self):
        """初始化种子URL"""
        # 添加链家网各城市的种子URL
        cities = ['sh', 'jx', 'fs', 'cd', 'cq']
        for city in cities:
            for page in range(1, 3):  # 每个城市爬取2页，可以根据需要调整
                url = f'https://{city}.lianjia.com/ershoufang/pg{page}'
                self.url_manager.add_seed_url(url)

    def _handle_client(self, client_socket, client_addr):
        """处理客户端连接"""
        addr_str = f"{client_addr[0]}:{client_addr[1]}"
        try:
            while self.running:
                # 接收消息
                data = b""
                while True:
                    chunk = client_socket.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                    if b"\n" in chunk:
                        break

                if not data:
                    break

                    # 解析消息
                try:
                    message = json.loads(data.decode('utf-8'))
                    message_type = message.get("type")

                    if message_type == "register":
                        # 工作节点注册
                        self.worker_manager.register_worker(addr_str)
                        response = {"status": "ok", "message": "已注册"}

                    elif message_type == "heartbeat":
                        # 工作节点心跳
                        self.worker_manager.update_worker(addr_str, message.get("data", {}))
                        response = {"status": "ok"}

                    elif message_type == "request_task":
                        # 工作节点请求任务
                        task = self._get_task()
                        if task:
                            response = {"status": "ok", "task": task}
                        else:
                            response = {"status": "no_task"}

                    elif message_type == "submit_result":
                        # 工作节点提交结果
                        result = message.get("data", {})
                        self._handle_result(result)
                        response = {"status": "ok"}

                    elif message_type == "submit_new_urls":
                        # 工作节点提交新发现的URL
                        new_urls = message.get("urls", [])
                        for url in new_urls:
                            self.url_manager.add_pending_url(url)
                        response = {"status": "ok", "accepted": len(new_urls)}

                    else:
                        # 未知消息类型
                        response = {"status": "error", "message": "未知消息类型"}

                        # 发送响应
                    client_socket.sendall(json.dumps(response).encode('utf-8') + b"\n")

                except json.JSONDecodeError:
                    logging.error(f"JSON解析错误: {data.decode('utf-8', 'ignore')}")
                    client_socket.sendall(
                        json.dumps({"status": "error", "message": "JSON解析错误"}).encode('utf-8') + b"\n")

                except Exception as e:
                    logging.error(f"处理客户端消息时出错: {e}")
                    client_socket.sendall(json.dumps({"status": "error", "message": str(e)}).encode('utf-8') + b"\n")

        except Exception as e:
            logging.error(f"客户端连接处理出错: {e}")

        finally:
            client_socket.close()
            logging.info(f"客户端 {addr_str} 已断开连接")

    def _get_task(self):
        """获取一个任务分配给工作节点"""
        url = self.url_manager.get_pending_url()
        if not url:
            return None

            # 尝试解析URL对应的IP
        ip = self.dns_resolver.resolve(url)

        # 获取一个随机代理
        proxy = self.proxy_pool.get_random_proxy()

        # 构建任务
        task = {
            "url": url,
            "ip": ip,
            "proxy": proxy,
            "task_id": hashlib.md5(url.encode()).hexdigest(),
            "timestamp": time.time()
        }

        return task

    def _handle_result(self, result):
        """处理工作节点提交的结果"""
        url = result.get("url")
        success = result.get("success", False)

        if success:
            # 更新URL状态
            self.url_manager.mark_as_crawled(url)
            self.data_storage.update_url_status(url, "crawled")

            # 存储爬取数据
            data = result.get("data", [])
            if data:
                self.data_storage.store_house_data(data)

                # 更新统计信息
            self.stats["total_crawled"] += 1
        else:
            # 更新URL状态
            self.url_manager.mark_as_failed(url)
            self.data_storage.update_url_status(url, "failed")

            # 记录失败原因
            error = result.get("error", "未知错误")
            logging.warning(f"爬取失败: {url}, 原因: {error}")

            # 更新统计信息
            self.stats["total_failed"] += 1

    def _dispatcher_thread(self):
        """任务分发线程"""
        while self.running:
            # 每10秒检查一次是否有新任务可分发
            time.sleep(10)

            if not self.url_manager.has_pending_urls():
                logging.info("没有待爬取的URL")
                continue

                # 主动通知工作节点有新任务
            self._notify_workers_new_task()

    def _notify_workers_new_task(self):
        """通知工作节点有新任务"""
        # 这个函数是一个占位符，实际中可能需要主动通知工作节点
        # 但是在我们的设计中，工作节点会定期请求任务，所以这里不需要实现
        pass

    def _monitor_thread(self):
        """监控线程，定期打印系统状态"""
        while self.running:
            time.sleep(30)  # 每30秒打印一次状态

            # 检查工作节点健康状态
            self.worker_manager.check_workers_health()

            # 计算爬取速率
            elapsed = time.time() - self.stats["start_time"]
            if elapsed > 0:
                self.stats["urls_per_second"] = self.stats["total_crawled"] / elapsed

                # 获取各模块统计信息
            url_stats = self.url_manager.get_stats()
            worker_stats = self.worker_manager.get_stats()

            # 打印统计信息
            logging.info("----- 系统状态 -----")
            logging.info(f"运行时间: {elapsed:.2f} 秒")
            logging.info(f"爬取速率: {self.stats['urls_per_second']:.2f} URL/秒")
            logging.info(f"总爬取数: {self.stats['total_crawled']}")
            logging.info(f"总失败数: {self.stats['total_failed']}")
            logging.info(f"种子URL数: {url_stats['seed_urls']}")
            logging.info(f"待爬取URL数: {url_stats['pending_urls']}")
            logging.info(f"已爬取URL数: {url_stats['crawled_urls']}")
            logging.info(f"失败URL数: {url_stats['failed_urls']}")
            logging.info(f"活跃工作节点数: {worker_stats['active_workers']}")
            logging.info(f"工作节点完成任务数: {worker_stats['total_completed']}")
            logging.info(f"工作节点失败任务数: {worker_stats['total_failed']}")
            logging.info("--------------------")


def start_master():
    """启动主节点"""
    master = Master(MASTER_HOST, MASTER_PORT)
    master.start()


if __name__ == "__main__":
    # 启动主节点
    start_master()