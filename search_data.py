from pymongo import MongoClient
import redis
import json
from bson import ObjectId
from datetime import datetime
import schedule  # 排程
import time

# 初始化 MongoDB 和 Redis 連線
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["Demo"]
collection = db["Demo"]

# 這裡檢查 Redis 是否可用，若不可用則完全跳過 Redis 的使用
redis_client = None
try:
    redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
    redis_client.ping()  # 嘗試 ping Redis
    print("Redis 連接成功")
except (redis.ConnectionError, redis.RedisError) as e:
    print(f"Redis 連接失敗，錯誤訊息: {str(e)}")
    redis_client = None  # 若 Redis 失敗，將 redis_client 設為 None，回退至 MongoDB

# 自定義 JSON 編碼器，用於處理 ObjectId
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)  # 將 ObjectId 轉為字串
        return super().default(obj)

# 將 MongoDB 資料載入到 Redis
def load_data_to_redis():
    if redis_client:
        print(f"開始刷新 Redis 資料: {datetime.now()}")
        # 清空 Redis 現有資料
        redis_client.flushdb()
        
        # 從 MongoDB 抓取資料
        for document in collection.find():
            key = str(document["_id"])  # 使用 MongoDB 的 `_id` 唯一屬性 作為 key
            value = json.dumps(document, cls=JSONEncoder)  # 使用編碼器轉換文件
            redis_client.set(key, value)
        print("資料已刷新至 Redis")
    else:
        print("Redis 未啟動，無法更新資料至 Redis")

# 搜索邏輯：加入後備機制，若 Redis 不可用則從 MongoDB 查詢
def search_keyword_in_cache(keyword):
    result = []
    if redis_client:
        try:
            # 嘗試從 Redis 搜索
            for key in redis_client.scan_iter():  # 掃描所有 key
                value = json.loads(redis_client.get(key))  # 從 Redis 獲取並解析 JSON
                if keyword.lower() in value.get("type", "").lower() or keyword.lower() in value.get("genre", "").lower():
                    result.append(value)
            if not result:
                raise ValueError("No results from Redis, fallback to MongoDB.")
        except (redis.ConnectionError, redis.RedisError, ValueError) as e:
            print(f"Redis 查詢失敗，使用 MongoDB 後備: {str(e)}")
            # 如果 Redis 查詢失敗，則從 MongoDB 查詢
            result = []
            for document in collection.find():
                # 僅當符合關鍵字的字段時，才返回結果
                if (keyword.lower() in document.get("type", "").lower() or 
                    keyword.lower() in document.get("genre", "").lower()):
                    result.append(json.loads(json.dumps(document, cls=JSONEncoder)))  # 確保 MongoDB 資料也能序列化
    else:
        print("Redis 連接失敗，直接使用 MongoDB 查詢")
        # 若 Redis 服務不可用，直接從 MongoDB 查詢
        result = []
        for document in collection.find():
            # 僅當符合關鍵字的字段時，才返回結果
            if (keyword.lower() in document.get("type", "").lower() or 
                keyword.lower() in document.get("genre", "").lower()):
                result.append(json.loads(json.dumps(document, cls=JSONEncoder)))  # 確保 MongoDB 資料也能序列化

    return result

# 設定排程，每 30 分鐘更新 Redis 資料
schedule.every(30).minutes.do(load_data_to_redis)

# 主程式
if __name__ == "__main__":
    # 初次加載資料
    if redis_client:
        load_data_to_redis()

    # 啟動排程
    while True:
        schedule.run_pending()
        time.sleep(1)
        
        # 用戶輸入查詢
        search_keyword = input("請輸入關鍵字進行搜索,輸入exit離開: ")
        if search_keyword == "exit":
            break
        else:
            search_results = search_keyword_in_cache(search_keyword)
            if search_results:
                print(f"找到的結果有 {len(search_results)} 筆: ")
                for result in search_results:
                    print(json.dumps(result, indent=4, ensure_ascii=False))
            else:
                print("未找到匹配結果。")
