import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient


def search():
    # 主頁 URL
    url = "https://zh.wikipedia.org/zh-tw/%E4%B8%AD%E5%9B%BD%E4%BA%BA%E6%B0%91%E8%A7%A3%E6%94%BE%E5%86%9B%E7%A9%BA%E5%86%9B"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')  # 使用 BeautifulSoup 解析 HTML
    table = soup.find("table", class_="infobox")        # 找到目標 table

    if table:
        rows = table.find_all("tr")  # 找到 table 內的所有 tr
        last_9_rows = rows[-9:]      # 取最後 9 行

        # 初始化 MongoDB 連接
        client = MongoClient("mongodb://localhost:27017/")
        db = client["Demo"]  # 指定資料庫
        collection = db["Demo"]  # 指定集合

        for row in last_9_rows:
            ths = row.find_all("th", class_="infobox-label")
            tds = row.find_all("td", class_="infobox-data")

            for th, td in zip(ths, tds):
                # 分割 genre 中的多個值，並對每個值插入一筆資料
                genre_list = [g.strip() for g in td.text.split(",")]

                for genre in genre_list:
                    # 初始化主資料結構
                    data = {
                        "country": "China",
                        "type": th.text.strip(),
                        "genre": genre,
                        "Performance": {},
                        "Weapons": {}
                    }

                    # 搜尋細項的連結
                    genre_urls = []
                    links = td.find_all("a")
                    for link in links:
                        href = link.get("href")
                        if href:
                            genre_urls.append("https://zh.wikipedia.org" + href)

                    # 爬取細項資料
                    detailed_data = genre_search(genre_urls)
                    if detailed_data:
                        data.update(detailed_data)

                    # 插入到 MongoDB
                    collection.insert_one(data)
                    print(f"資料已插入: {data}")
                    print("\n---\n")


def genre_search(genre_urls):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    detailed_data = {"Performance": {}, "Weapons": {}}

    for url in genre_urls:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find("table", class_="infobox")

        if table:
            rows = table.find_all("tr")  # 找到所有 tr
            last_9_rows = rows[-10:]   # 取最後 10 行

            for row in last_9_rows:
                ths = row.find_all("th", class_="infobox-label")
                tds = row.find_all("td", class_="infobox-data")

                for th, td in zip(ths, tds):
                    label = th.text.strip()
                    value = td.text.strip()

                    # 判斷並動態存入 Performance 資料
                    if "最大速度" in label:
                        detailed_data["Performance"]["maximum_speed"] = value
                    elif "巡航速度" in label:
                        detailed_data["Performance"]["cruising_speed"] = value
                    elif "爬升率" in label:
                        detailed_data["Performance"]["rate_of_climb"] = value
                    elif "實用升限" in label:
                        detailed_data["Performance"]["practical_ceiling"] = value
                    elif "最大升限" in label:
                        detailed_data["Performance"]["maximum_ceiling"] = value
                    elif "最大航程" in label:
                        detailed_data["Performance"]["maximum_range"] = value
                    elif "作戰半徑" in label:
                        detailed_data["Performance"]["combat_radius"] = value
                    elif "巡航半徑" in label:
                        detailed_data["Performance"]["cruising_radius"] = value
                    elif "翼負荷" in label:
                        detailed_data["Performance"]["wing_loading"] = value
                    elif "滑跑距離" in label:
                        detailed_data["Performance"]["rolling_distance"] = value

                    # 判斷並動態存入 Weapons 資料
                    elif "機炮" in label:
                        detailed_data["Weapons"]["cannon"] = value
                    elif "火箭" in label:
                        detailed_data["Weapons"]["rocket"] = value
                    elif "飛彈" in label or "导弹" in label:
                        detailed_data["Weapons"]["missile"] = value
                    elif "炸彈" in label:
                        detailed_data["Weapons"]["bomb"] = value
                    elif "其他" in label:
                        detailed_data["Weapons"]["other"] = value

    return detailed_data


if __name__ == "__main__":
    search()
