# import os
# import requests

# AUTH_KEY = os.environ.get("KRX_AUTH_KEY", "996F6A8B342145A696C06D05F3AB782F552BCAB4")
# BASE_URL = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"

# url = "https://openapi.krx.co.kr/svc/apis/sto/stk_bydd_trd"

# def fetch_stk_bydd_trd(bas_dd: str):
#     headers = {
#         "AUTH_KEY": AUTH_KEY,          # 스샷/가이드대로 헤더에 넣기
#         "Accept": "application/json",
#     }
#     params = {"basDd": bas_dd}         # 스샷처럼 쿼리 파라미터
#     r = requests.get(url, headers=headers, params=params, timeout=15)
#     r.raise_for_status()
#     return r.json()

# if __name__ == "__main__":
#     data = fetch_stk_bydd_trd("20260304")
#     # 스펙상 OutBlock_1 배열이 옴 :contentReference[oaicite:1]{index=1}
#     rows = data.get("OutBlock_1", [])
#     print("rows:", len(rows))
#     print(rows[0] if rows else data)


import requests

AUTH_KEY = "74D1B99DFBF345BBA3FB4476510A4BED4C78D13A"
MY_AUTH_KEY = "996F6A8B342145A696C06D05F3AB782F552BCAB4"

#url = "https://openapi.krx.co.kr/svc/apis/sto/stk_bydd_trd"
#URL = "https://data-dbg.krx.co.kr/svc/sample/apis/idx/krx_dd_trd"
URL = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"

headers = {
    "AUTH_KEY": MY_AUTH_KEY,
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
}

params = {
    "basDd": "20260304"
}

r = requests.get(URL, headers=headers, params=params, timeout=20)

print("status:", r.status_code)
print("content-type:", r.headers.get("Content-Type"))

# JSON이면 json으로, 아니면 앞부분만 출력
ct = (r.headers.get("Content-Type") or "").lower()
if "json" in ct:
    data = r.json()
    # 정상은 OutBlock_1 리스트 :contentReference[oaicite:2]{index=2}
    print("keys:", list(data.keys()))
    print("rows:", len(data.get("OutBlock_1", [])))
    if data.get("OutBlock_1"):
        print("first row:", data["OutBlock_1"][0])
else:
    print(r.text[:300])