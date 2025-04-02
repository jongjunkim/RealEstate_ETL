import requests
import configparser

config = configparser.ConfigParser(interpolation=None)
config.read("config.ini")
api_key = config["Realestate_API_Key"]["api_key"]

#공공API 데이터 request
def getRTMSDataSvcAptTrade(LAWD_CD, DEAL_YMD):
    base_url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
    
    params = {
        "LAWD_CD": LAWD_CD,
        "DEAL_YMD": DEAL_YMD,
        "serviceKey": api_key
    }

    response = requests.get(base_url, params=params)

    print("실제 요청된 URL:")
    print(response.url)  

    if response.status_code == 200:
        print("Reqeust Success")
        print(response.text)  
    else:
        print(f"Request Failed: {response.status_code}")

getRTMSDataSvcAptTrade("11110", "202401")
