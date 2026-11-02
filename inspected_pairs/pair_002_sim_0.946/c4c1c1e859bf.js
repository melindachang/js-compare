import urllib.request
import ssl
from bs4 import BeautifulSoup
import datetime
import json
import os

# -----------------------------------------------------------------------------
# 설정 / configuration
# -----------------------------------------------------------------------------
# GitHub Actions 또는 환경 변수에서 토큰과 ID를 가져옵니다.
# 로컬에서는 하드코딩된 값을 기본값으로 사용하거나 .env 파일을 쓸 수 있게 구성할 수 있습니다.
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '8707545343:AAGLSUsmxgr2irc7aaVxjqpNvN-JEq8Ysco')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '8400311014')

def get_menu_data():
    """신구대 홈페이지에서 오늘의 식단 데이터를 가져옵니다."""
    url = "https://www.shingu.ac.kr/cms/FR_CON/index.do?MENU_ID=1630"
    
    # SSL 핸드쉐이크 에러 방지를 위한 컨텍스트 설정
    # 일부 서버는 특정 프로토콜이나 사이퍼를 요구할 수 있습니다.
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    # 레거시 서버 대응을 위해 사이퍼 설정 (필요시)
    context.set_ciphers('DEFAULT@SECLEVEL=1')
    
    try:
        # urllib.request 사용하여 가져오기
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, context=context) as response:
            html = response.read().decode('utf-8')
            soup = BeautifulSoup(html, 'html.parser')

        
        # 오늘 날짜 정보 수집 (한국 시간 기준)
        from datetime import timezone, timedelta
        kst = timezone(timedelta(hours=9))
        now = datetime.datetime.now(kst)
        target_yymm = now.strftime('%Y.%m')
        target_dd = now.strftime('%d')
        
        # 식단 리스트 찾기
        diet_list = soup.select('ul.diet_list > li, .diet_list li')
        
        today_menu = ""
        found = False

        for li in diet_list:
            day_div = li.select_one('.day')
            if not day_div:
                continue
                
            yymm = day_div.select_one('.yymm').text.strip() if day_div.select_one('.yymm') else ""
            dd = day_div.select_one('.dd').text.strip() if day_div.select_one('.dd') else ""
            
            # 오늘 날짜와 일치하는지 확인
            if yymm == target_yymm and dd == target_dd:
                found = True
                menu_div = li.select_one('.menu')
                if menu_div:
                    p_tags = menu_div.find_all('p')
                    for p in p_tags:
                        category_span = p.select_one('.markRec')
                        if category_span:
                            category = category_span.text.strip()
                            # span 태그 제외한 나머지 텍스트 추출 (메뉴 내용)
                            items = p.get_text(separator="\n", strip=True).replace(category, "").strip()
                            today_menu += f"🍱 [{category}]\n{items}\n\n"
                break
        
        if not found:
            return f"📅 {now.strftime('%Y-%m-%d')} 식단 정보를 찾을 수 없습니다."

        message = f"📅 오늘의 신구대 식단 안내 ({now.strftime('%Y-%m-%d')})\n\n"
        message += today_menu
        message += "맛있게 드세요! 😋"
        
        return message

    except Exception as e:
        return f"❌ 데이터를 가져오는 중 오류가 발생했습니다: {e}"

def send_telegram_message(message):
    """텔레그램으로 메시지를 전송합니다."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    # 텔레그램 전송 데이터 구성
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    
    try:
        print("📤 텔레그램 메시지 전송 중...")
        with urllib.request.urlopen(req, context=ssl._create_unverified_context()) as response:
            result = json.loads(response.read().decode())
            if result.get("ok"):
                print("✅ 성공적으로 전송되었습니다!")
                return True
            else:
                print(f"❌ 전송 실패: {result.get('description')}")
                return False
    except Exception as e:
        print(f"❌ 전송 오류: {e}")
        return False

if __name__ == "__main__":
    menu_info = get_menu_data()
    print("-" * 30)
    print(menu_info)
    print("-" * 30)
    send_telegram_message(menu_info)

