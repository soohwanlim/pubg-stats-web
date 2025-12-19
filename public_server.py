from flask import Flask, render_template, send_file, request, jsonify
import os
import pubg_client as pc
from datetime import datetime

app = Flask(__name__)

# 1. 메인 홈페이지
@app.route('/')
def home():
    notices = pc.get_notices()
    return render_template('web_index.html', notices=notices)

# 2. 웹 라이브 상황판
@app.route('/live')
def live_dashboard():
    return render_template('web_live.html')

# 3. 전적 검색 페이지
@app.route('/stats')
def stats_page():
    nickname = request.args.get('nickname')
    platform = request.args.get('platform', 'steam') # Basic default
    
    if nickname:
        print(f"🔍 Searching stats for: {nickname} on {platform}")
        data = pc.get_recent_stats(nickname, platform)
        
        # timestamp formatting
        if data and "updated_at" in data:
            ts = data['updated_at']
            # Calculate "X minutes ago"
            import time
            elapsed = time.time() - ts
            if elapsed < 60:
                data['time_ago'] = "방금 전"
            elif elapsed < 3600:
                data['time_ago'] = f"{int(elapsed/60)}분 전"
            else:
                data['time_ago'] = f"{int(elapsed/3600)}시간 전"
        
        return render_template('web_stats.html', data=data, search_query=nickname)
    else:
        # 검색어 없이 들어오면 그냥 빈 페이지 혹은 검색 유도
        return render_template('web_stats.html', data=None)

# 4. [핵심] 다운로드 처리
@app.route('/download')
def download():
    # 서버 폴더에 있는 zip 파일을 찾아서 사용자에게 보냄
    file_name = "PUBG_Tracker_v1.0.zip"
    
    if os.path.exists(file_name):
        print(f"✅ 다운로드 요청 처리 중: {file_name}")
        return send_file(file_name, 
                         as_attachment=True,         # 다운로드 창 띄우기
                         download_name="PUBG_Tracker_v1.0.zip") # 저장될 이름
    else:
        return f"<h1>⚠️ 서버 오류: {file_name} 파일이 없습니다.</h1><p>개발자가 파일을 서버 폴더에 넣지 않았습니다.</p>"

# 4-1. 전적 갱신 API
@app.route('/api/update_stats', methods=['POST'])
def api_update_stats():
    nickname = request.json.get('nickname')
    platform = request.json.get('platform', 'steam')
    
    result = pc.update_player_stats(nickname, platform)
    return jsonify(result)

# 5. 랭킹 페이지
@app.route('/ranking')
def ranking_page():
    platform = request.args.get('platform', 'steam')
    # Default to ranked squad for now
    data, updated_at = pc.get_leaderboard(platform, 'squad-fpp')
    
    # Process timestamp 
    # If 0, it means failure or no data found
    from datetime import datetime
    if updated_at > 0:
        updated_str = datetime.fromtimestamp(updated_at).strftime('%Y-%m-%d %H:%M:%S')
    else:
        updated_str = "업데이트 정보 없음"

    return render_template('web_ranking.html', ranking=data, platform=platform, updated_at=updated_str)

if __name__ == '__main__':
    print("🌍 Public Server Running on Port 8080")
    app.run(port=8080, debug=True)