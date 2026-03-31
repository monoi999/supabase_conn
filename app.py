import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime

# 페이지 설정
st.set_page_config(page_title="Google Sheet Library", page_icon="📊", layout="wide")

# 디자인 CSS (상단 박스 높이 통일 포함)
st.markdown("""
    <style>
    [data-testid="stMetric"] {
        background-color: #ffffff;
        border-radius: 10px;
        padding: 15px;
        border: 1px solid #f0f2f6;
        min-height: 130px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .stButton>button { border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

# 1. 구글 시트 연결
conn = st.connection("gsheets", type=GSheetsConnection)

# 2. 데이터 로드 함수
def load_data():
    # 시트의 모든 데이터를 읽어옵니다.
    return conn.read(ttl=0) # 실시간 반영을 위해 ttl=0

# 3. 데이터 저장 함수 (전체 덮어쓰기 방식)
def save_data(df):
    conn.update(data=df)
    st.cache_data.clear()

# --- 앱 로직 시작 ---
df = load_data()

with st.sidebar:
    st.title("🍀 G-Sheet Admin")
    menu = st.radio("메뉴", ["📊 대시보드", "📚 도서 관리", "➕ 신규 등록"])
    st.divider()
    if st.button("🔄 강제 새로고침"):
        st.rerun()

# --- 1. 대시보드 ---
if menu == "📊 대시보드":
    st.title("System Overview (Google Sheets)")
    
    if not df.empty:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("전체 도서", f"{len(df)}권")
        m2.metric("대출 중", f"{len(df[df['status'] == '대출중'])}권")
        m3.metric("대출 가능", f"{len(df[df['status'] == '대출가능'])}권")
        m4.metric("기타/분실", f"{len(df[df['status'] == '분실'])}권")
        
        st.divider()
        st.subheader("실시간 시트 데이터 미리보기")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("데이터가 없습니다. 도서를 먼저 등록해주세요.")

# --- 2. 도서 관리 (수정 및 삭제) ---
elif menu == "📚 도서 관리":
    st.title("Inventory Management")
    
    if not df.empty:
        for index, row in df.iterrows():
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
                with c1:
                    st.markdown(f"**{row['title']}**")
                    st.caption(f"저자: {row['author']}")
                with c2:
                    # 인덱스를 이용한 상태 변경
                    status_options = ["대출가능", "대출중", "분실"]
                    new_status = st.selectbox("상태", status_options, 
                                            index=status_options.index(row['status']),
                                            key=f"status_{index}")
                    if new_status != row['status']:
                        df.at[index, 'status'] = new_status
                        df.at[index, 'updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M")
                        save_data(df)
                        st.rerun()
                with c3:
                    # 간단한 수정 (제목/저자) - 여기서는 텍스트 입력으로 예시
                    if st.button("📝 수정", key=f"edit_{index}"):
                        st.info("수정 모드는 팝업 대신 직접 입력을 지원하도록 확장 가능합니다.")
                with c4:
                    if st.button("🗑️ 삭제", key=f"del_{index}"):
                        df = df.drop(index)
                        save_data(df)
                        st.warning("삭제되었습니다.")
                        st.rerun()
    else:
        st.write("표시할 데이터가 없습니다.")

# --- 3. 신규 등록 ---
elif menu == "➕ 신규 등록":
    st.title("Register New Book")
    with st.form("add_form"):
        title = st.text_input("도서 제목")
        author = st.text_input("저자")
        status = st.selectbox("초기 상태", ["대출가능", "대출중"])
        submit = st.form_submit_button("구글 시트에 저장")
        
        if submit:
            if title:
                new_data = {
                    "id": len(df) + 1,
                    "title": title,
                    "author": author,
                    "status": status,
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                # 기존 데이터프레임에 추가
                df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)
                save_data(df)
                st.success(f"'{title}'이(가) 구글 시트에 기록되었습니다!")
            else:
                st.error("제목을 입력하세요.")
