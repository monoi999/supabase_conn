import os
from typing import Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, inspect
from supabase_client import delete_record
import uuid
import toml


DB_TABLE = "score_data"
# Resolve CSV path: prefer local file, fall back to parent folder's score.csv
DEFAULT_CSV_CANDIDATES = ["score.csv", os.path.join(os.path.pardir, "score.csv")]
CSV_PATH = None
for p in DEFAULT_CSV_CANDIDATES:
	if os.path.exists(p):
		CSV_PATH = p
		break
if CSV_PATH is None:
	# use first candidate by default (will be checked later)
	CSV_PATH = DEFAULT_CSV_CANDIDATES[0]


def get_engine() -> Optional[object]:
	global CONNECTION_SOURCE, CONNECTION_ERROR
	CONNECTION_SOURCE = None
	CONNECTION_ERROR = None

	# 1) try environment
	db_url = os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL")
	if db_url:
		try:
			CONNECTION_SOURCE = "env: DATABASE_URL"
			return create_engine(db_url)
		except Exception as e:
			CONNECTION_ERROR = str(e)
			# try ssl fallback
			try:
				CONNECTION_ERROR += "; retrying with sslmode=require"
				return create_engine(db_url, connect_args={"sslmode": "require"})
			except Exception as e2:
				CONNECTION_ERROR += f"; ssl retry failed: {e2}"

	# 2) try streamlit secrets
	try:
		conns = st.secrets.get("connections", {})
		if isinstance(conns, dict):
			for name, cfg in conns.items():
				if isinstance(cfg, dict):
					url = cfg.get("url") or cfg.get("dsn") or cfg.get("connection_string")
					if url:
						try:
							CONNECTION_SOURCE = f"secrets.connections.{name}"
							return create_engine(url)
						except Exception as e:
							CONNECTION_ERROR = f"secrets.connections.{name}: {e}"
							try:
								CONNECTION_ERROR += "; retrying with sslmode=require"
								return create_engine(url, connect_args={"sslmode": "require"})
							except Exception as e2:
								CONNECTION_ERROR += f"; ssl retry failed: {e2}"
								continue
	except Exception as e:
		CONNECTION_ERROR = CONNECTION_ERROR or str(e)

	# 3) try reading .streamlit/secrets.toml next to this file
	try:
		here = os.path.dirname(__file__)
		secret_path = os.path.join(here, ".streamlit", "secrets.toml")
		if os.path.exists(secret_path):
			try:
				with open(secret_path, "r", encoding="utf-8") as f:
					data = toml.load(f)
				conns = data.get("connections", {})
				if isinstance(conns, dict):
					for name, cfg in conns.items():
						if isinstance(cfg, dict):
							url = cfg.get("url") or cfg.get("dsn") or cfg.get("connection_string")
							if url:
								try:
									CONNECTION_SOURCE = f"file:.streamlit/secrets.toml -> connections.{name}"
									return create_engine(url)
								except Exception as e:
									CONNECTION_ERROR = f"file secrets.connections.{name}: {e}"
									try:
										return create_engine(url, connect_args={"sslmode": "require"})
									except Exception as e2:
										CONNECTION_ERROR += f"; ssl retry failed: {e2}"
										continue
			except Exception as e:
				CONNECTION_ERROR = CONNECTION_ERROR or str(e)

	except Exception:
		pass

	return None


def ensure_table(engine):
	if engine is None:
		return
	insp = inspect(engine)
	if insp.has_table(DB_TABLE):
		return
	create_sql = f"""
	CREATE TABLE IF NOT EXISTS {DB_TABLE} (
		id TEXT PRIMARY KEY,
		class INTEGER,
		name TEXT,
		email TEXT,
		tel TEXT,
		avg FLOAT,
		grade TEXT
	);
	"""
	with engine.begin() as conn:
		conn.execute(text(create_sql))


def load_data(engine) -> pd.DataFrame:
	if engine is not None:
		try:
			df = pd.read_sql_table(DB_TABLE, con=engine)
			return df
		except Exception:
			pass

	# fallback to CSV
	df = pd.read_csv(CSV_PATH)
	# Normalize column names (support Korean headers in provided CSV)
	col_map = {
		"ID": "id",
		"반": "class",
		"이름": "name",
		"이메일": "email",
		"연락처": "tel",
		"평균": "avg",
		"등급": "grade",
	}
	df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
	# ensure expected columns
	expected = ["id", "class", "name", "email", "tel", "avg", "grade"]
	for c in expected:
		if c not in df.columns:
			df[c] = None
	return df[expected]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
	# map various possible header names to canonical English keys used throughout the app
	if df is None or df.empty:
		return df
	col_map = {
		"id": ["id", "ID"],
		"class": ["class", "Class", "반"],
		"name": ["name", "Name", "이름"],
		"email": ["email", "Email", "이메일"],
		"tel": ["tel", "Tel", "연락처"],
		"avg": ["avg", "Avg", "평균"],
		"grade": ["grade", "Grade", "등급"],
	}
	rename = {}
	cols = set(df.columns)
	for canon, candidates in col_map.items():
		for c in candidates:
			if c in cols:
				rename[c] = canon
				break
	if rename:
		df = df.rename(columns=rename)
	# ensure expected canonical columns exist
	expected = ["id", "class", "name", "email", "tel", "avg", "grade"]
	for c in expected:
		if c not in df.columns:
			df[c] = None
	return df


def upsert_record(engine, rec: dict):
	# rec keys: id,class,name,email,tel,avg,grade
	if engine is not None:
		upsert_sql = f"""
		INSERT INTO {DB_TABLE} (id, class, name, email, tel, avg, grade)
		VALUES (:id, :class, :name, :email, :tel, :avg, :grade)
		ON CONFLICT (id) DO UPDATE SET
		  class = EXCLUDED.class,
		  name = EXCLUDED.name,
		  email = EXCLUDED.email,
		  tel = EXCLUDED.tel,
		  avg = EXCLUDED.avg,
		  grade = EXCLUDED.grade;
		"""
		with engine.begin() as conn:
			# SQLAlchemy Connection.execute may not accept keyword args depending on version.
			# Pass the parameter dict as the second positional argument.
			conn.execute(text(upsert_sql), rec)
		return True

	# fallback: append/update CSV
	df = pd.read_csv(CSV_PATH)
	# normalize
	if "ID" in df.columns:
		df = df.rename(columns={"ID": "id", "반": "class", "이름": "name", "이메일": "email", "연락처": "tel", "평균": "avg", "등급": "grade"})
	if rec.get("id") in df["id"].astype(str).values:
		df.loc[df["id"].astype(str) == str(rec["id"]), ["id", "class", "name", "email", "tel", "avg", "grade"]] = [rec["id"], rec.get("class"), rec.get("name"), rec.get("email"), rec.get("tel"), rec.get("avg"), rec.get("grade")]
	else:
		df = df.append({"id": rec.get("id"), "class": rec.get("class"), "name": rec.get("name"), "email": rec.get("email"), "tel": rec.get("tel"), "avg": rec.get("avg"), "grade": rec.get("grade")}, ignore_index=True)
	# write back
	df.to_csv(CSV_PATH, index=False)
	return True


def compute_stats(df: pd.DataFrame) -> dict:
	out = {}
	if df.empty:
		return out
	out["count"] = len(df)
	# flexible column name resolution
	def find_col(candidates):
		for c in candidates:
			if c in df.columns:
				return c
		return None

	avg_col = find_col(["avg", "Avg", "평균"])
	grade_col = find_col(["grade", "Grade", "등급"])
	class_col = find_col(["class", "Class", "반"])

	try:
		if avg_col:
			out["avg_of_avg"] = float(pd.to_numeric(df[avg_col], errors="coerce").dropna().mean())
		else:
			out["avg_of_avg"] = None
	except Exception:
		out["avg_of_avg"] = None

	# grade distribution
	if grade_col and grade_col in df.columns:
		out["by_grade"] = df[grade_col].value_counts(dropna=True).to_dict()
	else:
		out["by_grade"] = {}

	# class stats
	if class_col and avg_col and class_col in df.columns and avg_col in df.columns:
		# ensure numeric
		tmp = df[[class_col, avg_col]].copy()
		tmp[avg_col] = pd.to_numeric(tmp[avg_col], errors="coerce")
		grouped = tmp.groupby(class_col)[avg_col].agg(["count", "mean"]).to_dict(orient="index")
		out["by_class"] = grouped
	else:
		out["by_class"] = {}
	return out


def main():
	st.title("성적처리 (Streamlit + Supabase)")

	st.sidebar.header("환경 설정")
	st.sidebar.markdown("앱은 `DATABASE_URL` 환경 변수가 설정되어 있으면 데이터베이스에 연결합니다. 없으면 `score.csv`를 사용합니다.")
	engine = get_engine()
	if engine is not None:
		src = globals().get("CONNECTION_SOURCE") or "unknown"
		st.sidebar.success(f"DB 연결 사용 ({src})")
		ensure_table(engine)
	else:
		# If DB not available, only fall back to CSV when the file exists.
		if os.path.exists(CSV_PATH):
			st.sidebar.info("로컬 CSV 사용 (score.csv)")
		else:
			# No DB and no CSV -> treat as connection failure
			st.sidebar.error("데이터 소스 없음: DB 연결 실패하고 로컬 파일 score.csv가 존재하지 않습니다.")
			if globals().get("CONNECTION_ERROR"):
				st.sidebar.error(str(globals().get("CONNECTION_ERROR")))
			st.error("데이터 연결 실패: 관리자에게 문의하거나 score.csv를 준비하세요.")
			st.stop()

	

	df = load_data(engine)
	# Normalize column names to canonical keys (name, email, class, avg, grade, id, tel)
	df = normalize_columns(df)

	tabs = st.tabs(["조회", "입력", "통계", "동기화"])

	with tabs[0]:
		st.header("조회")
		q = st.text_input("검색 (이름 또는 이메일 또는 반으로 검색할 수 있습니다)")
		filt = df.copy()
		if q:
			# build a combined mask for name, email, or class (class may be numeric)
			mask = pd.Series(False, index=df.index)
			if "name" in df.columns:
				mask = mask | df["name"].astype(str).str.contains(q, case=False, na=False)
			if "email" in df.columns:
				mask = mask | df["email"].astype(str).str.contains(q, case=False, na=False)
			if "class" in df.columns:
				mask = mask | df["class"].astype(str).str.contains(q, case=False, na=False)
			filt = filt[mask]
		# render table into a placeholder so we can refresh it after delete without full rerun
		table_ph = st.empty()
		table_ph.dataframe(filt)

		# Deletion helper: select an ID from filtered results and delete with confirmation
		ids = []
		try:
			ids = filt['id'].astype(str).tolist()
		except Exception:
			ids = []
		if ids:
			sel = st.selectbox("삭제할 ID 선택", options=[""] + ids, key="delete_select")
			if sel:
				if st.button("삭제", key="delete_request"):
					st.session_state.pending_delete = sel
		if st.session_state.get('pending_delete'):
			pdid = st.session_state.pending_delete
			st.warning(f"정말 삭제하시겠습니까? ID={pdid} — 이 작업은 되돌릴 수 없습니다.")
			if st.button("확인: 삭제", key="confirm_delete"):
				ok = False
				try:
					ok = delete_record(engine, pdid, csv_path=CSV_PATH)
				except Exception as e:
					st.error(f"삭제 중 오류: {e}")
					ok = False
				if ok:
					st.success("삭제 완료")
					st.session_state.pending_delete = None
					# reload data and update table placeholder
					try:
						new_df = load_data(engine)
						new_df = normalize_columns(new_df)
						new_filt = new_df.copy()
						q_val = st.session_state.get('q_text') if 'q_text' in st.session_state else None
						if q_val:
							mask = pd.Series(False, index=new_df.index)
							if "name" in new_df.columns:
								mask = mask | new_df["name"].astype(str).str.contains(q_val, case=False, na=False)
							if "email" in new_df.columns:
								mask = mask | new_df["email"].astype(str).str.contains(q_val, case=False, na=False)
							if "class" in new_df.columns:
								mask = mask | new_df["class"].astype(str).str.contains(q_val, case=False, na=False)
							new_filt = new_df[mask]
						table_ph.dataframe(new_filt)
					except Exception:
						# fallback to full rerun if immediate refresh fails
						try:
							st.experimental_rerun()
						except Exception:
							pass
				else:
					st.error("삭제 실패: 레코드가 없거나 권한이 없습니다.")

	with tabs[1]:
		st.header("신규/수정 입력")

		# initialize session state for form-driven workflow
		if 'mode' not in st.session_state:
			st.session_state.mode = 'new'  # or 'edit'
		if 'loaded_id' not in st.session_state:
			st.session_state.loaded_id = None
		# form fields stored in session_state so they can be pre-filled on load
		if 'form_class' not in st.session_state:
			st.session_state.form_class = 1
		if 'form_name' not in st.session_state:
			st.session_state.form_name = ''
		if 'form_email' not in st.session_state:
			st.session_state.form_email = ''
		if 'form_tel' not in st.session_state:
			st.session_state.form_tel = ''
		if 'form_avg' not in st.session_state:
			st.session_state.form_avg = 0.0
		if 'form_grade' not in st.session_state:
			st.session_state.form_grade = 'A'

		st.caption("ID를 입력한 후 `Load` 버튼으로 기존 레코드를 불러오세요. 불러온 후 필드를 확인하고 `저장`하세요.")
		# smaller ID input column so UI looks tighter
		col1, col2 = st.columns([2,1])
		with col1:
			sid_input = st.text_input("ID", value="", key="sid_input")
		with col2:
			if st.button("Load"):
				key = sid_input.strip()
				if key == "":
					st.warning("ID를 입력하세요.")
				else:
					# try to find record in current dataframe (loaded from DB or CSV)
					found = None
					try:
						mask = df['id'].astype(str) == str(key)
						if mask.any():
							found = df.loc[mask].iloc[0]
					except Exception:
						found = None
					if found is not None:
						# populate session state fields from found record
						st.session_state.form_class = int(found.get('class') if pd.notna(found.get('class')) else 1)
						st.session_state.form_name = found.get('name') or ''
						st.session_state.form_email = found.get('email') or ''
						st.session_state.form_tel = found.get('tel') or ''
						try:
							st.session_state.form_avg = float(found.get('avg') if pd.notna(found.get('avg')) else 0.0)
						except Exception:
							st.session_state.form_avg = 0.0
						st.session_state.form_grade = found.get('grade') or 'A'
						st.session_state.mode = 'edit'
						st.session_state.loaded_id = key
						st.success("기존 레코드 로드됨 — 수정 모드")
					else:
						# clear form fields for new entry
						st.session_state.form_class = 1
						st.session_state.form_name = ''
						st.session_state.form_email = ''
						st.session_state.form_tel = ''
						st.session_state.form_avg = 0.0
						st.session_state.form_grade = 'A'
						st.session_state.mode = 'new'
						st.session_state.loaded_id = None
						st.info("신규 입력 모드 — 저장을 눌러 새 레코드를 생성하세요.")

		with st.form("add_form"):
			# form fields use keys that map to session_state so values persist/are prefilled
			sclass = st.number_input("Class", min_value=1, step=1, value=st.session_state.form_class, key="form_class")
			name = st.text_input("Name", value=st.session_state.form_name, key="form_name")
			email = st.text_input("Email", value=st.session_state.form_email, key="form_email")
			tel = st.text_input("Tel", value=st.session_state.form_tel, key="form_tel")
			avg = st.number_input("Avg", min_value=0.0, max_value=100.0, value=st.session_state.form_avg, step=0.01, key="form_avg")
			grade = st.selectbox("Grade", ["A", "B", "C", "D", "F"], index=["A","B","C","D","F"].index(st.session_state.form_grade) if st.session_state.form_grade in ["A","B","C","D","F"] else 0, key="form_grade")
			submitted = st.form_submit_button("저장")
		if submitted:
			# basic validation: require name
			if not (name and str(name).strip()):
				st.error("이름(Name)은 필수 입력 항목입니다.")
			else:
				# determine id and whether this is a new record
				if st.session_state.mode == 'edit' and st.session_state.loaded_id:
					save_id = st.session_state.loaded_id
					is_new = False
				else:
					save_id = sid_input.strip() or f"user_{uuid.uuid4().hex[:8]}"
					is_new = True
				# check duplicate id when creating
				exists = False
				try:
					exists = (df['id'].astype(str) == str(save_id)).any()
				except Exception:
					exists = False
				if is_new and exists:
					st.error("해당 ID가 이미 존재합니다. 기존 레코드를 수정하려면 Load 버튼으로 불러오세요.")
				else:
					rec = {"id": save_id, "class": int(sclass), "name": name, "email": email, "tel": tel, "avg": float(avg), "grade": grade}
					ok = upsert_record(engine, rec)
					if ok:
						st.success("레코드 저장 완료")
						st.session_state.mode = 'edit'
						st.session_state.loaded_id = save_id
						# refresh app to show updated data
						try:
							st.experimental_rerun()
						except Exception:
							# fallback: reload local dataframe
							df = load_data(engine)
							df = normalize_columns(df)
					else:
						st.error("저장 실패")

	with tabs[2]:
		st.header("통계")
		stats = compute_stats(df)
		st.metric("전체 학생 수", stats.get("count", 0))
		st.metric("평균 평균점수", f"{stats.get('avg_of_avg', 0):.2f}")
		st.subheader("등급 분포")
		# grade distribution chart
		grade_col = None
		for c in ("grade", "Grade", "등급"):
			if c in df.columns:
				grade_col = c
				break
		if grade_col:
			grade_count = df[grade_col].value_counts().reset_index()
			grade_count.columns = ["grade", "count"]
			import altair as alt
			chart = alt.Chart(grade_count).mark_bar().encode(x=alt.X("grade:N", sort="-y"), y="count:Q", color="grade:N")
			st.altair_chart(chart, width='stretch')
		else:
			st.write({})

		st.subheader("반별 평균 (평균 점수)")
		# class average chart
		class_col = None
		for c in ("class", "Class", "반"):
			if c in df.columns:
				class_col = c
				break
		avg_col = None
		for c in ("avg", "Avg", "평균"):
			if c in df.columns:
				avg_col = c
				break
		if class_col and avg_col:
			class_avg = df.groupby(class_col)[avg_col].apply(lambda s: pd.to_numeric(s, errors="coerce").mean()).reset_index()
			class_avg.columns = ["class", "avg"]
			chart2 = alt.Chart(class_avg).mark_bar().encode(x="class:N", y="avg:Q", color="class:N")
			st.altair_chart(chart2, width='stretch')
		else:
			st.write({})

		st.subheader("평균 점수 분포")
		if avg_col:
			hist = alt.Chart(df).transform_density(avg_col, as_=["Avg", "density"]).mark_area(opacity=0.4).encode(x="Avg:Q", y="density:Q")
			st.altair_chart(hist, width='stretch')
		else:
			st.write("평균 점수 데이터가 없습니다.")

	with tabs[3]:
		st.header("동기화")
		st.write("데이터를 DB에 업로드하거나 CSV로 내려받을 수 있습니다.")
		if engine is None:
			st.info("DB 연결 정보가 없습니다. `DATABASE_URL` 환경변수를 설정하세요.")
		else:
			st.write("### CSV -> DB 업로드 (전체)")
			uploaded_file = st.file_uploader("업로드할 CSV 파일을 선택하세요", type=["csv"], accept_multiple_files=False)
			if uploaded_file is not None:
				try:
					csv_df = pd.read_csv(uploaded_file)
				except Exception as e:
					st.error(f"파일을 읽을 수 없습니다: {e}")
					csv_df = None
				if csv_df is not None:
					st.write("업로드 미리보기")
					st.dataframe(csv_df.head())
					if st.button("선택한 파일을 DB로 업로드"):
						uploaded = 0
						# normalize headers if needed
						if "ID" in csv_df.columns:
							csv_df = csv_df.rename(columns={"ID": "id", "반": "class", "이름": "name", "이메일": "email", "연락처": "tel", "평균": "avg", "등급": "grade"})
						for _, r in csv_df.iterrows():
							rec = {"id": str(r.get("id") if pd.notna(r.get("id")) else f"user_{uuid.uuid4().hex[:8]}"),
								"class": int(r.get("class") if pd.notna(r.get("class")) else 0),
								"name": r.get("name"),
								"email": r.get("email"),
								"tel": r.get("tel"),
								"avg": float(r.get("avg") if pd.notna(r.get("avg")) else 0.0),
								"grade": r.get("grade")}
							try:
								upsert_record(engine, rec)
								uploaded += 1
							except Exception as e:
								st.warning(f"레코드 업로드 실패: {e}")
						st.success(f"업로드 완료: {uploaded} 건")

		st.write("### CSV로 저장 (파일명 지정)")
		save_name = st.text_input("저장할 파일명", value="export_score.csv")
		if st.button("파일로 저장"):
			# prepare CSV bytes and provide a download button (safer than saving on server)
			fname = os.path.basename(save_name.strip())
			if not fname:
				st.error("유효한 파일명을 입력하세요")
			else:
				try:
					csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
					st.download_button(label=f"다운로드 {fname}", data=csv_bytes, file_name=fname, mime="text/csv")
					st.success(f"다운로드 버튼이 생성되었습니다 — 클릭하여 {fname}을(를) 저장하세요.")
				except Exception as e:
					st.error(f"다운로드 준비 실패: {e}")


if __name__ == "__main__":
	main()

