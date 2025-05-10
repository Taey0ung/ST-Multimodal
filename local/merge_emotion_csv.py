import os
import pandas as pd

# 데이터셋 폴더 경로
folder = os.path.join(os.path.dirname(__file__), '감정 분류를 위한 대화 음성 데이터셋')

# 결과를 저장할 리스트
merged_rows = []

# 폴더 내 모든 csv 파일 탐색
for fname in os.listdir(folder):
    if fname.lower().endswith('.csv'):
        fpath = os.path.join(folder, fname)
        # 인코딩 자동 감지 및 예외 처리
        df = None
        for encoding in ['utf-8-sig', 'cp949', 'euc-kr']:
            try:
                df = pd.read_csv(fpath, encoding=encoding)
                break
            except Exception as e:
                continue
        if df is None:
            print(f"[경고] 파일을 읽을 수 없습니다: {fname}")
            continue
        # 필요한 열만 추출 (없는 경우 에러 방지)
        if not set(['wav_id', '발화문', '상황']).issubset(df.columns):
            print(f"[경고] 필요한 열이 없는 파일: {fname}")
            continue
        df = df[['wav_id', '발화문', '상황']].copy()
        df = df.dropna(subset=['wav_id', '발화문', '상황'])
        for _, row in df.iterrows():
            segment_id = str(row['wav_id'])
            wav = segment_id + '.wav'
            txt = str(row['발화문'])
            emotion = str(row['상황'])
            merged_rows.append({
                'segment_id': segment_id,
                'wav': wav,
                'txt': txt,
                'emotion': emotion
            })

# 데이터프레임으로 변환 및 저장
result_df = pd.DataFrame(merged_rows, columns=['segment_id', 'wav', 'txt', 'emotion'])
result_df.to_csv('merged_emotion_dataset(1).csv', index=False, encoding='utf-8-sig')

print(f"총 {len(result_df)}개의 데이터가 병합되어 'merged_emotion_dataset.csv'로 저장되었습니다.")

# =============================
# wav 폴더와 segment_id 비교 코드
# =============================
wav_dir = os.path.join(os.path.dirname(__file__), 'wav')
if os.path.exists(wav_dir):
    wav_files = [os.path.splitext(f)[0] for f in os.listdir(wav_dir) if f.lower().endswith('.wav')]
    wav_set = set(wav_files)
else:
    print("[경고] wav 폴더가 존재하지 않습니다.")
    wav_set = set()

segment_id_set = set(result_df['segment_id'])

only_in_wav = sorted(list(wav_set - segment_id_set))
only_in_csv = sorted(list(segment_id_set - wav_set))
both = sorted(list(wav_set & segment_id_set))

print(f"\n[비교 결과]")
print(f"CSV에만 있는 segment_id 수: {len(only_in_csv)}")
if only_in_csv:
    print(f"예시: {only_in_csv[:10]}")
print(f"양쪽 모두에 있는 파일 수: {len(both)}")

# =============================
# CSV에만 있는 segment_id 행 삭제
# =============================
if only_in_csv:
    print(f"\n[정리] CSV에만 있는 segment_id {len(only_in_csv)}개 행을 삭제합니다.")
    result_df = result_df[~result_df['segment_id'].isin(only_in_csv)]
    result_df.to_csv('merged_emotion_dataset(1).csv', index=False, encoding='utf-8-sig')
    print(f"필터링 후 {len(result_df)}개의 데이터가 'merged_emotion_dataset(1).csv'로 저장되었습니다.") 