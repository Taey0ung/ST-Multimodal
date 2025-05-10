import os
import pandas as pd
import glob

# 파일 경로
csv_path = 'merged_emotion_dataset_cleaned_txtcleaned_emotioncleaned.csv'
wav_root = 'wav'

# CSV 불러오기
print(f"{csv_path} 불러오는 중...")
df = pd.read_csv(csv_path)

# segment_id set
segment_ids = set(df['segment_id'].astype(str))

# 2. wav 폴더 내 모든 .wav 파일 경로 수집 (하위 폴더 포함)
wav_files = glob.glob(os.path.join(wav_root, '**', '*.wav'), recursive=True)

# 3. 파일명(확장자 제외) 추출
def get_wav_id(filepath):
    return os.path.splitext(os.path.basename(filepath))[0]

wav_ids = set(get_wav_id(f) for f in wav_files)

# 4. segment_id에 없는 .wav 파일 찾기
unmatched_wavs = [f for f in wav_files if get_wav_id(f) not in segment_ids]

print(f'일치하지 않는 .wav 파일 개수: {len(unmatched_wavs)}')

if unmatched_wavs:
    print('삭제할 .wav 파일 목록:')
    for f in unmatched_wavs:
        print(f)
    # 파일 삭제
    for f in unmatched_wavs:
        try:
            os.remove(f)
        except Exception as e:
            print(f'삭제 실패: {f} ({e})')
    print('일치하지 않는 .wav 파일 삭제 완료')
else:
    print('모든 .wav 파일이 segment_id와 일치합니다.')

# 총 데이터 개수
print(f"\n총 데이터(행) 개수: {len(df)}")

# emotion 분포
print(f"\nemotion 열 분포:")
print(df['emotion'].value_counts())

# =============================
# WAV에만 있는 파일 삭제
# =============================
deleted = []
if unmatched_wavs:
    print(f"\n[정리] WAV에만 있는 파일 {len(unmatched_wavs)}개를 삭제합니다.")
    for fname in unmatched_wavs:
        try:
            os.remove(fname)
            deleted.append(fname)
        except Exception as e:
            print(f"[오류] {fname} 삭제 실패: {e}")
    print(f"삭제된 파일 수: {len(deleted)}")
    if deleted:
        print(f"삭제된 파일 예시: {deleted[:10]}") 