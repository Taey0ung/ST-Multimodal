import os
import pandas as pd
from tqdm import tqdm

# txt 파일을 다양한 인코딩으로 읽기 위한 함수
def read_txt_with_fallback(path):
    for enc in ['utf-8', 'cp949', 'euc-kr']:
        try:
            with open(path, 'r', encoding=enc) as f:
                return f.read().strip()
        except Exception:
            continue
    return ''  # 모두 실패 시 빈 문자열 반환

# segment_id 컬럼 찾기
def find_segment_id_column(columns):
    for col in columns:
        if isinstance(col, tuple) and len(col) > 0 and col[0] == 'Segment ID':
            return col
    return None

# emotion 컬럼 찾기
def find_emotion_column(columns):
    for col in columns:
        if col == ('Total Evaluation', 'Emotion'):
            return col
    return None

# wav 폴더 내 txt 파일 인덱싱
def index_txt_files(wav_dir):
    txt_map = {}
    for root, _, files in os.walk(wav_dir):
        for f in files:
            if f.endswith('.txt'):
                seg_id = f[:-4].strip().lower()
                txt_map[seg_id] = os.path.join(root, f)
    return txt_map

# annotation 폴더 내 csv 파일 병합 함수
def merge_annotation_csv(ann_dir, txt_map, file_suffix, seg_col_func, emo_col_func, pbar, merged_rows, missing_txt_segments):
    if not os.path.exists(ann_dir):
        return
    for fname in os.listdir(ann_dir):
        if fname.endswith(file_suffix):
            fpath = os.path.join(ann_dir, fname)
            try:
                df = pd.read_csv(fpath, header=[0,1])
            except Exception as e:
                print(f"[경고] {fname} 읽기 실패: {e}")
                continue
            seg_col = seg_col_func(df.columns)
            emo_col = emo_col_func(df.columns)
            if seg_col and emo_col:
                for _, row in df.iterrows():
                    segment_id = str(row[seg_col])
                    segment_id_key = segment_id.strip().lower()
                    if not segment_id or segment_id == 'nan':
                        pbar.update(1)
                        continue
                    wav = segment_id + '.wav'
                    txt_val = ''
                    if segment_id_key in txt_map:
                        txt_val = read_txt_with_fallback(txt_map[segment_id_key])
                    else:
                        print(f"[경고] txt 파일 없음: {segment_id}")
                        missing_txt_segments.append(segment_id)
                        pbar.update(1)
                        continue
                    emotion = str(row[emo_col])
                    merged_rows.append({
                        'segment_id': segment_id,
                        'wav': wav,
                        'txt': txt_val,
                        'emotion': emotion
                    })
                    pbar.update(1)
            else:
                pbar.update(len(df))
                print(f"[경고] {fname}에서 필요한 열을 찾을 수 없음. (seg_col: {seg_col}, emo_col: {emo_col})")

# 메인 실행부
if __name__ == "__main__":
    merged_rows = []
    missing_txt_segments = []
    base_dir = os.path.dirname(__file__)
    wav19_dir = os.path.join(base_dir, 'KEMDy19_v1_3', 'wav')
    wav20_dir = os.path.join(base_dir, 'KEMDy20_v1_2', 'wav')
    txt_map_19 = index_txt_files(wav19_dir)
    txt_map_20 = index_txt_files(wav20_dir)

    # txt_map 예시 키 출력
    print(f"txt_map_19 예시 키: {list(txt_map_19.keys())[:10]}")
    print(f"txt_map_20 예시 키: {list(txt_map_20.keys())[:10]}")

    # 전체 row 개수 미리 세기
    total_rows = 0
    ann19_dir = os.path.join(base_dir, 'KEMDy19_v1_3', 'annotation')
    ann20_dir = os.path.join(base_dir, 'KEMDy20_v1_2', 'annotation')
    if os.path.exists(ann19_dir):
        for fname in os.listdir(ann19_dir):
            if fname.endswith('_F_res.csv'):
                try:
                    df = pd.read_csv(os.path.join(ann19_dir, fname), header=[0,1])
                    total_rows += len(df)
                except:
                    pass
    if os.path.exists(ann20_dir):
        for fname in os.listdir(ann20_dir):
            if fname.endswith('_eval.csv'):
                try:
                    df = pd.read_csv(os.path.join(ann20_dir, fname), header=[0,1])
                    total_rows += len(df)
                except:
                    pass

    pbar = tqdm(total=total_rows, desc='전체 진행률')

    # KEMDy19 병합
    merge_annotation_csv(
        ann19_dir, txt_map_19, '_F_res.csv',
        find_segment_id_column, find_emotion_column, pbar, merged_rows, missing_txt_segments
    )
    # KEMDy20 병합
    merge_annotation_csv(
        ann20_dir, txt_map_20, '_eval.csv',
        find_segment_id_column, find_emotion_column, pbar, merged_rows, missing_txt_segments
    )

    pbar.close()

    # 데이터프레임으로 변환 및 저장
    result_df = pd.DataFrame(merged_rows, columns=['segment_id', 'wav', 'txt', 'emotion'])
    result_df.to_csv('merged_emotion_dataset(2).csv', index=False, encoding='utf-8-sig')

    print(f"총 {len(result_df)}개의 데이터가 병합되어 'merged_emotion_dataset.csv'로 저장되었습니다.")

    # txt 파일이 없는 segment_id 목록 저장
    if missing_txt_segments:
        with open('missing_txt_segments.txt', 'w', encoding='utf-8') as f:
            for seg_id in missing_txt_segments:
                f.write(seg_id + '\n')
        print(f"txt 파일이 없는 segment_id가 {len(missing_txt_segments)}개 발견되어 'missing_txt_segments.txt'로 저장되었습니다.")
    else:
        print("모든 segment_id에 대해 txt 파일이 존재합니다.") 