import os
import time
import shutil
import zipfile
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# 경로 상수 선언
SOURCE_DIR = Path("/content/drive/MyDrive/Datasets/data")
TARGET_DIR = Path("/content/data")
WAV_DIR = TARGET_DIR / "wav"
CSV_DIR = TARGET_DIR / "csv"

# 1. 파일 복사 함수

def copy_all_files(source_dir, target_dir):
    all_files = [f for f in source_dir.rglob("*") if f.is_file()]
    total = len(all_files)
    for idx, file_path in enumerate(all_files, 1):
        relative_path = file_path.relative_to(source_dir)
        destination_path = target_dir / relative_path

        if destination_path.exists() and destination_path.stat().st_size == file_path.stat().st_size:
            print(f"[{idx}/{total}] SKIP (already exists): {relative_path}")
            continue

        print(f"[{idx}/{total}] 복사 중: {relative_path}")
        copy_file_with_speed(file_path, destination_path)
    print("모든 파일 복사 완료!")

def copy_file_with_speed(src, dst, buffer_size=1024*1024):
    dst.parent.mkdir(parents=True, exist_ok=True)
    total_size = os.path.getsize(src)
    copied = 0
    start_time = time.time()
    last_time = start_time
    last_copied = 0
    with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
        while True:
            buf = fsrc.read(buffer_size)
            if not buf:
                break
            fdst.write(buf)
            copied += len(buf)
            now = time.time()
            elapsed = now - last_time
            if elapsed > 0.5:
                speed = (copied - last_copied) / elapsed / (1024 * 1024)
                percent = copied / total_size * 100
                remain_time = (total_size - copied) / (speed * 1024 * 1024) if speed > 0 else 0
                print(f"\r    파일 복사 진행률: {percent:.1f}% | 속도: {speed:.2f} MB/s | 남은 시간: {remain_time:.1f}초", end="")
                last_time = now
                last_copied = copied
    total_elapsed = time.time() - start_time
    avg_speed = copied / total_elapsed / (1024 * 1024) if total_elapsed > 0 else 0
    print(f"\r    파일 복사 진행률: 100.0% | 평균 속도: {avg_speed:.2f} MB/s | 남은 시간: 0.0초")

# 2. zip 압축 해제 함수

def extract_all_zips(data_dir):
    zip_files = list(data_dir.rglob("*.zip"))
    for zip_path in tqdm(zip_files, desc="압축 해제 진행률", leave=True):
        temp_extract_dir = data_dir / f"temp_extract_{zip_path.stem}"
        if temp_extract_dir.exists() and any(temp_extract_dir.iterdir()):
            print(f"SKIP (already extracted): {zip_path.name}")
            continue
        temp_extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            members = zip_ref.namelist()
            with tqdm(total=len(members), desc=f"{zip_path.name} 압축 해제", leave=False, position=0) as file_tqdm:
                for member in members:
                    zip_ref.extract(member, temp_extract_dir)
                    file_tqdm.update(1)
        # zip_path.unlink()  # 필요시 압축 해제 후 삭제

# 3. 특정 확장자 파일 이동 함수

def move_files_by_ext(data_dir, ext, target_dir):
    target_dir.mkdir(parents=True, exist_ok=True)
    all_files = [f for f in data_dir.rglob(f"*.{ext}") if f.parent != target_dir]
    total = len(all_files)
    start_time = time.time()
    for idx, file in enumerate(all_files, 1):
        dest = target_dir / file.name
        if dest.exists():
            dest.unlink()
        shutil.move(str(file), str(dest))
        elapsed = time.time() - start_time
        speed = idx / elapsed if elapsed > 0 else 0
        remain = (total - idx) / speed if speed > 0 else 0
        print(f"\r[{idx}/{total}] {ext} 이동 중: {file.name} | 속도: {speed:.2f}개/초 | 남은 시간: {remain:.1f}초", end="")
    print(f"\n모든 .{ext} 파일 이동 완료!")
    # 만약 csv 이동이 끝났으면 Session##_M_res.csv 파일 20개 삭제
    if ext == 'csv':
        for i in range(1, 21):
            filename = f"Session{str(i).zfill(2)}_M_res.csv"
            file_path = target_dir / filename
            if file_path.exists():
                file_path.unlink()
                print(f"{filename} 삭제 완료")
            else:
                print(f"{filename} 파일 없음")
        print("Session##_M_res.csv 파일 삭제 작업 완료!")

# 4. CSV 병합 함수 (특정 파일)

def merge_specific_csv(csv_files, output_path):
    dfs = []
    for file in csv_files:
        df = pd.read_csv(file, encoding='cp949')
        df = df[['wav_id', '발화문', '상황']]
        df = df.rename(columns={'wav_id': 'segment_id', '발화문': 'txt', '상황': 'emotion'})
        df['wav'] = df['segment_id'].astype(str) + '.wav'
        df = df[['segment_id', 'wav', 'txt', 'emotion']]
        dfs.append(df)
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"합쳐진 CSV가 {output_path} 에 저장되었습니다.")

# 5. 기타 CSV 병합 함수 (txt 내용 포함)

def merge_other_csv(csv_dir, data_dir, exclude_files, output_path):
    txt_files = list(data_dir.rglob("*.txt"))
    txt_dict = {f.name: f for f in txt_files}
    target_csv_files = [fname for fname in os.listdir(csv_dir) if fname.endswith('.csv') and fname not in exclude_files]
    data = []
    def read_text_file(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except UnicodeDecodeError:
            with open(path, 'r', encoding='cp949') as f:
                return f.read().strip()
    def process_csv(fname):
        csv_path = csv_dir / fname
        if os.path.getsize(csv_path) == 0:
            print(f"파일 {fname}이 비어 있습니다. 건너뜁니다.")
            return []
        try:
            df = pd.read_csv(csv_path, header=0)
        except pd.errors.EmptyDataError:
            print(f"파일 {fname}이 비어 있거나 컬럼이 없습니다. 건너뜁니다.")
            return []
        if 'Segment ID' not in df.columns:
            print(f"파일 {fname}에서 'Segment ID' 컬럼을 찾을 수 없습니다. 컬럼 구조: {df.columns.tolist()}")
            return []
        if 'Emotion' in df.columns:
            emotion_col = 'Emotion'
        elif 'Total Evaluation' in df.columns:
            emotion_col = 'Total Evaluation'
        else:
            print(f"파일 {fname}에서 'Emotion' 또는 'Total Evaluation' 컬럼을 찾을 수 없습니다. 컬럼 구조: {df.columns.tolist()}")
            return []
        rows = []
        for idx, row in df.iterrows():
            segment_id = row['Segment ID']
            emotion = row[emotion_col]
            wav_file = str(segment_id) + '.wav'
            txt_file_name = str(segment_id) + '.txt'
            txt_path = txt_dict.get(txt_file_name)
            if txt_path and txt_path.exists():
                txt_content = read_text_file(txt_path)
            else:
                txt_content = ''
            rows.append({'segment_id': segment_id, 'wav': wav_file, 'txt': txt_content, 'emotion': emotion})
        return rows
    with ThreadPoolExecutor(max_workers=8) as executor:
        for result in tqdm(executor.map(process_csv, target_csv_files), total=len(target_csv_files), desc="CSV 파일 병렬 처리 중", leave=False):
            data.extend(result)
    result_df = pd.DataFrame(data)
    result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f'완료! {output_path}로 저장되었습니다.')

# 6. DataFrame 합치기 함수

def merge_dataframes(csv1, csv2, output_path):
    df1 = pd.read_csv(csv1)
    df2 = pd.read_csv(csv2)
    merged_df = pd.concat([df1, df2], ignore_index=True)
    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"합쳐진 CSV가 {output_path} 에 저장되었습니다.")

# 7. wav/csv 비교 및 불일치 파일/row 삭제 함수

def sync_wav_and_csv(wav_dir, csv_path, diff_path):
    wav_files = [f.stem for f in wav_dir.glob("*.wav")]
    wav_set = set(wav_files)
    df = pd.read_csv(csv_path)
    csv_set = set(df['segment_id'].astype(str))
    wav_not_in_csv = sorted(wav_set - csv_set)
    csv_not_in_wav = sorted(csv_set - wav_set)
    max_len = max(len(wav_not_in_csv), len(csv_not_in_wav))
    wav_not_in_csv += [''] * (max_len - len(wav_not_in_csv))
    csv_not_in_wav += [''] * (max_len - len(csv_not_in_wav))
    result_df = pd.DataFrame({'wav_only': wav_not_in_csv, 'csv_only': csv_not_in_wav})
    result_df.to_csv(diff_path, index=False, encoding='utf-8-sig')
    print(f"{diff_path}로 저장되었습니다.")
    # 불일치 파일 삭제
    for fname in result_df['wav_only'].dropna():
        if fname and (wav_dir / (fname + '.wav')).exists():
            (wav_dir / (fname + '.wav')).unlink()
            print(f"{fname}.wav 삭제")
    merged_df = pd.read_csv(csv_path)
    merged_df = merged_df[~merged_df['segment_id'].astype(str).isin(result_df['wav_only'].dropna())]
    merged_df = merged_df[~merged_df['segment_id'].astype(str).isin(result_df['csv_only'].dropna())]
    merged_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print("불일치 파일/row 삭제 및 merged_all.csv 갱신 완료!")

# 8. nan row 삭제 함수

def remove_nan_segment_id(csv_path):
    df = pd.read_csv(csv_path)
    df = df[df['segment_id'].notna()]
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print("segment_id가 nan인 행을 삭제했습니다.")

# 9. wav 폴더 복사 함수

def copy_wav_folder(src, dst):
    if dst.exists():
        shutil.rmtree(dst)
    all_files = list(src.rglob("*.*"))
    for file in tqdm(all_files, desc="Copying files", unit="file"):
        relative_path = file.relative_to(src)
        target_file_path = dst / relative_path
        target_file_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file, target_file_path)
    print(f"총 {len(all_files)}개 파일 복사 완료!")

# 10. 메인 실행 함수

def remove_merged_csv_and_wav(csv_path, wav_dir):
    if os.path.exists(csv_path):
        os.remove(csv_path)
        print(f"{csv_path} 파일 삭제 완료")
    else:
        print(f"{csv_path} 파일이 존재하지 않습니다.")
    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)
        print(f"{wav_dir} 폴더 삭제 완료")
    else:
        print(f"{wav_dir} 폴더가 존재하지 않습니다.")

def clean_data_except_merged_and_wav(data_dir, merged_csv, wav_dir):
    data_dir = Path(data_dir)
    merged_csv = Path(merged_csv)
    wav_dir = Path(wav_dir)
    for item in data_dir.iterdir():
        if item == merged_csv or item == wav_dir:
            continue
        if item.is_file():
            item.unlink()
            print(f"파일 삭제: {item}")
        elif item.is_dir():
            import shutil
            shutil.rmtree(item)
            print(f"폴더 삭제: {item}")
    print("merged_all.csv와 wav 폴더를 제외한 모든 파일/폴더 삭제 완료!")

# 11. segment_id 기준 정렬 함수

def sort_csv_by_segment_id(csv_path):
    df = pd.read_csv(csv_path)
    df_sorted = df.sort_values(by="segment_id")
    df_sorted.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"segment_id 기준으로 정렬 완료: {csv_path}")

if __name__ == "__main__":
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    copy_all_files(SOURCE_DIR, TARGET_DIR)
    extract_all_zips(TARGET_DIR)
    move_files_by_ext(TARGET_DIR, 'wav', WAV_DIR)
    move_files_by_ext(TARGET_DIR, 'csv', CSV_DIR)
    merge_specific_csv([
        str(CSV_DIR / "4차년도.csv"),
        str(CSV_DIR / "5차년도.csv"),
        str(CSV_DIR / "5차년도_2차.csv")
    ], str(TARGET_DIR / "merged_segments.csv"))
    merge_other_csv(CSV_DIR, TARGET_DIR, {
        "4차년도.csv", "5차년도.csv", "5차년도_2차.csv"
    }, str(TARGET_DIR / "merged_others.csv"))
    merge_dataframes(
        str(TARGET_DIR / "merged_others.csv"),
        str(TARGET_DIR / "merged_segments.csv"),
        str(TARGET_DIR / "merged_all.csv")
    )
    sync_wav_and_csv(WAV_DIR, str(TARGET_DIR / "merged_all.csv"), str(TARGET_DIR / "wav_csv_diff.csv"))
    remove_nan_segment_id(str(TARGET_DIR / "merged_all.csv"))
    sort_csv_by_segment_id(str(TARGET_DIR / "merged_all.csv"))
    clean_data_except_merged_and_wav(TARGET_DIR, TARGET_DIR / "merged_all.csv", WAV_DIR)