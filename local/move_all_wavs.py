import os
import shutil

# 기준 경로
base_dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.join(base_dir)
wav_dir = os.path.join(root_dir, 'wav')

# wav 폴더가 없으면 생성
os.makedirs(wav_dir, exist_ok=True)

wav_count = 0

for foldername, subfolders, filenames in os.walk(root_dir):
    # 'wav' 폴더 자체는 탐색에서 제외
    if os.path.abspath(foldername) == os.path.abspath(wav_dir):
        continue
    for filename in filenames:
        if filename.lower().endswith('.wav'):
            src_path = os.path.join(foldername, filename)
            dst_path = os.path.join(wav_dir, filename)
            # 파일명이 중복될 경우 이름 변경
            if os.path.exists(dst_path):
                base, ext = os.path.splitext(filename)
                i = 1
                while True:
                    new_name = f"{base}_{i}{ext}"
                    dst_path = os.path.join(wav_dir, new_name)
                    if not os.path.exists(dst_path):
                        break
                    i += 1
            shutil.move(src_path, dst_path)
            wav_count += 1

print(f"총 {wav_count}개의 .wav 파일을 '{wav_dir}'로 이동했습니다.") 