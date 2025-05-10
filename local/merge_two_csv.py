import pandas as pd

# 파일 경로
csv1 = 'merged_emotion_dataset(1).csv'
csv2 = 'merged_emotion_dataset(2).csv'
output_csv = 'merged_emotion_dataset_merged.csv'

# CSV 파일 불러오기
print(f"{csv1} 불러오는 중...")
df1 = pd.read_csv(csv1)
print(f"{csv2} 불러오는 중...")
df2 = pd.read_csv(csv2)

# 데이터 합치기
merged_df = pd.concat([df1, df2], ignore_index=True)

# 중복 제거 (모든 열이 동일한 경우만)
merged_df = merged_df.drop_duplicates()

# 결과 저장
merged_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
print(f"총 {len(merged_df)}개의 데이터가 '{output_csv}'로 저장되었습니다.") 