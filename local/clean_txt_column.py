import pandas as pd
import re
import string

# 파일 경로
input_path = 'merged_emotion_dataset_cleaned.csv'
output_path = 'merged_emotion_dataset_cleaned_txtcleaned.csv'

# 데이터 불러오기
df = pd.read_csv(input_path)

def clean_txt(text):
    if pd.isnull(text):
        return text
    # 1. 슬래시(/)와 그 앞 한 글자 모두 삭제
    text = re.sub(r'.?/', '', str(text))
    # 2. 괄호( )와 그 안의 텍스트 모두 삭제
    text = re.sub(r'\([^)]*\)', '', text)
    # 앞뒤 공백 정리
    return text.strip()

# txt 컬럼 정제
df['txt'] = df['txt'].apply(clean_txt)

# txt 컬럼이 빈칸(공백 포함)이거나, NaN이거나, 특수문자 한 글자만 있는 경우 필터링
is_blank = df['txt'].isnull() | df['txt'].astype(str).str.strip().eq('')
is_special_onechar = df['txt'].astype(str).str.match(rf'^[{re.escape(string.punctuation)}]$')

to_drop = df[is_blank | is_special_onechar]
print(f"삭제할 행 개수: {len(to_drop)}")
if len(to_drop) > 0:
    print("삭제할 행 정보:")
    print(to_drop)

df_cleaned = df[~(is_blank | is_special_onechar)].copy()

df_cleaned.to_csv('merged_emotion_dataset_cleaned_txtcleaned_emotioncleaned.csv', index=False, encoding='utf-8-sig')

print('txt 컬럼 정제 완료: merged_emotion_dataset_cleaned_txtcleaned_emotioncleaned.csv')

print(f'정제 완료: {output_path}') 