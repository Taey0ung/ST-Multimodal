import pandas as pd

# CSV 파일 경로
csv_path = "merged_emotion_dataset_merged.csv"
df = pd.read_csv(csv_path)

def clean_emotion(emotion_str):
    # 감정 분리 (쉼표, 슬래시, 공백 등 구분자 처리)
    if pd.isna(emotion_str):
        return None
    emotions = [e.strip().lower() for e in emotion_str.replace('/', ',').split(',') if e.strip()]
    # 1. surprise 단독 삭제
    if len(emotions) == 1 and emotions[0] == "surprise":
        return None
    # 2. disgust→angry, fear→sad
    emotions = ["angry" if e == "disgust" else e for e in emotions]
    emotions = ["sad" if e == "fear" else e for e in emotions]
    # 3. anger→angry, happiness→happy, sadness→sad
    emotions = ["angry" if e == "anger" else e for e in emotions]
    emotions = ["happy" if e == "happiness" else e for e in emotions]
    emotions = ["sad" if e == "sadness" else e for e in emotions]
    # 4. 4개 감정만 남기기
    emotions = [e for e in emotions if e in {"neutral", "sad", "angry", "happy"}]
    if not emotions:
        return None
    # 5. 중복 제거
    emotions = list(dict.fromkeys(emotions))  # 순서 유지 중복 제거
    # 6. neutral + 다른 감정이면 neutral 제거
    if len(emotions) == 2 and "neutral" in emotions:
        emotions = [e for e in emotions if e != "neutral"]
    # 7. neutral 아닌 감정끼리 중복이면 첫 번째 감정 선택
    return emotions[0]

# emotion 열 정제
df['emotion'] = df['emotion'].apply(clean_emotion)
# surprise 단독 등으로 None된 행 삭제
df = df[df['emotion'].notna()]

# 결과 저장
# utf-8-sig로 저장하여 엑셀에서 한글이 깨지지 않도록 함
df.to_csv("merged_emotion_dataset_cleaned.csv", index=False, encoding="utf-8-sig")

# 전체 행의 수 출력
print(f"전체 행의 수: {len(df)}")
# emotion 분포 출력
print("emotion 분포:")
print(df['emotion'].value_counts())
print("정제 완료: merged_emotion_dataset_cleaned.csv") 