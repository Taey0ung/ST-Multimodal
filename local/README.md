---

# 📁 Korean Multimodal Emotion Dataset Preprocessing Pipeline

이 문서는 로컬 환경에서 멀티모달 감정 인식 학습을 위한 `.wav` 오디오 파일과 여러 CSV 파일을 결합하고 정제하는 전체 과정을 설명합니다.

## ✅ 전체 순서 요약

```bash
1. move_all_wavs.py
2. merge_emotion_csv.py
3. merge_kemdy_csv.py
4. merge_two_csv.py
5. clean_emotion_column.py
6. clean_txt_column.py
7. remove_unmatched_wavs.py
```

---

## 1. `move_all_wavs.py`: 모든 .wav 파일 단일화

* 여러 경로에 분산된 `.wav` 파일들을 하나의 `wav/` 디렉토리로 이동합니다.
* 파일 구조를 단순화하여 이후 정제 및 매칭 작업을 쉽게 하기 위함입니다.

```bash
📁 /source1/*.wav  ─┐
📁 /source2/*.wav  ─┴─▶ 📁 /wav/
```

---

## 2. `merge_emotion_csv.py`: 감정 분류 대화 데이터 결합

* AIHub 또는 기타 감정 분류 기반 대화 데이터셋을 단일 `.csv` 파일로 병합합니다.
* 주로 자유대화 기반 감정 태깅 데이터에 적용됩니다.

## 3. `merge_kemdy_csv.py`: KEMDy19/20 정제 및 병합

* KEMDy19와 KEMDy20의 세그먼트 레벨 `.csv` 파일을 읽고 단일 `.csv`로 결합합니다.
* 감정 레이블, 텍스트, Segment ID 등 주요 열 유지.

---

## 4. `merge_two_csv.py`: 두 종류의 데이터셋 통합

* 위의 2단계와 3단계에서 생성된 두 개의 `.csv`를 하나로 합칩니다.
* 통일된 구조로 모델 학습에 사용 가능.

---

## 5. `clean_emotion_column.py`: 감정 라벨 정제

* `emotion` 열에서 **단일 감정**만 남기고 다중 감정 또는 모호한 감정 라벨 제거.

---

## 6. `clean_txt_column.py`: 텍스트 컬럼 정제

* 슬래시(`/`), 괄호 등 불필요한 기호 제거
* 텍스트가 비어 있거나 특수문자 1글자만 포함한 경우 해당 행 제거

---

## 7. `remove_unmatched_wavs.py`: 매칭되지 않는 오디오 제거

* 최종 `.csv` 파일과 `wav/` 폴더의 `segment_id`를 비교하여
* `.csv`에 존재하지 않는 `.wav` 파일은 `wav/` 폴더에서 삭제

---

## 🧼 최종 결과

* 📁 `wav/`: 유효한 `.wav` 오디오 파일만 존재
* 🗂️ `merged_emotion_dataset_cleaned_txtcleaned_emotioncleaned`: 모델 학습용 단일 CSV 파일

---
