


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
import os
from config import result_path_strategy1


category_path = os.path.join(result_path_strategy1, "fan_token")
target_pair = "CHZ_PORTO"
pair_path = os.path.join(category_path, target_pair)
target_path = os.path.join(pair_path, 'CHZ_PORTO.csv')

df = pd.read_csv(target_path)
df = df[['entry_z', 'exit_z', 'stop_z', 'total_ret']]

# 변수 설정
X = df[['entry_z', 'exit_z', 'stop_z']]
y = df['total_ret']
feature_names = ['entry_z', 'exit_z', 'stop_z']

# RandomForest 모델 학습
rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(X, y)

# 그래프 설정
fig, axs = plt.subplots(2, 2, figsize=(16, 14))
axs = axs.ravel()

# 각 독립변수에 대한 2D 산점도
for i, feature in enumerate(feature_names):
    # 나머지 두 변수의 고유값 쌍 생성
    other_features = [f for f in feature_names if f != feature]
    unique_pairs = df[other_features].drop_duplicates()

    for _, pair in unique_pairs.iterrows():
        # 고정된 두 변수의 값과 일치하는 데이터 선택
        mask = (df[other_features[0]] == pair[other_features[0]]) & (df[other_features[1]] == pair[other_features[1]])
        subset = df[mask]

        # 정렬 후 플롯
        subset_sorted = subset.sort_values(by=feature)
        axs[i].plot(subset_sorted[feature], subset_sorted['total_ret'], '-o', markersize=4,
                    label=f"{other_features[0]}={pair[other_features[0]]:.2f}, {other_features[1]}={pair[other_features[1]]:.2f}")

    axs[i].set_xlabel(feature)
    axs[i].set_ylabel('total_ret')
    axs[i].set_title(f'{feature} vs total_ret')
    axs[i].legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small')

# 특성 중요도
importances = rf.feature_importances_
indices = np.argsort(importances)[::-1]
axs[3].bar(range(X.shape[1]), importances[indices])
axs[3].set_title("Feature Importances")
axs[3].set_xticks(range(X.shape[1]))
axs[3].set_xticklabels([feature_names[i] for i in indices], rotation=45)

plt.tight_layout()
plt.subplots_adjust(top=0.95, right=0.85)

os.makedirs(pair_path, exist_ok=True)
image_filename = os.path.join(pair_path, "z_score_performance")
plt.savefig(image_filename)
