import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
files = os.listdir("./distributions")
txt_files = [filename for filename in files if filename.endswith('.txt')]
pageRankDistributions = []
txt_files.sort()
print(txt_files)
for filename in txt_files:
    pageRankDistributions.append(pd.read_csv("./distributions/"+filename, header=None))

print(pageRankDistributions)
header = 0.7
STDEV = []
MEAN = []
X = [0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
for dist in pageRankDistributions:
    norm_dist = dist/len(dist)
    #print(np.sum(norm_dist))
    STDEV.append(np.std(norm_dist.values))
    MEAN.append(np.mean(dist.values))
    #print("Stddev for Normalised pageRank with damping ", header, " : ", norm_dist.std())
    #print("Stddev for Normalised pageRank with damping ", header, " : ", norm_dist.std())

print(X)
print(MEAN)
plt.bar(X,MEAN, width=0.02)
plt.xlabel("Different damping values")
plt.ylabel("Mean in PageRank distribution")
plt.title("Variation of Mean with Damping factor")
plt.show()


