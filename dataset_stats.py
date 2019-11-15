import matplotlib.pyplot as plt
import os

nums = list()

with open("dataset_stats", "r", encoding="utf-8") as textfile:
    for line in textfile:
        if line.startswith("*"):
            line = line.split()
            num_tweets = line[-2]
            num_tweets = num_tweets.split(",")
            num_tweets = "".join(num_tweets)
            num_tweets = int(num_tweets)
            nums.append(round(num_tweets/1000000, 2))

nums = sorted(nums, reverse=True)

fig = plt.figure(figsize=(20, 10))
axes = fig.add_axes()
plt.bar(range(len(nums)), nums, alpha=0.5, color='r')
plt.xlabel("event index")
plt.ylabel("number of tweets in million")
plt.title("number of tweets in major events fetched by keywords")
plt.xticks(range(len(nums)))
plt.savefig("./num_tweets_events.png")