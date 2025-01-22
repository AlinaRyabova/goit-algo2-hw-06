from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import requests
import matplotlib.pyplot as plt

def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        return None

def map_function(word):
    return word, 1

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

def map_reduce(text):
    words = text.split()

    # Паралельний Мапінг
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Паралельна Редукція
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

def visualize_top_words(word_freq):
    top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]
    words, counts = zip(*top_words)
    plt.bar(words, counts)
    plt.xlabel('Words')
    plt.ylabel('Frequency')
    plt.title('Top 10 Words by Frequency')
    plt.xticks(rotation=45)
    plt.show()

if __name__ == '__main__':
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = get_text(url)
    if text:
        result = map_reduce(text)

        # Виведення топ 10 слів
        top_words = sorted(result.items(), key=lambda x: x[1], reverse=True)[:10]
        for word, freq in top_words:
            print(f"{word}: {freq}")

        # Візуалізація топ 10 слів
        visualize_top_words(result)
    else:
        print("Помилка: Не вдалося отримати вхідний текст.")
