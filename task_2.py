import string
import concurrent.futures
from collections import defaultdict, Counter
import requests
from matplotlib import pyplot as plt

# Функція для видалення знаків пунктуації
def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))

def map_function(word) -> tuple:
    return word, 1

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

# Виконання MapReduce
def map_reduce(url, search_words=None):
    response = requests.get(url)
    if response.status_code == 200:
        # Отримання тексту з відповіді
        text = response.text

        # Видалення знаків пунктуації
        text = remove_punctuation(text)
        words = text.split()

        # Мапінг
        with concurrent.futures.ThreadPoolExecutor() as executor:
            mapped_values = list(executor.map(map_function, words))

        # Shuffle
        shuffled_values = shuffle_function(mapped_values)

        # Редукція
        with concurrent.futures.ThreadPoolExecutor() as executor:
            reduced_values = list(executor.map(reduce_function, shuffled_values))

        return dict(reduced_values)
    else:
        return None

def visual_result(result):
    top_10 = Counter(result).most_common(10)
    labels, values = zip(*top_10)
    plt.figure(figsize=(10, 5))
    plt.barh(labels, values, color='g')
    plt.xlabel('Кількість')
    plt.ylabel('Слово')
    plt.title('10 найпопулярніших слів')
    plt.show()

if __name__ == '__main__':
    # Вхідний текст для обробки
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    # Виконання MapReduce на вхідному тексті
    result = map_reduce(url)

    print("Результат підрахунку слів:", result)
    visual_result(result)