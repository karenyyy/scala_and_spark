from mrjob.job import MRJob
import re

class WordCount(MRJob):

    def mapper_init(self):
        self.set_status("Initializing")
        self.cache = {}
        self.cache_size = 1000

    def mapper(self, _, line):
        # read in one line in the file each time
        self.set_status("Mapping")
        tweets = "".join(line.split(",")[5:])
        words = re.compile(r"(http(s)?:/(/[A-Za-z0-9.\\-_]+)*|[A-Za-z]+(\')?[A-Za-z]+)").findall(tweets)
        for word in words:
            word=word[0]
            if word not in self.cache and len(self.cache) >= self.cache_size:
                for word, count in self.cache.items():
                    yield (word, count)
                self.cache = {}
                self.increment_counter("DS410", "flushes", 1)
            self.cache[word] = self.cache.get(word, 0) + 1

    def mapper_final(self):
        for word, count in self.cache.items():
            yield (word, count)
        self.cache = {}
        self.increment_counter("DS410", "flushes", 1)

    def reducer(self, word, count):
        self.set_status("Reducing")
        yield word, sum(count)


if __name__ == '__main__':
    WordCount.run()


