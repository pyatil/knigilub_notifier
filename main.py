import re
import asyncio
import aiohttp

from collections import namedtuple

from config import TELEGRAM_API, TELEGRAM_TOKEN


KNIGILUB_PROFILE_PATTERN = (
    r"http://knigilub.ru/users/\w+"
)

NEWS_PATTERN = (
    r"(rel=\”nofollow.*Количество произведений у автора на СИ?)\1*"
)

AUTHOR_PATTERN = (
    r'.*00">(.*?) </a'  # author
    '.*u1=(.*?) target'  # author link
    '.*Дата последнего изменения">(.*?)</ac'  # changes date
    '.*автора на СИ в килобайтах">(.*k?)</acr'  # changes size
)


New = namedtuple("New", ["name", "url", "last_changes", "size_changes"])


def format_new(new):
    return "[%s](%s) *%s*" % (new.name, new.url, new.size_changes)


class LastTelegramUpdate(object):
    value = 0


class Subscriber(object):
    def __init__(self, telegram_id, knigilub_profile):
        self.telegram_id = telegram_id
        self.knigilub_profile = knigilub_profile
        self.news_cache = set()
        self.news_queue = asyncio.Queue()
        self._initial_cache = False

    def parse_news(self, text):
        for raw_new in re.findall(NEWS_PATTERN, text):
            try:
                parsed_new = re.search(AUTHOR_PATTERN, raw_new).groups()
            except Exception:
                continue
            new = New(*parsed_new)
            if self._initial_cache:
                if new not in self.news_cache:
                    self.news_cache.add(new)
                    print("add new to queue:", new)
                    self.news_queue.put_nowait(format_new(new))
            else:
                self.news_cache.add(new)
        # print("initial cache for profile:", self.knigilub_profile)
        assert self.news_cache
        self._initial_cache = True


subscribers = {}  # id: Subscriber


@asyncio.coroutine
def fetch_knigilub_news():
    print("start fetch_knigilub_news")
    for subsc_id, sub in subscribers.items():
        sub_profile = sub.knigilub_profile
        print("check profile:%s" % sub_profile)
        response = yield from aiohttp.request(
            'GET', sub_profile,
        )
        text = yield from response.text()
        sub.parse_news(text)
    print("finish fetch_knigilub_news")


@asyncio.coroutine
def send_news():
    print("start send_news")
    for sub_id, sub in subscribers.items():
        if not sub.news_queue.empty():
            new = yield from sub.news_queue.get()
            response = yield from aiohttp.request(
                'POST', "%s%s/sendMessage" % (TELEGRAM_API, TELEGRAM_TOKEN),
                data={'chat_id': sub_id, 'text': new, 'parse_mode': 'MARKDOWN'}
            )
            text = yield from response.text()
            print("telegram res:", text)
    print("finish send_news")


@asyncio.coroutine
def fetch_new_subscribers():
    print("start fetch_new_subscribers")
    response = yield from aiohttp.request(
        'POST', "%s%s/getUpdates" % (TELEGRAM_API, TELEGRAM_TOKEN),
        data={'offset': LastTelegramUpdate.value + 1, 'limit': 0, 'timeout': 0}
    )
    updates = yield from response.json()
    for update in updates['result']:
        LastTelegramUpdate.value = update['update_id']
        message = update.get('message', {})
        text = message.get('text', '')
        isCorrectProfile = re.match(KNIGILUB_PROFILE_PATTERN, text)
        if isCorrectProfile:
            sub_id = update['message']['chat']['id']
            if sub_id not in subscribers:
                subscribers[sub_id] = Subscriber(sub_id, text)
                print("new sub:", sub_id, subscribers[sub_id].knigilub_profile)
    print("finish fetch_new_subscribers")


@asyncio.coroutine
def run_tasks_every(tiemout, tasks):
    while True:
        for task in tasks:
            yield from task()
        yield from asyncio.sleep(tiemout)


def main():
    loop = asyncio.get_event_loop()
    tasks = [fetch_new_subscribers, fetch_knigilub_news, send_news]
    task = run_tasks_every(600, tasks)
    loop.run_until_complete(task)


def test_fetch_profile():
    subscribers[25] = Subscriber(25, "http://knigilub.ru/users/1111")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_knigilub_news())


if __name__ == '__main__':
    pass
    main()
    # test_fetch_profile()
