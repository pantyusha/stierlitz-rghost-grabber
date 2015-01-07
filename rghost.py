#!/usr/bin/env python

from gevent.monkey import patch_all
patch_all()
import gevent
import gevent.queue
import gevent.event
import gevent.pool

import itertools
import time
import glob
import requests
import random
import requesocks

import os
import urllib
from lxml import html
import logging
from logging import warning, info, error

import user_agents


class NotExistsYet(Exception):
    pass


class Forbidden(Exception):
    pass


def get_max_size(ext):
    # Put here your limitations for different file types
    if ext in ['mp3', 'wav', 'ogg']:
        return 300
    elif ext in ['exe', 'scr']:
        return 0
    elif ext in ['pdf']:
        return 10e6
    elif ext in ['apk', ]:
        return 0
    elif ext in ['rar', 'zip']:
        return 10e6
    # Other files
    return 10e6


def download_sharded(n, shard, *args, **kwargs):
    n = shard.sharded_to_real(n)
    return download(n, *args, **kwargs)


def download(n, downloads_folder, session):
    "Download file "
    link = 'http://rghost.net/{n}'.format(n=n)
    info("Downloading %d", n)
    fail_filename = os.path.join(downloads_folder.bad, str(n) + '.html')
    response = session.get(link)
    if response.status_code != 200:
        warning("Got status code %d for getting %s", response.status_code, link)
        if response.status_code == 404:
            raise NotExistsYet()
        handle_fail(response, fail_filename)
    tree = html.fromstring(response.text)
    download_link_elements = tree.xpath('''//a[contains(concat(' ', normalize-space(@class), ' '), ' download ')]''')
    if not download_link_elements:
        warning("Can't find download link for %s, status code: %d", link, response.status_code)
        handle_fail(response, fail_filename)
    download_link = download_link_elements[0].attrib['href']
    extension = download_link.rpartition('.')[-1]
    if '/' in extension or '\x00' in extension:
        extension = ''
    if extension:
        extension = '.' + extension
    filename = str(n) + extension

    max_size = get_max_size(extension.lower().strip('.'))
    estimated_size_elements = tree.xpath('//h1/small')

    for element in estimated_size_elements:
        text = element.text_content().strip()
        if text.startswith('(') and text.endswith(')'):
            estimated_size = float('inf')
            try:
                estimated_size = parse_size(text[1:-1])
            except Exception:
                error("Failed to parse size %s", text, exc_info=True)

    response = session.get(download_link)
    if response.status_code != 200:
        warning("Got status code %d for getting %s", response.status_code, download_link)
        if response.status_code == 404:
            raise NotExistsYet()
        handle_fail(response, fail_filename)

    if estimated_size > max_size:
        warning("Skipping %s because it's size is %s", link, pretty_size(estimated_size))
        return

    size = handle_file(response, os.path.join(downloads_folder.good, filename))
    try:
        real_filename = store_filename(download_link, os.path.join(downloads_folder.names, filename))
    except Exception:
        error('Failed to store filename', exc_info=True)
    else:
        info("Downloaded %s, name: %s, size: %s", link, real_filename, pretty_size(size))


def parse_size(val):
    num, _, mod = val.partition(' ')
    num = float(num)
    if mod == 'MB':
        num *= 1e6
    elif mod == 'KB':
        num *= 1e3
    return num


def pretty_size(size):
    if 0 <= size < 1000:
        f = size
        suf = 'B'
    elif 1000 <= size < 1000000:
        f = size / 1e3
        suf = 'KB'
    elif 1000000 <= size:
        f = size / 1e6
        suf = 'MB'
    return '%.2f %s' % (f, suf)


def store_filename(link, filename):
    downloaded_file_name = link.rpartition('/')[-1]
    downloaded_file_name = urllib.unquote(downloaded_file_name)
    with open(filename, 'wb') as f:
        f.write(downloaded_file_name)
    return downloaded_file_name


def handle_fail(response, filename):
    with open(filename, 'wb') as f:
        f.write(response.text.encode('utf-8'))
    response.raise_for_status()


def handle_file(response, filename):
    size = 0
    failed = True
    try:
        with open(filename + '.part', 'wb') as f:
            first = True
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:  # filter out keep-alive new chunks
                    if first:
                        if chunk.startswith('<!DOCTYPE html>') and 'Fast and free download from rghost' in chunk and '<html' in chunk:
                            raise ValueError("RGhost gave html instead of actual file")
                        first = False
                    f.write(chunk)
                    f.flush()
                    size += len(chunk)
        failed = False
    finally:
        if failed:
            try:
                os.unlink(filename + '.part')
            except Exception:
                error("Failed to unlink file %s.part", filename, exc_info=True)
        raise
    os.rename(filename + '.part', filename)
    return size


def worker(input_queue, output_queue, tasks_lock, tasks_set, done_tasks, timeout=90):
    for task in input_queue:
        func, n = task[:2]
        with tasks_lock:
            if n in tasks_set or n in done_tasks:
                continue
            tasks_set.add(n)
        try:
            result = gevent.event.AsyncResult()
            try:
                with gevent.Timeout(timeout):
                    func(*task[1:])
            except (Exception, gevent.Timeout), ex:
                result.set_exception(ex)
            else:
                result.set(result)
            output_queue.put((n, result))
        finally:
            with tasks_lock:
                tasks_set.discard(n)
                done_tasks.add(n)


def downloader(shard, downloads_folder, session_factory):
    info("Another cycle of sansara")
    concurency = 3
    pool = gevent.pool.Pool(concurency)
    input_queue = gevent.queue.Queue(concurency)
    output_queue = gevent.queue.Queue()

    done_tasks = set()
    tasks_set = set()
    tasks_lock = gevent.lock.RLock()
    for i in xrange(concurency):
        pool.spawn(worker, input_queue, output_queue, tasks_lock, tasks_set, done_tasks, timeout=60)

    session = session_factory.next()
    tree = html.fromstring(session.get('http://rghost.net/main').text)
    latest_links_elements = tree.xpath("//div[contains(concat(' ', normalize-space(@class), ' '), ' main-column ')]/ul/li/a")
    latest_link = max(el.attrib['href'] for el in latest_links_elements)
    latest_num = int(latest_link[1:])
    latest_num = shard.real_to_sharded(latest_num)

    task_retries = {}
    suspicious = set()

    MAX_RETRIES = 2

    def add_job(n, force=False):
        if force:
            with tasks_lock:
                done_tasks.discard(n)
        input_queue.put((
            download_sharded,
            n,
            shard,
            downloads_folder,
            session_factory.next()
            )
        )

    start = time.time()

    while time.time() < start + 90:
        for i in xrange(concurency):
            try:
                out = output_queue.get_nowait()
            except gevent.queue.Empty:
                break
            else:
                n, result = out
                try:
                    result = result.get()
                except NotExistsYet:
                    if n in suspicious:
                        suspicious.discard(n)
                        warning("File %d seems to be instantly 404", n)
                        continue
                    if latest_num > n:
                        suspicious.add(n)
                    elif latest_num == n:
                        gevent.sleep(shard.sub_total * 0.5)
                    add_job(n, force=True)
                except (Exception, gevent.Timeout):
                    error("Failed to download %d", n, exc_info=True)
                    retries = task_retries.get(n, 0)
                    if retries <= MAX_RETRIES:
                        info("Retrying %d", n)
                        task_retries[n] = task_retries.get(n, 0) + 1
                        add_job(n, force=True)
                    else:
                        info("%d has reached max retry tries", n)
                        task_retries.pop(n, 0)
                    latest_num = max(latest_num, n + 1)

                else:
                    latest_num = max(latest_num, n + 1)
        add_job(latest_num)
        gevent.sleep(0.1)

    pool.kill()

class Shard(object):
    def __init__(self, shard, total, sub_shard, sub_total):
        assert 0 <= shard < total
        assert 0 <= sub_shard < sub_total
        self.shard = shard
        self.total = total
        self.sub_shard = sub_shard
        self.sub_total = sub_total

    def real_to_sharded(self, n):
        n /= self.total
        n /= self.sub_total
        return n

    def sharded_to_real(self, n):
        n *= self.sub_total
        n += self.sub_shard
        n *= self.total
        n += self.shard
        return n


def get_session_factory(proxy):
    while True:
        yield get_session(proxy)


def get_session(proxy):
    session = requesocks.session()
    session.proxies = {'http': proxy,
                       'https': proxy}
    session.headers.update({
        'User-Agent': random.choice(user_agents.get_user_agents())
    })
    return session


def create_folder(location):
    for i in itertools.count(1):
        folder_glob = os.path.join(location, 'download_%d_*' % i)
        if not glob.glob(folder_glob):
            folder_name = 'download_%d_%s' % (i, time.strftime('%Y%m%d_%H%M%S'))
            break
    folder_path = os.path.join(location, folder_name)
    return DownloadsFolder(folder_path)


class DownloadsFolder(object):
    def __init__(self, folder_path):
        self.good = os.path.join(folder_path, 'good')
        self.bad = os.path.join(folder_path, 'bad')
        self.names = os.path.join(folder_path, 'names')

        for folder in [self.good, self.bad, self.names]:
            os.makedirs(folder)


def roll_sansara_wheel(shard, downloads_folder, session_factory):
    while True:
        try:
            with gevent.Timeout(180):
                downloader(shard, downloads_folder, session_factory)
        except (Exception, gevent.Timeout):
            error("Downloader failed", exc_info=True)
            gevent.sleep(1)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--shard', type=int, help="Your user number. You will download files that give SHARD modulo TOTAL", default=0)
    parser.add_argument('--total', type=int, help="Total users", default=1)
    parser.add_argument('--proxy', type=str, nargs='+', help="Socks proxies to download files through")
    parser.add_argument('--loglevel', type=str, default='INFO',
                        choices=[k for k in logging._levelNames.values() if isinstance(k, basestring)],
                        help="Logging level")
    parser.add_argument('--dst-location', type=str, help="Folder where a folder will be created and downloaded files will be stored there", required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging._levelNames[args.loglevel])
    downloads_folder = create_folder(args.dst_location)
    downloaders = []
    for i, proxy in enumerate(args.proxy):
        session_factory = get_session_factory(proxy)
        shard = Shard(args.shard, args.total, i, len(args.proxy))
        downloaders.append(gevent.spawn(
            roll_sansara_wheel, shard,
            downloads_folder, session_factory)
        )
    while True:
        if os.getppid() == 1:
            info("Parent is dead, exiting")
            break
        gevent.sleep(1)
