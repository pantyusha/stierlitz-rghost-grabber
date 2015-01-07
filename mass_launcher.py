#!/usr/bin/env python

if __name__ == '__main__':
    import argparse, subprocess, time, sys, os
    dirname = os.path.dirname(os.path.abspath(sys.argv[0]))
    multi_tor_folder = os.path.join(dirname, 'Multi-TOR')

    parser = argparse.ArgumentParser()
    parser.add_argument('--threads', help='Number of tor instances through which you should download', default=1, type=int)
    args, unknown = parser.parse_known_args()
    proxies = []
    subprocess.check_call(['./multi-tor.sh', str(args.threads)], cwd=multi_tor_folder)
    for i in xrange(args.threads):
        proxies.append('socks5://127.0.0.1:%d' % (9051 + i))
    p = subprocess.Popen([os.path.join(dirname, 'rghost.py')] + unknown + ['--proxy'] + proxies)

  
    last_update = time.time()
    while True:
        time.sleep(1)
        if time.time() - last_update > 30:
            subprocess.check_call(['./tor_newid.sh', str(args.threads)], cwd=multi_tor_folder)
            last_update = time.time() 
        if p.poll() is not None:
            print "Downloader exited with exit code", p.poll()
            break
