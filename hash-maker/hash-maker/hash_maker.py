from multiprocessing.pool import ThreadPool, Lock
from hashlib import md5, sha1, sha256
from os.path import abspath, isfile
from time import sleep


#primary function sets up the output files if they dont exist, and reads the input into a pool, a threadsafe counter is used to keep the pool's queue to a memory friendly size, this function can be called and directly if file is imported
def main(file, output, type=None,threadcount=8,lock=None,verbose=False):
    pool = ThreadPool(threadcount)
    file = abspath(file)
    output = abspath(output)
    counter = Counter(1)
    if type == None:
        type= 'csv'
    elif 'csv' in type.lower():
        type =='csv'
    elif 'db' in type.lower() or 'sqlite' in type.lower():
        type == 'sqlite'
    else:
        if verbose:
            print('Type error')
        return -1
    if type!='csv':
        if verbose:
            print("writing to sqlite db at {0}".format(output))
        import sqlalchemy
    elif verbose:
        print("writing to csv file at {0}".format(output))
    if lock ==None:
        lock = Lock()
        if verbose:
            print("generating own threadlock")
    if not(isfile(output)):
            if type=='csv':
                with open(output) as outp:
                    outp.write("line,md5,sha1,sha256")
            else:
                engine = sqlalchemy.create_engine(r'sqlite:///'+output)
    
    with open(file) as source:
        for line in source:
            counter.increment()
            pool.apply_async(counter,processing,args=(line,lock,output,verbose,type,engine),callback=printresults)
            while counter.value() >100:
                sleep(1)
        pool.close()
        pool.join()

def processing(counter, line, lock, output, verbose, type, engine):
    md5result=md5(line.encode()).hexdigest()
    sha1result=sha1(line.encode()).hexdigest()
    sha256result=sha256(line.encode()).hexdigest()
    return counter, line, lock, output, verbose, type, md5result,sha1result,sha256result

def printresult(counter, line,lock, output, verbose, type, md5result,sha1result,sha256result):
    if verbose:
        print("results for {0}:".format(line))
        print("md5: {0}".format(md5results))
        print("sha1: {0}".format(sha1results))
        print("sha25: {0}".format(sha256results))
        print("print to {0} output".format(type))
    if type == 'csv':
        with lock:            
            with open(output) as outp:
                outp.write("{0},{1},{2},{3}".format(line,md5result,sha1result,sha256result))     
    counter.decrement()  
class Counter(object):
    def __init__(self, initval=0):
        self.val = mp.Value('i', initval)
        self.lock = mp.Lock()
    def increment(self):
        with self.lock:
            self.val.value += 1
    def decrement(self):
        with self.lock:
            self.val.value -= 1
    def value(self):
        with self.lock:
            return self.val.value
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-f","--file", help = "location of list of strings to hash", required=True)
    parser.add_argument("-tc","--threadcount", help = "number of lines to process in parallel, defaults to 8", default=8)
    parser.add_argument("-o","--output", help = "output location, including filename", required=True)
    parser.add_argument("-t","--type", help = "output format in csv or sqlite, defaults to csv", default='csv')
    parser.add_argument("-v","--verbose", help = "verbose flag", action='store_true')
    argsr = parser.parse_args()
    main(argsr.file,argrsr.output,argsr.type,argsr.threadcount,argsr.verbose)
