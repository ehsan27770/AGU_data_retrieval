import GOES

import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import calendar
from datetime import datetime, timedelta
from tqdm import tqdm
import time
import os
import argparse
import yaml
# %% tests
#GOES.download('goes16', 'ABI-L2-RRQPEF', DateTimeIni = '20200701-000000', DateTimeFin = '20200701-235959', channel = ['13'], path_out='./ABI/')


#GOES.download('goes16', 'GLM-L2-LCFA', DateTimeIni = '20200701-000000', DateTimeFin = '20200701-235959', channel = ['13'], path_out='./GLM/')




#GOES.download('goes16', 'ABI-L2-CPSF', DateTimeIni = '20200701-000000', DateTimeFin = '20200701-235959', channel = ['13'], path_out='./ABI/')

# %% helper functions to parallelize
class GOESParallelDownloader(object):
    """docstring for GOESParallelDownloader."""

    def __init__(self, satellite, product, DateTimeIni, DateTimeFin, channel, path_out):
        super(GOESParallelDownloader, self).__init__()
        self.satellite = satellite
        self.product = product
        self.DateTimeIni = DateTimeIni
        self.DateTimeFin = DateTimeFin
        self.channel = channel
        self.path_out = path_out

    @staticmethod
    def time_str2num(input):
        return datetime.strptime(input,'%Y%m%d-%H%M%S')

    @staticmethod
    def time_num2str(num):
        assert type(num) == datetime
        return num.strftime('%Y%m%d-%H%M%S')


    def time_slicer(self, step):
        start = self.time_str2num(self.DateTimeIni)
        end = self.time_str2num(self.DateTimeFin)

        slices = []
        while True:
            slices.append(start)
            if start + step < end:
                start = start + step
            else:
                slices.append(end)
                break
        return list(map(self.time_num2str,slices))

    def job_distributer(self,step):
        slices = self.time_slicer(step)
        #with ProcessPoolExecutor() as executer:
        #    futures = {executer.submit(GOES.download,self.satellite,self.product,DateTimeIni = start_batch, DateTimeFin = end_batch,channel=self.channel,path_out=self.path_out):idx for idx,(start_batch,end_batch) in enumerate(zip(slices[:-1],slices[1:]))}

        with tqdm(total = len(slices)-1) as pbar:
            with ProcessPoolExecutor() as executer:
                futures = {executer.submit(GOES.download,self.satellite,self.product,DateTimeIni = start_batch, DateTimeFin = end_batch,channel=self.channel,path_out=self.path_out,show_download_progress=False):idx for idx,(start_batch,end_batch) in enumerate(zip(slices[:-1],slices[1:]))}
                results = {}
                for future in concurrent.futures.as_completed(futures):
                    arg = futures[future]
                    results[arg] = future.result()
                    pbar.update(1)
        return results


if __name__ ==  '__main__':
    '''
    rsync -r data_retrieval semansou@fdata1.epfl.ch:/home/semansou/code
    source venvs/tf/bin/activate
    python code/data_retrieval/downloader.py --output /work/sci-sti-fr/semansou/GOES_data_download_automatic/ --config code/data_retrieval/config.yaml
    '''
    parser = argparse.ArgumentParser("data downloader")
    #parser.add_argument("--year", help = "year to download data")
    #parser.add_argument("--month", help = "month to download data")
    parser.add_argument("--output", help = "output folder")
    parser.add_argument("--config", help = "config file")
    args = parser.parse_args()
    #year = int(args.year)
    #month = int(args.month)
    output = args.output
    config = args.config

    with open(config,'r') as f:
        to_download = yaml.load(f, Loader=yaml.SafeLoader)
    
    for group in to_download:
        for param in group['params']:
            if param == 'GLM-L2-LCFA':
                folder = 'GLM/'
            else:
                folder = 'ABI/'
            for year in group['years']:
                for month in group['months']:
                    start_download = GOESParallelDownloader.time_num2str(datetime(year = year, month = month, day = 1))
                    finish_download = GOESParallelDownloader.time_num2str(datetime(year = year, month = month+1, day = 1))
                    t0 = time.perf_counter()
                    a = GOESParallelDownloader('goes16', param, DateTimeIni = start_download, DateTimeFin = finish_download, channel = ['13'], path_out=os.path.join(output,folder))
                    a.job_distributer(timedelta(hours=1))
                    t1 = time.perf_counter()
                    print(f'time to retrieve {param} = {t1-t0} seconds = {timedelta(seconds=t1-t0)}')

    #start_download = GOESParallelDownloader.time_num2str(datetime(year = 2020, month = 7, day = 1))
    #finish_download = GOESParallelDownloader.time_num2str(datetime(year = 2020, month = 8, day = 1))

    # start_download = GOESParallelDownloader.time_num2str(datetime(year = year, month = month, day = 1))
    # finish_download = GOESParallelDownloader.time_num2str(datetime(year = year, month = month+1, day = 1))

    # t0 = time.perf_counter()

    # a = GOESParallelDownloader('goes16', 'ABI-L2-CPSF', DateTimeIni = start_download, DateTimeFin = finish_download, channel = ['13'], path_out=os.path.join(output,'ABI/'))
    # a.job_distributer(timedelta(hours=1))

    # t1 = time.perf_counter()
    # print(f'time to retrieve cloud particle size = {t1-t0} seconds = {timedelta(seconds=t1-t0)}')

    # a = GOESParallelDownloader('goes16', 'ABI-L2-RRQPEF', DateTimeIni = start_download, DateTimeFin = finish_download, channel = ['13'], path_out=os.path.join(output,'ABI/'))
    # a.job_distributer(timedelta(hours=1))

    # t2 = time.perf_counter()
    # print(f'time to retrieve rainfall = {t2-t1} seconds = {timedelta(seconds=t2-t1)}')

    # a = GOESParallelDownloader('goes16', 'GLM-L2-LCFA', DateTimeIni = start_download, DateTimeFin = finish_download, channel = ['13'], path_out=os.path.join(output,'GLM/'))
    # a.job_distributer(timedelta(hours=1))

    # t3 = time.perf_counter()
    # print(f'time to retrieve GLM data = {t3-t2} seconds = {timedelta(seconds=t3-t2)}')

    # print(f'{t1-t0} \t {timedelta(seconds=t1-t0)}\n{t2-t1} \t {timedelta(seconds=t2-t1)}\n{t3-t2} \t {timedelta(seconds=t3-t2)}\n')
