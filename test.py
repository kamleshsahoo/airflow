import aiohttp, logging, pendulum, asyncio, time, math, sys #aiohttp_socks, random,
from pyspark.sql import SparkSession
from lxml import html
#import pandas as pd
#import socket, ssl, certifi #, resource
from aiolimiter import AsyncLimiter

LOGGER_FROMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=LOGGER_FROMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger()
log.setLevel(logging.INFO)
'''
class scrap_etRev:
    def __init__(self, start_date = '2008-01-01', end_date = None, sleep=1/10, sem_limit=100):
        self.sleep = sleep
        self.sem = asyncio.Semaphore(sem_limit)
        self.limiter = AsyncLimiter(1,0.05)
        self.conn = aiohttp.TCPConnector(limit_per_host=5, family=socket.AF_INET, ssl=False)
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.base_date = pendulum.from_format('2008-01-01', 'YYYY-MM-DD') 
        self.base_cms = 39448
        self.start_date = pendulum.from_format(start_date, 'YYYY-MM-DD')
        assert self.start_date >= self.base_date, "start date cannot be smaller than '2008-01-01' !!"
        self.cms =  self.base_cms
        if end_date!=None:
            self.end_date = pendulum.from_format(end_date, 'YYYY-MM-DD')
        else:
            self.end_date = pendulum.now(tz='UTC')
        
        if self.start_date != self.base_date:
            self.cms += self.start_date.diff(self.base_date).in_days()
        
        self.period = pendulum.period(self.start_date , self.end_date )
        self.data = []
        self.url_dict = {}
        self.href_dict = {'Dt':[], 'href':[]}
        self.results = {'Dt':[], 'href':[], 'news':[]}
        self.base_url = "https://economictimes.indiatimes.com"
        self.template_url = "https://economictimes.indiatimes.com/archivelist/year-{},month-{},starttime-{}.cms"
        self.spark = SparkSession.builder.getOrCreate()
        self.create_urls()

    def create_urls(self):
        for dt in self.period.range('days'):
            url = self.template_url.format(dt.year, dt.month, self.cms)
            self.url_dict[dt.to_date_string()] = url
            self.cms += 1

    async def make_href_request(self, url_element):
        async with self.limit:
            #connector=aiohttp.TCPConnector(limit=5)
            async with aiohttp.ClientSession() as session:   #verify_ssl=False, family=socket.AF_INET), trust_env=True)
                async with session.get(url_element[1]) as response:   #, ssl=self.ssl_context
                    resp = await response.content.read()
                    log.info(f'***Made href request: {url_element[0]}')
                    tree = html.fromstring(resp)
                    tags = tree.xpath("//a[contains(@href, '/markets') and not(contains(@href, '/markets/stocks/recos'))]")
                    if len(tags)==0:
                        print('no tags found')
                    else:
                        for tag in tags:
                            if len(tag.attrib)==1:
                                #print('single tag attrib (should hv req href): ', tag.attrib)
                                self.href_dict['Dt'].append(url_element[0])
                                self.href_dict['href'].append(self.base_url + tag.attrib['href'])
                            # else:
                            #     print('more than 1 tag attribs (shouldnt hv req href): ',tag.attrib)
                    await asyncio.sleep(1/10)

    async def make_news_request(self, href_element):
        async with asyncio.Semaphore(100):     
            async with aiohttp.ClientSession(connector=self.conn) as session: #connector=aiohttp.TCPConnector(limit_per_host=3, ssl=False, family=socket.AF_INET), trust_env=True
                async with session.get(href_element[1]) as response:          #, ssl=self.ssl_context
                    resp = await response.content.read()
                    log.info(f'===Made news request: {href_element[0]}')
                    news = self.parse_html(resp)
                    self.results['Dt'].append(href_element[0])
                    self.results['href'].append(href_element[1])
                    self.results['news'].append(news)
                    await asyncio.sleep(1/20)

    def parse_html(self, html_doc):
        tree = html.fromstring(html_doc)
        div_elements = tree.cssselect('div.artText, div.artSyn') 
        news = [str(div.text_content()) for div in div_elements if div.text_content().strip()]  #filter empty news
        return news

    async def make_allrequests(self, func, itr):
        tasks = [func(element) for element in itr] #self.url_dict.items(), self.make_href_request(url_element))
        await asyncio.gather(*tasks)
    
    async def get_news(self):
        start_time = time.perf_counter()
        await self.make_allrequests(self.make_href_request, self.url_dict.items()) 
        print('[INFO]href task completed and took {:.2f}s'.format(time.perf_counter()-start_time))
        start_time = time.perf_counter()
        await self.make_allrequests(self.make_news_request, zip(self.href_dict['Dt'], self.href_dict['href']))
        print('[INFO]news task completed and took {:.2f}s'.format(time.perf_counter()-start_time))
        #change to df
        return pd.DataFrame(self.results)

class scrap_etRev1:
    def __init__(self, start_date = '2008-01-01', end_date = None, sleep=2, limit=2):
        self.sleep = sleep
        self.limit = asyncio.Semaphore(limit)
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.base_date = pendulum.from_format('2008-01-01', 'YYYY-MM-DD') 
        self.base_cms = 39448
        self.start_date = pendulum.from_format(start_date, 'YYYY-MM-DD')
        assert self.start_date >= self.base_date, "start date cannot be smaller than '2008-01-01' !!"
        self.cms =  self.base_cms
        if end_date!=None:
            self.end_date = pendulum.from_format(end_date, 'YYYY-MM-DD')
        else:
            self.end_date = pendulum.now(tz='UTC')
        
        if self.start_date != self.base_date:
            self.cms += self.start_date.diff(self.base_date).in_days()
        
        self.period = pendulum.period(self.start_date , self.end_date )
        self.data = []
        self.url_dict = {}
        self.href_dict = {'Dt':[], 'href':[]}
        self.results = {'Dt':[], 'href':[], 'news':[]}
        self.base_url = "https://economictimes.indiatimes.com"
        self.template_url = "https://economictimes.indiatimes.com/archivelist/year-{},month-{},starttime-{}.cms"
        self.spark = SparkSession.builder.getOrCreate()
        self.create_urls()

    def create_urls(self):
        for dt in self.period.range('days'):
            url = self.template_url.format(dt.year, dt.month, self.cms)
            self.url_dict[dt.to_date_string()] = url
            self.cms += 1

    async def get_hrefs(self, session, date, url):
        async with self.limit:
            async with session.get(url) as response:   #, ssl=self.ssl_context
                resp = await response.content.read()
                #log.info(f'***Made href request: {url}')
                tree = html.fromstring(resp)
                tags = tree.xpath("//a[contains(@href, '/markets') and not(contains(@href, '/markets/stocks/recos'))]")
                if len(tags)==0:
                    print('no tags found')
                else:
                    for tag in tags:
                        if len(tag.attrib)==1:
                            self.href_dict['Dt'].append(date) 
                            self.href_dict['href'].append(self.base_url + tag.attrib['href'])
                            # self.href_dict['Dt'].append(url_element[0])
                            # self.href_dict['href'].append(self.base_url + tag.attrib['href'])

    async def get_news(self, session, date, href):
        async with self.limit:
            async with session.get(href) as response:   #, ssl=self.ssl_context
                resp = await response.content.read()
                #log.info(f'===Made news request: {href}')
                self.results['Dt'].append(date)
                self.results['href'].append(href)
                self.results['news'].append(self.parse_html(resp))
                # self.results['Dt'].append(href_element[0])
                # self.results['href'].append(href_element[1])
                # self.results['news'].append(news)

    def parse_html(self, html_doc):
        tree = html.fromstring(html_doc)
        div_elements = tree.cssselect('div.artText, div.artSyn') 
        news = [str(div.text_content()) for div in div_elements if div.text_content().strip()]  #filter empty news
        return news
    
    async def get_allnews(self):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=5)) as session:  #, verify_ssl=False, family=socket.AF_INET), trust_env=True)
            href_tasks = [self.get_hrefs(session, date=row[0], url=row[1]) for row in self.url_dict.items()]
            start_time = time.perf_counter()
            await asyncio.gather(*href_tasks)
            print('[INFO]href task completed and took {:.2f}s'.format(time.perf_counter()-start_time))
            news_tasks = [self.get_news(session, dt, href) for dt, href in zip(self.href_dict['Dt'], self.href_dict['href'])]
            start_time = time.perf_counter()
            await asyncio.gather(*news_tasks)
            print('[INFO]news task completed and took {:.2f}s'.format(time.perf_counter()-start_time))
            return pd.DataFrame(self.results)
'''
class SCRAP_ET:
    def __init__(self, start_date = '2008-01-01', end_date = None, href_limit=0.2, news_limit=0.02, filename='ET_News'):
        self.href_limiter = AsyncLimiter(1, href_limit)
        self.news_limiter = AsyncLimiter(1, news_limit)
        self.href_cntr, self.news_cntr = 0, 0
        # self.proxy_list = ['socks5://dante-ks:Net2023@0.0.0.0:112', 'socks5://dante-ks:Net2023@0.0.0.0:115', 
        #                 'socks5://dante-ks:Net2023@0.0.0.0:209', 'socks5://dante-ks:Net2023@0.0.0.0:567',
        #                 'socks5://dante-ks:Net2023@0.0.0.0:20000', 'socks5://dante-ks:Net2023@0.0.0.0:455',
        #                 'socks5://dante-ks:Net2023@0.0.0.0:1920', 'socks5://dante-ks:Net2023@0.0.0.0:1515',
        #                 'socks5://dante-ks:Net2023@0.0.0.0:21', 'socks5://dante-ks:Net2023@0.0.0.0:2007']
        #self.conn = aiohttp.TCPConnector(limit_per_host=5, family=socket.AF_INET, ssl=False)
        #self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.base_date = pendulum.from_format('2008-01-01', 'YYYY-MM-DD') 
        self.base_cms = 39448
        self.start_date = pendulum.from_format(start_date, 'YYYY-MM-DD')
        assert self.start_date >= self.base_date, "start date cannot be smaller than '2008-01-01' !!"
        self.cms =  self.base_cms
        if end_date!=None:
            self.end_date = pendulum.from_format(end_date, 'YYYY-MM-DD')
        else: 
            self.end_date = pendulum.now(tz='UTC')
            print('[INFO]end date set to ', self.end_date.to_date_string())
        
        if self.start_date != self.base_date:
            self.cms += self.start_date.diff(self.base_date).in_days()
    
        self.filename = filename #+ '(' + self.start_date.format('DD/MM/YY') + ' to ' + self.end_date.format('DD/MM/YY') + ')'
        self.period = pendulum.period(self.start_date , self.end_date )
        self.url_dict = {}
        self.href_dict = {'Dt':[], 'href':[]}
        self.results = {'Dt':[], 'href':[], 'news':[]}
        self.base_url = "https://economictimes.indiatimes.com"
        self.template_url = "https://economictimes.indiatimes.com/archivelist/year-{},month-{},starttime-{}.cms"
        self.spark = SparkSession.builder.getOrCreate()
        self.create_urls()

    def create_urls(self):
        for dt in self.period.range('days'):
            url = self.template_url.format(dt.year, dt.month, self.cms)
            self.url_dict[dt.to_date_string()] = url
            self.cms += 1

    def parse_html(self, html_doc):
        tree = html.fromstring(html_doc)
        div_elements = tree.cssselect('div.artText, div.artSyn') 
        news = [str(div.text_content()) for div in div_elements if div.text_content().strip()]  #filter empty news
        return news
        
    async def scrap_href(self, session, url_element):
        async with session.get(url_element[1]) as response:   
            resp = await response.content.read()
            self.href_cntr += 1
            log.info(f'***Made href req {self.href_cntr}: {url_element[0]}')
            #self.sem.release()
            tree = html.fromstring(resp)
            tags = tree.xpath("//a[contains(@href, '/markets') and not(contains(@href, '/markets/stocks/recos'))]")
            if len(tags)==0:
                print('no tags found')
            else:
                for tag in tags:
                    if len(tag.attrib)==1:
                        self.href_dict['Dt'].append(url_element[0])
                        self.href_dict['href'].append(self.base_url + tag.attrib['href'])

    async def get_hrefs(self):
        async with self.href_limiter:
            async with aiohttp.ClientSession() as session:
                href_tasks = [asyncio.create_task(self.scrap_href(session,url_element)) for url_element in self.url_dict.items()]
                await asyncio.gather(*href_tasks)

    async def scrap_news(self, session, href_element):
        async with session.get(href_element[1]) as response:
            resp = await response.content.read()
            self.news_cntr += 1
            log.info(f'===Made news req {self.news_cntr}: {href_element[0]}')
            news = self.parse_html(resp)
            self.results['Dt'].append(href_element[0])
            self.results['href'].append(href_element[1])
            self.results['news'].append(news)

    async def get_news(self):
        async with self.news_limiter:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
                news_tasks = [asyncio.create_task(self.scrap_news(session,href_element)) for href_element in zip(*self.href_dict.values())] #zip(self.href_dict['Dt'], self.href_dict['href'])]
                await asyncio.gather(*news_tasks)

    def clean_results(self):
        cleaned_results = []
        start_time = time.perf_counter()
        for element in zip(*self.results.values()):#self.results['Dt'], self.results['href'], self.results['news']):
            element = list(element)
            if len(element[2]) == 0:
                #print('got element {} with news of length 0'.format(element))
                continue
            # Check if the 'news' field has only one element
            elif len(element[2]) == 1:
                synopsis = element[2][0][8:].strip()
                element[2] = [synopsis, synopsis]
            else:
                element[2][0] = element[2][0][8:].strip()
                element[2][1] = element[2][0] if element[2][1]=='ERR-RTF' else ' '.join(element[2][1].split())     #element['news'][1].strip().replace('\n', '')
            cleaned_results.append({'Dt':element[0], 'href':element[1], 'news':element[2]})
        print('[INFO]cleaned the results which took: {:2f}s'.format(time.perf_counter()-start_time))
        print('[INFO]total news items after cleaning: ',len(cleaned_results))
        start_time = time.perf_counter()
        rdd = self.spark.sparkContext.parallelize(cleaned_results, numSlices=math.ceil(sys.getsizeof(cleaned_results)/1024)) #limit size to 1KiB (=1024bytes) per slice
        results_df = rdd.toDF()
        results_df = results_df.select(['Dt', 'href'] + [results_df.news[idx].alias(col_name) for idx, col_name in enumerate(['Synopsis', 'News'])])
        results_df.write.option("header",True).csv(self.filename)
        print('[INFO]created a spark DF and saved the results as csv in {}, which took: {:.2f}s'.format(self.filename, time.perf_counter()-start_time))

    async def run(self):
        start_time = time.perf_counter()
        await self.get_hrefs() 
        print('[INFO]scraping hrefs completed which took {:.2f}s'.format(time.perf_counter()-start_time))
        print('[INFO]total hrefs: ',len(self.href_dict['href'])) #href_df (233011, 2)- (1/0.1-32.8s, 1/0.03-34.4, )
        print('[INFO]now sleeping for 2 seconds..')
        time.sleep(2)
        start_time = time.perf_counter()
        await self.get_news()
        print('[INFO]scraping news completed which took {:.2f}s'.format(time.perf_counter()-start_time))
        print('[INFO]total news items: ',len(self.results['news']))
        self.clean_results()

async def main():
    scrap_et = SCRAP_ET(start_date='2012-01-01', href_limit=0.125, news_limit=0.01, filename='ET_News') 
    await scrap_et.run()

if __name__== "__main__":
    asyncio.run(main())