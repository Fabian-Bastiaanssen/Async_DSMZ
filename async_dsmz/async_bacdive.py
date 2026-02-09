# Extend the bacdive client to add multithreaded retrieval
from keycloak.exceptions import KeycloakAuthenticationError, KeycloakPostError, KeycloakConnectionError
import json
import aiohttp
import asyncio
import bacdive
class bacdive_async(bacdive.BacdiveClient):
    def __init__(self, user, password, public=True, max_retries=3, retry_delay=10, request_timeout=60000):
        super().__init__(user, password, public, max_retries, retry_delay, request_timeout)
        self.session = None
        self.conn = None
        self._lock = asyncio.Lock()
        self.sem = asyncio.Semaphore(50)


    def do_api_call(self, url):
        ''' Initialize API call on given URL and returns result as json '''
        if self.public:
            baseurl = "https://api.bacdive.dsmz.de/"
        else:
            baseurl = "http://api.bacdive-dev.dsmz.local/"
        
        if not url.startswith("http"):
            # if base is missing add default:
            url = baseurl + url
        resp = self.do_request(url)

        if resp.status_code == 500 or resp.status_code == 400 or resp.status_code == 503:
            print(f"Error {resp.status_code}: {resp.content}")
            return json.loads(resp.content)
        elif (resp.status_code == 401):
            msg = json.loads(resp.content)
            if msg['message'] == "Expired token":
                # Access token might have expired (15 minutes life time).
                # Get new tokens using refresh token and try again.
                try:
                    token = self.keycloak_openid.refresh_token(self.refresh_token)
                    self.access_token = token['access_token']
                    self.refresh_token = token['refresh_token']
                except Exception as e:
                    print(f"Error refreshing tokens: {e}")
                    return msg
                return self.do_api_call(url)
            return msg
        else:
            return json.loads(resp.content)
    async def get_session(self):
        async with self._lock:
            if self.conn is None:
                self.conn = aiohttp.TCPConnector(limit=200)
            if self.session is None:
                self.session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=60000),
                    connector=self.conn
                )
            return self.session
    async def refresh_tokens(self):
        try:
            token = self.keycloak_openid.refresh_token(self.refresh_token)
            self.access_token = token['access_token']
            self.refresh_token = token['refresh_token']
        except (KeycloakAuthenticationError, KeycloakPostError, KeycloakConnectionError) as e:
            raise e
        return token

    async def close(self):
        await self.session.close()
        await self.conn.close()
        self.session = None
        self.conn = None
        return self.session

    async def do_request_async(self, url):
        """Async HTTP GET with retry + token auth"""

        if self.predictions:
            if "?" in url:
                url += "&predictions=1"
            else:
                url += "?predictions=1"
        if self.session is None:
            try:
                self.session = await self.get_session()
            except Exception as e:
                print(f"Error getting session: {e}")
                return {}, {}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        async with self.sem:
            for attempt in range(1, self.max_retries + 1):
                headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}"}
                try:
                    async with self.session.get(url, headers=headers, timeout=timeout) as resp:
                        if resp.status == 401:
                            # refresh token
                            await self.refresh_tokens()
                            continue
                        
                        if resp.status in [429, 500, 502, 503, 504]:
                            # Retryable errors
                            await self.refresh_tokens()
                            # await self.session.close()
                            # self.session = await self.get_session()
                            await asyncio.sleep(2 ** attempt)
                            continue

                        # Return JSON for non-error statuses
                        data = await resp.json()
                        return resp, data

                except aiohttp.ClientError as e:
                    await self.refresh_tokens()
                    await asyncio.sleep(2 ** attempt)
                    print(f"Retrying {url}, attempt {attempt}")
                

        raise RuntimeError(f"Failed to GET {url} after {self.max_retries} retries {resp.status if 'resp' in locals() else 'No Response'}")
    async def do_api_call_async(self, url):
            baseurl = (
                "https://api.bacdive.dsmz.de/" if self.public else "http://api.bacdive-dev.dsmz.local/"
            )
            if not url.startswith("http"):
                url = baseurl + url
            try:
                resp, data = await self.do_request_async(url)
            except Exception as e:
                print(f"Error retrieving data from request_async {url}: {e}")
                # print(resp, data)
                return {}
            if resp.status in (500, 400, 503):
                print(f"Error {resp.status}: {data}")
                token = await self.refresh_tokens()
                self.access_token = token['access_token']
                self.refresh_token = token['refresh_token']
                return data
            elif (resp.status == 401):
                # Access token might have expired (15 minutes life time).
                # Get new tokens using refresh token and try again.
                token = await self.refresh_tokens()
                self.access_token = token['access_token']
                self.refresh_token = token['refresh_token']
                return await self.do_api_call_async(url)
            return data
    async def parse_entries_async(self, url):
        try:
            result = await self.do_api_call_async(url)
        except Exception as e:
            print(f"Error retrieving entries from {url}: {e}")
            return []
        try:
            if result.get('results') == []:
                return []
            ids = ";".join(str(i) for i in result['results'])
            entries = await self.do_api_call_async(f"fetch/{ids}")
            # print(f"Retrieved {len(entries['results'])} entries from {url}")
            # print(f"First entry: {entries['results'].keys()}")
            return [el for el in entries["results"].values()] if isinstance(entries.get("results"), dict) else []
        except Exception as e:
            print(f"Error parsing entries from {url}: {e}")
            return []

    
    async def retrieve_async(self, url=None):
        self.session = await self.get_session()
        # async with self.session as session:
        if url is not None:
            result = await self.do_api_call_async(url)
            try:
                num_jobs = (result['count'] -1) // 100
            except Exception as e:
                print(f"Error retrieving count from result: {e}")
                print(result)
                return []
            if num_jobs < 0:
                return []
            elif num_jobs == 0:
                ids = ";".join(str(i) for i in result['results'])
                entries = await self.do_api_call_async(f"fetch/{ids}")
                return [el for el in entries["results"].values()] if isinstance(entries.get("results"), dict) else []
            urls = [f"{url}?page={i}" for i in range(num_jobs + 1) ]
        else:
            # self.session = session  # temporarily assign for internal methods
            try:
                num_jobs = (self.result['count'] -1) // 100
                if num_jobs < 0:
                    num_jobs = 0
            except Exception as e:
                print(f"Error retrieving count from result: {e}")
                print(self.result)
                return []
            # prepare URLs for all pages
            if "?" in self.url:
                if self.predictions:
                    if num_jobs == 0:
                        urls = [f"{self.url}&predictions=1"]
                    else:
                        urls = [f"{self.url}&predictions=1&page={i}" for i in range(num_jobs + 1)]
                else:
                    if num_jobs == 0:
                        urls = [f"{self.url}"]
                    else:
                        urls = [f"{self.url}&page={i}" for i in range(num_jobs + 1)]
            else:
                if self.predictions:
                    if num_jobs == 0:
                        urls = [f"{self.url}?predictions=1"]
                    else:
                        urls = [f"{self.url}?predictions=1&page={i}" for i in range(num_jobs + 1)]
                else:
                    if num_jobs == 0:
                        urls = [f"{self.url}"]
                    else:
                        urls = [f"{self.url}?page={i}" for i in range(num_jobs + 1)]                
        try:
            tasks = [self.parse_entries_async(url) for url in urls]
        except Exception as e:
            print(f"Error creating tasks for URLs: {e}")
            print(num_jobs, urls)
        all_results = await asyncio.gather(*tasks, return_exceptions=True)

        # flatten the results
        return [item for sublist in all_results for item in sublist]
    def retrieve(self):
        async def runner():
            try:
                return await self.retrieve_async()
            finally:
                if self.session is not None:
                    await self.close()

        return asyncio.run(runner())
    def getIDByCultureno(self, culturecolnumber):
        ''' Initialize search by culture collection number '''
        item = culturecolnumber.strip()
        self.url = 'culturecollectionno/'+str(item)
        result = self.do_api_call('culturecollectionno/'+str(item))
        return result

    def getIDsByTaxonomy(self, genus, species_epithet=None, subspecies_epithet=None):
        ''' Initialize search by taxonomic names '''
        item = genus.strip()
        if species_epithet:
            item += "/" + species_epithet
            if subspecies_epithet:
                item += "/" + subspecies_epithet
        self.url = 'taxon/'+str(item)
        result = self.do_api_call("taxon/"+item)
        return result

    def getIDsBy16S(self, seq_acc_num):
        ''' Initialize search by 16S sequence accession '''
        item = seq_acc_num.strip()
        self.url = 'sequence_16s/'+str(item)
        result = self.do_api_call('sequence_16s/'+str(item))
        return result

    def getIDsByGenome(self, seq_acc_num):
        ''' Initialize search by genome sequence accession '''
        item = seq_acc_num.strip()
        self.url = 'sequence_genome/'+str(item)
        result = self.do_api_call('sequence_genome/'+str(item))
        return result
    async def async_search(self, **params):
        ''' Initialize search with *one* of the following parameters:
        
        id -- BacDive-IDs either as a semicolon seperated string or list
        taxonomy -- Taxonomic names either as string or list
        sequence -- Sequence accession number of unknown type
        genome -- Genome sequence accession number
        16s -- 16S sequence accession number
        culturecolno -- Culture collection number (mind the space!)
        '''
        params = list(params.items())
        allowed = ['id', 'taxonomy', 'sequence',
                   'genome', '16s', 'culturecolno']
        if len(params) != 1:
            print(
                "ERROR: Exacly one parameter is required. Please choose one of the following:")
            print(", ".join(allowed))
            return 0
        querytype, query = params[0]
        querytype = querytype.lower()
        if querytype not in allowed:
            print(
                "ERROR: The given query type is not allowed. Please choose one of the following:")
            print(", ".join(allowed))
            return 0
        if querytype == 'id':
            if type(query) == type(1):
                query = str(query)
            if type(query) == type(""):
                query = query.split(';')
            self.result = {'count': len(query), 'next': None,
                           'previous': None, 'results': query}
        elif querytype == 'taxonomy':
            if type(query) == type(""):
                query = [i for i in query.split(" ") if i != "subsp."]
            if len(query) > 3:
                print("Your query contains more than three taxonomical units.")
                print(
                    "This query supports only genus, species epithet (optional), and subspecies (optional).")
                print("They can be defined as list, tuple or string (space separated).")
                return 0
            self.result = await self.async_getIDsByTaxonomy(*query)
        elif querytype == 'sequence':
            query = self.parseSearchTypeQuery(query)
            self.result = await self.async_getIDsByGenome(query)
            if self.result['count'] == 0:
                self.result = await self.async_getIDsBy16S(query)
        elif querytype == 'genome':
            query = self.parseSearchTypeQuery(query)
            self.result = await self.async_getIDsByGenome(query)
        elif querytype == '16s':
            query = self.parseSearchTypeQuery(query)
            self.result = await self.async_getIDsBy16S(query)
        elif querytype == 'culturecolno':
            query = self.parseSearchTypeQuery(query)
            self.result = await self.async_getIDByCultureno(query)

        if not self.result:
            print("ERROR: Something went wrong. Please check your query and try again")
            return 0
        if not 'count' in self.result:
            print("ERROR:", self.result.get("title"))
            print(self.result.get("message"))
            return 0
        if self.result['count'] == 0:
            # print("Your search did not receive any results.")
            return 0
        return self.result['count']
    async def async_getIDByCultureno(self, culturecolnumber):
        ''' Initialize search by culture collection number '''
        item = culturecolnumber.strip()
        self.url = 'culturecollectionno/'+str(item)
        result = await self.do_api_call_async('culturecollectionno/'+str(item))
        return result

    async def async_getIDsByTaxonomy(self, genus, species_epithet=None, subspecies_epithet=None):
        ''' Initialize search by taxonomic names '''
        item = genus.strip()
        if species_epithet:
            item += "/" + species_epithet
            if subspecies_epithet:
                item += "/" + subspecies_epithet
        self.url = 'taxon/'+str(item)
        result = await self.do_api_call_async("taxon/"+item)
        return result

    async def async_getIDsBy16S(self, seq_acc_num):
        ''' Initialize search by 16S sequence accession '''
        item = seq_acc_num.strip()
        self.url = 'sequence_16s/'+str(item)
        result = await self.do_api_call_async('sequence_16s/'+str(item))
        return result

    async def async_getIDsByGenome(self, seq_acc_num):
        ''' Initialize search by genome sequence accession '''
        item = seq_acc_num.strip()
        self.url = 'sequence_genome/'+str(item)
        result = await self.do_api_call_async('sequence_genome/'+str(item))
        return result
