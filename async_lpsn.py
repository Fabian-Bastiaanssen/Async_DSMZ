# Extend the lpsn client to add multithreaded retrieval
from keycloak.exceptions import KeycloakAuthenticationError, KeycloakPostError, KeycloakConnectionError
import json
import aiohttp
import asyncio
import lpsn
class lpsn_async(lpsn.LpsnClient):
    def __init__(self, user, password, public=True, max_retries=10, retry_delay=50, request_timeout=300, config=None):
        super().__init__(user, password, public, max_retries, retry_delay, request_timeout)
        self.session = None
        self.config = config
            
    async def get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
        return self.session

    def search(self, **params):
        ''' Initialize search with parameters
        '''
        if 'id' in params:
            query = params['id']
            if type(query) == type(1):
                query = str(query)
            if type(query) == type(""):
                query = query.split(';')
            self.result = {'count': len(query), 'next': None,
                           'previous': None, 'results': query}
            return self.result['count']

        query = []
        for k, v in params.items():
            k = k.replace("_", "-")
            if v == True:
                v = "yes"
            elif v == False:
                v = "no"
            else:
                v = str(v)
            query.append(k + "=" + v)
        self.result = self.do_api_call('advanced_search?'+'&'.join(query))
        # we need to store the URL for later retrieval
        self.url = 'advanced_search?'+'&'.join(query)
        if not self.result:
            print("ERROR: Something went wrong. Please check your query and try again")
            return 0
            
        if not 'count' in self.result:
            print("ERROR:", self.result.get("title"))
            print(self.result.get("message"))
            return 0
            
        if self.result['count'] == 0:
            print("Your search did not receive any results.")
            return 0
            
        return self.result['count']
    def do_api_call(self, url):
        ''' Initialize API call on given URL and returns result as json '''
        if self.public:
            baseurl = "https://api.lpsn.dsmz.de/"
        else:
            baseurl = "http://api.pnu-dev.dsmz.local/"
        
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
                except (KeycloakAuthenticationError, KeycloakPostError, KeycloakConnectionError) as e:
                    print(f"Error refreshing tokens: {e}")
                    return msg
                return self.do_api_call(url)
            return msg
        else:
            return json.loads(resp.content)

    def flex_search(self, search, negate=False):
        ''' Initialize flexible search with parameters
        '''
       
        if not search:
            print("You must enter search parameters.")
            return 0
        
        param_str = '?search='+json.dumps(search)
        if negate:
            param_str += '&not=yes'

        
        self.result = self.do_api_call('flexible_search'+param_str)
        # we need to store the URL for later retrieval
        self.url = 'flexible_search'+param_str
        if not self.result:
            print("ERROR: Something went wrong. Please check your query and try again")
            return 0
            
        if not 'count' in self.result:
            print("ERROR:", self.result.get("title"))
            print(self.result.get("message"))
            return 0
            
        if self.result['count'] == 0:
            print("Your search did not receive any results.")
            return 0
            
        return self.result['count']

    async def refresh_tokens(self):
        try:
            token = self.keycloak_openid.refresh_token(self.refresh_token)
            self.access_token = token['access_token']
            self.refresh_token = token['refresh_token']
        except (KeycloakAuthenticationError, KeycloakPostError, KeycloakConnectionError) as e:
            raise e
    async def close(self):
        await self.session.close()

    async def do_request_async(self, url):
        """Async HTTP GET with retry + token auth"""
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        
        self.session = await self.get_session()
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.session.get(url, headers=headers, timeout=timeout) as resp:
                    if resp.status == 401:
                        # refresh token
                        await self.refresh_tokens()
                        continue
                    
                    if resp.status in [429, 500, 502, 503, 504]:
                        # Retryable errors
                        await asyncio.sleep(2 ** attempt)
                        continue

                    # Return JSON for non-error statuses
                    data = await resp.json()
                    return resp, data

            except aiohttp.ClientError:
                await asyncio.sleep(2 ** attempt)

        raise RuntimeError(f"Failed to GET {url} after {self.max_retries} retries")
    async def do_api_call_async(self, url):
        baseurl = (
            "https://api.lpsn.dsmz.de/" if self.public else "http://api.pnu-dev.dsmz.local/"
        )
        if not url.startswith("http"):
            url = baseurl + url

        resp, data = await self.do_request_async(url)
        if resp.status in (500, 400, 503):
            print(f"Error {resp.status}: {data}")
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
        result = await self.do_api_call_async(url)
        try:
            if result.get('results') == []:
                return []
            ids = ";".join(str(i) for i in result['results'])
            entries = await self.do_api_call_async(f"fetch/{ids}")
            return [el for el in entries['results']]
        except Exception as e:
            print(f"Error parsing entries from {url}: {e}")
            return []

    
    async def retrieve_async(self):
        async with aiohttp.ClientSession() as session:
            self.session = session  # temporarily assign for internal methods
            num_jobs = self.result['count'] // 100
            # prepare URLs for all pages
            # if &not=yes is in the URL, we need to keep it at the end
            if '&not=yes' in self.url:
                translation = {'{':'%7B', '}':'%7D', '"':'%22', ' ':'+', ':':'%3A', ',':'%2C', '[':'%5B', ']':'%5D'}
                base_url = self.url.replace('&not=yes', '').translate(str.maketrans(translation))
                urls = [f"{base_url}&not=yes&page={i}" for i in range(num_jobs + 1)]
            else:
                urls = [f"{self.url}&page={i}" for i in range(num_jobs + 1)] 
            tasks = [self.parse_entries_async(url) for url in urls]
            all_results = await asyncio.gather(*tasks)

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