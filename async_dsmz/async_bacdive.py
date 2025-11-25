# Extend the bacdive client to add multithreaded retrieval
from keycloak.exceptions import KeycloakAuthenticationError, KeycloakPostError, KeycloakConnectionError
import json
import aiohttp
import asyncio
import bacdive
class bacdive_async(bacdive.BacdiveClient):
    def __init__(self, user, password, public=True, max_retries=10, retry_delay=50, request_timeout=300):
        super().__init__(user, password, public, max_retries, retry_delay, request_timeout)
        self.session = None
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
                except (KeycloakAuthenticationError, KeycloakPostError, KeycloakConnectionError) as e:
                    print(f"Error refreshing tokens: {e}")
                    return msg
                return self.do_api_call(url)
            return msg
        else:
            return json.loads(resp.content)
    async def get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
        return self.session
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
        if self.predictions:
            if "?" in url:
                url += "&predictions=1"
            else:
                url += "?predictions=1"
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
                "https://api.bacdive.dsmz.de/" if self.public else "http://api.bacdive-dev.dsmz.local/"
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
            # print(f"Retrieved {len(entries['results'])} entries from {url}")
            # print(f"First entry: {entries['results'].keys()}")
            return [el for el in entries['results'].values()]
        except Exception as e:
            print(f"Error parsing entries from {url}: {e}")
            return []

    
    async def retrieve_async(self):
        async with aiohttp.ClientSession() as session:
            self.session = session  # temporarily assign for internal methods
            num_jobs = self.result['count'] // 100
            # prepare URLs for all pages
            if "?" in self.url:
                if self.predictions:
                    urls = [f"{self.url}&predictions=1&page={i}" for i in range(num_jobs + 1)]
                else:
                    urls = [f"{self.url}&page={i}" for i in range(num_jobs + 1)]
            else:
                if self.predictions:
                    urls = [f"{self.url}?predictions=1&page={i}" for i in range(num_jobs + 1)]
                else:
                    urls = [f"{self.url}?page={i}" for i in range(num_jobs + 1)]                

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