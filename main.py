from bs4 import BeautifulSoup
import requests
import time
import os
from tqdm import tqdm
import html
import re
from datetime import datetime
from multiprocessing import Manager, Pool
import pickle
import pandas as pd


"""
You'll need to prepopulate this section with your own cookies and headers.

If you don't know how to do this:
    1. Go on Firefox/Chrome
    2. Open up the network tab
    3. Login to your Heritage Auctions account
    4. load up some search results
    5. Right click the request and `Copy as CURL`
"""
COOKIES = {
}

HEADERS = {
}


def params_for_page(n):
    """
    Max pagination size is 204, so increments of 204 required.

    Function returns tuple containing the search params + the offset.
    """
    return (
        ('No', n),
        ('N', '790 231 52 6437'),
    )


for n in range(0, 9598, 204):
    """
    Simple loop to get all the search results, we need to grab the link for each listing.
    """
    params = params_for_page(n)
    response = requests.get('https://comics.ha.com/c/search-results.zx', headers=COOKIES, params=params, cookies=COOKIES)

    with open(f'./dumps/{n}.html', 'w') as file:
        file.write(response.text)

    time.sleep(0.5)


"""
Create an array containing all our links.
"""
links = []
for filepath in os.listdir('./dumps'):
    with open(os.path.join('dumps', filepath), 'r') as file:
        data = file.read()
        soup = BeautifulSoup(data, 'html.parser')
        auction_items = soup.find('ul', {'class': 'auction-items'})
        for div in auction_items.find_all('div', {'class': 'current-amount'}):
            for link in div.find_all('a'):
                if link.text == 'Click to view amount':
                    links.append(link.attrs.get('href'))


"""
Time to loop through all the links we have, I've set a 0.5 second delay to avoid being throttled/blocked.
"""
base_url = 'https://comics.ha.com'
base_dir = './dumps'
for link in tqdm(links):
    response = requests.get(base_url + link, headers=HEADERS, params=params, cookies=COOKIES)
    if response.status_code != 200:
        raise Exception('Non-200 response')
    with open(os.path.join('./dumps', f'{link}.html'), 'w') as file:
        file.write(response.text)
    time.sleep(0.5)


"""
Don't forget to create folders.
"""
base_dir = './dumps/c/'

manager = Manager()
sales = manager.list()

def process_file(filepath):
    try:
        with open(os.path.join(base_dir, filepath)) as file:
            data = file.read()
            soup = BeautifulSoup(data, 'html.parser')

            # Should be fine to take the first element, the pages seem fairly unchanging.
            description = soup.find('h1', {'itemprop': 'name'}).text.replace('\n', ' ')

            # Format of date is d MMM, yyyy
            rx = re.compile(r"(?<=Sold on )([A-Za-z 0-9,])+(?= for)")
            sale_date = soup.find('div', {'class': 'item-info' }).find_all('div', {'class': 'section-headline'})[0].text
            sale_date = datetime.strptime(rx.search(sale_date)[0], '%b %d, %Y')

            """
            The page writes the value of the bid using encoded data for some reason, I suppose in an attempt to stop scraping?
            This took like 30 seconds to work-around, not too sure what's happening there to be honest.
            """
            rx = re.compile(r"(?<=')([0-9;&#]+)(?=')")
            raw_sale_price = soup.find('strong', {'class': 'opening-bid'}).find('script')
            raw_sale_price = rx.search(str(raw_sale_price.contents[0]))[0]
            sale_price = float(re.sub(r'[,\$]', '', html.unescape(raw_sale_price)))

            rx = re.compile(r'\b[0-9]+\b')
            sale_id, lot_id = tuple(rx.findall(filepath))

            sale = {
                'sale_date': sale_date,
                'sale_price': sale_price,
                'description': description,
                'sale_id': sale_id,
                'lot_id': lot_id
            }

            sales.append(sale)
    except AttributeError as exception:
        """
        Not handling exceptions btw, this was just for debugging.
        """
        raise AttributeError(exception, filepath)

with Pool() as pool:
    files = os.listdir(base_dir)
    list(tqdm(pool.imap_unordered(process_file, files), total=len(files)))


# We only care about Wata graded games, so this means we can quickly filter out the non-Wata games which leaves us 9286/9637 sales.
wata = []

for sale in sales:
    sale_tuple = tuple(i.strip() for i in sale['description'].replace(' - Wata', 'Wata').split('Wata'))
    if len(sale_tuple) == 2:
        wata.append((sale, *sale_tuple))


sales = []

grade_rx = re.compile(r'\b[0-9].[0-9]\b')
seal_grade_rx = re.compile(r'\b(CIB|[ABC]{1})[\+]*(?!=[^ \+])')
seal_type_rx = re.compile(r'SEALED|GLUE SEAL|NO SEAL|LOOSE CART')
variant_rx = re.compile(r'VARIANT: ')
# {'9.4', '7.0', '2.5', '9.2', '3.0', '9.6', '6.0', '6.5', '5.0', '7.5', '4.5', '5.5', '8.0', '9.0', '4.0', None, '9.8', '8.5', '3.5'}

seal_comments = set()

for index, sale in enumerate(wata):
    sale, title, grading = sale

    # The titles have bunch of random comments sometimes, this gets rid of that.
    comment_rx = re.compile(r'\b(CIB|(?<!=\d)[ABC]{1})[\+]*(?!=[^\+])')
    comments = [*re.findall(comment_rx, title), *re.findall(comment_rx, grading)]

    clean_up_rx = re.compile(r'\.{3,}|\.$|[\[\(].+[\]\)]|,')
    cleaned = re.sub(clean_up_rx, '', sale.get('description'))
    cleaned = cleaned.split('Wata')[-1]
    cleaned = re.sub(r'[ ]{1,}', ' ', cleaned)
    cleaned = re.sub(r'\. ', '', cleaned)
    cleaned = cleaned.strip().upper()

    grade = re.search(grade_rx, sale.get('description'))
    if grade:
        grade = grade[0]

    seal_grade = re.search(seal_grade_rx, sale.get('description'))
    if seal_grade:
        seal_grade = seal_grade[0].strip()

    seal_type = re.search(seal_type_rx, cleaned)
    if seal_type:
        seal_type = seal_type[0]

    variant = re.search(variant_rx, cleaned)
    if variant:
        variant = True

    _sale = {
        **sale,
        'description': cleaned,
        'comments': re.sub(r'[\[\(\]\)]', '', ", ".join(comments)),
        'title': re.sub(r'[\[\(].+[\]\)]', '', title).strip().upper(),
        'grade': grade,
        'seal_grade': seal_grade,
        'seal_type': seal_type,
        'variant': variant or False
    }

    sales.append(_sale)

df = pd.DataFrame().from_dict(sales)
df.to_csv('heritage_sales.csv', index=False)
