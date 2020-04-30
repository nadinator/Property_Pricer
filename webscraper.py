# Got a bit of help for getting proxies and rotating them from:
#   https://codelike.pro/create-a-crawler-with-rotating-ip-proxy-in-python/

import requests
import random
import math
from bs4 import BeautifulSoup
from csv import writer
import time

headers = ['Value', 'Type', 'Reference', 'Bedrooms', 'Bathrooms', 'Furnishing', 'Size', 'Amenities', 'Agency', 'Location']
dataset = [] # List of entries; Entry as list of features (seen in headers ^)
bad_types = ['Whole Building', 'Bulk Rent Units']

delays = [7, 4, 6, 2, 10, 19] # lengths of time we might wait before making a request

request_nr = 0 # Counts the number of requests we make
proxies = [] # Will contain proxies [ip, port]
# proxies = [{'ip': '174.138.74.211', 'port': '8080'}, {'ip': '81.91.144.53', 'port': '8080'}, {'ip': '197.210.217.66', 'port': '36494'}, {'ip': '102.164.222.60', 'port': '40013'}, {'ip': '210.210.128.166', 'port': '48922'}, {'ip': '182.54.207.74', 'port': '38877'}, {'ip': '144.76.214.152', 'port': '1080'}, {'ip': '136.243.92.25', 'port': '1080'}, {'ip': '202.91.77.122', 'port': '49878'}, {'ip': '42.115.88.12', 'port': '58399'}, {'ip': '144.76.214.156', 'port': '1080'}, {'ip': '188.40.183.189', 'port': '1080'}, {'ip': '80.187.140.26', 'port': '8080'}, {'ip': '200.52.141.162', 'port': '53281'}, {'ip': '92.51.149.81', 'port': '3128'}, {'ip': '188.40.183.186', 'port': '1080'}, {'ip': '177.139.193.145', 'port': '61741'}, {'ip': '111.95.23.253', 'port': '3128'}, {'ip': '197.255.162.162', 'port': '55062'}, {'ip': '92.247.2.26', 'port': '21231'}, {'ip': '177.241.245.226', 'port': '37415'}, {'ip': '188.40.183.185', 'port': '1080'}, {'ip': '128.199.246.10', 'port': '44344'}, {'ip': '187.87.38.28', 'port': '53281'}, {'ip': '201.20.98.22', 'port': '37596'}, {'ip': '117.58.241.164', 'port': '52636'}, {'ip': '110.74.199.16', 'port': '63141'}, {'ip': '172.254.124.231', 'port': '3128'}, {'ip': '61.118.35.94', 'port': '55725'}, {'ip': '41.190.95.20', 'port': '56167'}, {'ip': '170.238.255.90', 'port': '34586'}, {'ip': '193.178.50.49', 'port': '3128'}, {'ip': '181.52.85.249', 'port': '36107'}, {'ip': '118.175.93.171', 'port': '32866'}, {'ip': '177.94.225.58', 'port': '55674'}, {'ip': '54.38.110.35', 'port': '47640'}, {'ip': '65.152.119.226', 'port': '47831'}, {'ip': '139.255.42.156', 'port': '43771'}, {'ip': '118.175.207.183', 'port': '42517'}, {'ip': '185.49.130.81', 'port': '48377'}, {'ip': '1.20.102.228', 'port': '61606'}, {'ip': '110.77.207.83', 'port': '45619'}, {'ip': '187.108.86.40', 'port': '30175'}, {'ip': '92.247.142.14', 'port': '53281'}, {'ip': '144.76.214.154', 'port': '1080'}, {'ip': '166.249.185.133', 'port': '45607'}, {'ip': '85.10.219.96', 'port': '1080'}, {'ip': '181.117.176.236', 'port': '61358'}, {'ip': '103.129.113.194', 'port': '31195'}, {'ip': '200.60.79.11', 'port': '53281'}, {'ip': '103.9.188.151', 'port': '38439'}, {'ip': '103.115.116.17', 'port': '36165'}, {'ip': '168.181.134.119', 'port': '52351'}, {'ip': '77.46.138.1', 'port': '30762'}, {'ip': '190.181.62.90', 'port': '33195'}, {'ip': '96.9.80.62', 'port': '45557'}, {'ip': '78.29.42.10', 'port': '4550'}, {'ip': '1.20.100.133', 'port': '46755'}, {'ip': '138.122.140.35', 'port': '3128'}, {'ip': '67.60.137.219', 'port': '35979'}, {'ip': '191.98.184.124', 'port': '42221'}, {'ip': '110.232.74.233', 'port': '40661'}, {'ip': '101.51.106.70', 'port': '47390'}, {'ip': '200.73.128.5', 'port': '8080'}, {'ip': '91.207.185.235', 'port': '45359'}, {'ip': '178.151.156.112', 'port': '34689'}, {'ip': '72.35.40.34', 'port': '8080'}, {'ip': '128.199.241.229', 'port': '44344'}, {'ip': '124.41.213.201', 'port': '42580'}, {'ip': '78.46.81.7', 'port': '1080'}, {'ip': '128.199.252.41', 'port': '44344'}, {'ip': '176.195.22.156', 'port': '47099'}, {'ip': '189.57.62.146', 'port': '80'}, {'ip': '128.199.245.99', 'port': '44344'}, {'ip': '186.232.48.98', 'port': '40052'}, {'ip': '41.215.180.237', 'port': '57797'}, {'ip': '143.255.52.102', 'port': '31158'}, {'ip': '190.144.116.34', 'port': '36742'}, {'ip': '118.200.73.124', 'port': '8080'}, {'ip': '36.89.229.13', 'port': '45843'}, {'ip': '134.249.167.184', 'port': '53281'}, {'ip': '111.92.240.134', 'port': '30598'}, {'ip': '125.27.251.82', 'port': '50574'}, {'ip': '200.94.140.50', 'port': '36017'}, {'ip': '190.171.180.162', 'port': '32709'}, {'ip': '176.28.75.193', 'port': '53737'}, {'ip': '193.85.228.178', 'port': '43036'}, {'ip': '31.204.180.44', 'port': '53281'}, {'ip': '117.102.104.131', 'port': '61988'}, {'ip': '210.56.245.77', 'port': '8080'}, {'ip': '187.109.114.181', 'port': '56388'}, {'ip': '147.75.126.171', 'port': '49559'}, {'ip': '103.15.167.38', 'port': '41787'}, {'ip': '200.108.183.2', 'port': '8080'}, {'ip': '186.47.46.30', 'port': '33556'}, {'ip': '181.10.210.99', 'port': '44252'}, {'ip': '109.245.239.125', 'port': '55311'}, {'ip': '110.5.100.130', 'port': '54300'}, {'ip': '188.40.183.184', 'port': '1080'}, {'ip': '188.40.183.188', 'port': '1080'}]

diff_auth = ['http', 'https']


def get_proxies():
    global proxies

    response = requests.get("https://sslproxies.org/")
    soup = BeautifulSoup(response.content, 'html.parser')
    proxies_table = soup.find(id='proxylisttable')

    # Save proxies in the list
    for row in proxies_table.tbody.find_all('tr'):
        proxies.append({
            'ip': row.find_all('td')[0].string,
            'port': row.find_all('td')[1].string
        })

def get_random_proxy():
    proxy_nr = len(proxies)
    proxy_index = random.randint(0, proxy_nr-1)
    proxy = proxies[proxy_index]
    proxy_str = proxy['ip'] + ':' + proxy['port']
    proxy_dict = {}
    for auth in diff_auth:
        proxy_dict[auth] = auth + '://' + proxy_str

    return (proxy_dict, proxy_index)

get_proxies()
(proxy, proxy_index) = get_random_proxy()



def get_list_count_per_page(first_soup):
    list_count_per_page = 0
    listings = first_soup.find_all(class_="card-list__item")
    for listing in listings:
        list_count_per_page += 1

    return list_count_per_page


def get_page_count(first_soup):
    list_count = int(first_soup.find(class_="property-header__list-count").get_text().replace(' results', ''))
    list_count_per_page = get_list_count_per_page(first_soup)
    page_count = math.ceil(list_count / list_count_per_page)

    return page_count


def make_csv_file(file_name, headers, data):
    print("----- Making csv file: " + file_name + " -----")
    with open(file_name, 'w') as csv_file:
        csv_writer = writer(csv_file)
        csv_writer.writerow(headers)
        csv_writer.writerows(data)



def request_listing(link):
    global request_nr, proxies, proxy, proxy_index

    # Every 10 requests, generate a new proxy
    if request_nr % 10 == 0:
        (proxy, proxy_index) = get_random_proxy()

    try:
        print('Requesting link now.')
        page = requests.get(link, proxies=proxy)
        print('#' + str(request_nr) + ': ' + link)
        request_nr += 1
        return page

    except:  # If error, delete this proxy and find another one
        del proxies[proxy_index]
        print('Proxy #' + str(proxy_index) + ' deleted.')
        (proxy, proxy_index) = get_random_proxy()



def scrape_listing(link, location, run_proxy=False):
    global dataset

    # delay = random.choice(delays)
    # time.sleep(delay)

    print("Accessing listing on:", link)

    if run_proxy:
        page = request_listing(link)
    else:
        page = requests.get(link)

    if page == None:
        print(" - Could not reach page")
        return

    data = []
    soup = BeautifulSoup(page.text, 'html.parser')

    # getting Value, Type, Reference, Bedrooms, Bathrooms, Furnishing, Size
    facts = soup.find_all(class_="facts__list-item")
    for fact in facts:
        data.append(fact.find(class_="facts__content").get_text().replace('\n', ''))

    # check if this listing is a bad type
    for bad_t in bad_types:
        if bad_t in data:
            print(" - Found bad type, not including it in dataset")
            return

    # getting Amenities
    all_amenities = []
    amenities = soup.find_all(class_="amenities__list-item")
    for amenity in amenities:
        all_amenities.append(amenity.find(class_="amenities__content").get_text())
    all_amenities_str = ' - '.join(all_amenities)
    data.append(all_amenities_str)

    # getting Agency
    all_agent_info = []
    agent_info = soup.find_all(class_="agent-info__detail-content--bold")
    for info in agent_info:
        all_agent_info.append(info.get_text())
    data.append(all_agent_info[1])

    data.append(location)

    print(" - Data:", data)
    dataset.append(data)


def scrape_page(sp, root, run_proxy=False):
    cards = sp.find_all(class_="card-list__item")
    for card in cards:
        listing_link = root + card.find('a', class_="card--clickable", href=True)['href']
        listing_location = card.find(class_="card__location").get_text().replace(',', ' -')
        scrape_listing(listing_link, listing_location, run_proxy)


def scrape(root, link_pre, link_post, file_name, run_proxy=False):
    link1 = link_pre + '1' + link_post
    page1 = requests.get(link1)
    soup1 = BeautifulSoup(page1.text, 'html.parser')

    page_count = get_page_count(soup1)

    print("----- Starting scrape on " + str(page_count) + " pages -----")

    # Iterating through all pages on site
    start_page_nr = 1
    for page_nr in range(start_page_nr, page_count+1):
        print("---------- Accessing page", str(page_nr), "----------")
        link = link_pre + str(page_nr) + link_post
        page = requests.get(link)
        soup = BeautifulSoup(page.text, 'html.parser')
        scrape_page(soup, root, run_proxy)

        # Keep backing up dataset every 5 pages
        if page_nr % 5 == 0:
            file_name_nr = file_name + str(start_page_nr) + "_" + str(page_nr) + ".csv"
            make_csv_file(file_name_nr, headers, dataset)

        print("Size of dataset after scraping page", str(page_nr), ":", str(len(dataset)))

    print("----- Successfully scraped " + str(page_count+1) + "pages -----")


#PF for PropertyFinder

# links are split depending on where the page nr fits in
PF_link_pre = 'https://www.propertyfinder.qa/en/search?c=2&l=9&ob=ba&page='
PF_link_post = '&rp=m'
PF_root = 'https://www.propertyfinder.qa'

PF_file_name = "PF_scrape_"
scrape(PF_root, PF_link_pre, PF_link_post, PF_file_name)
PF_file_name_final = "PF_scrape_final_starting_at_85.csv"
make_csv_file(PF_file_name_final, headers, dataset)
