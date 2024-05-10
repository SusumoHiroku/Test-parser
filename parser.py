import asyncio
from requests_html import AsyncHTMLSession
import xml.etree.ElementTree as ET
from urllib.parse import urljoin


async def get_product_links(session, base_url, current_page=1):
    max_links = 120
    collected_links = []

    while len(collected_links) < max_links:
        url = f"{base_url}?page={current_page}&view=96&sort=3"
        print(f"Парсинг страницы: {url}")

        try:
            response = await session.get(url)
            await response.html.arender(timeout=50)

            product_list = response.html.find('ul[data-testid="product-card-list"]')
            if product_list:
                link_elements = product_list[0].find('a')
                for link_element in link_elements:
                    href = link_element.attrs['href']
                    collected_links.append(urljoin(base_url, href))
                    print(f"Добавлена ссылка: {urljoin(base_url, href)}")

                    if len(collected_links) >= max_links:
                        return collected_links

            next_page_link = get_next_page_link(response, current_page)
            if not next_page_link:
                break
            current_page += 1

        except Exception as e:
            print(f"Ошибка при парсинге страницы: {e}")

    return collected_links


def get_next_page_link(response, current_page):
    try:
        next_page_url = response.url.split('?')[0] + f"?page={current_page + 1}&view=96&sort=3"
        print("Next page link:", next_page_url)
        return next_page_url
    except Exception as e:
        print(f"Ошибка при поиске ссылки на следующую страницу: {e}")
        return None


async def get_product_details(session, product_url):
    try:
        response = await session.get(product_url)
        await response.html.arender(timeout=50)

        product_details = {}

        farfetch_id_element = response.html.find('p[data-component="Body"]', containing="FARFETCH ID:", first=True)
        if farfetch_id_element:
            farfetch_id = farfetch_id_element.find('span', first=True).text.strip()
            product_details['id'] = farfetch_id

        brand_style_id_element = response.html.find('p[data-component="Body"]', containing="Brand style ID:",
                                                    first=True)
        if brand_style_id_element:
            brand_style_id = brand_style_id_element.find('span', first=True).text.strip()
            product_details['item_group_id'] = brand_style_id
            product_details['mpn'] = brand_style_id

        title_element = response.html.find('p[data-testid="product-short-description"]', first=True)
        if title_element:
            brand_element = response.html.find('a[data-ffref="pp_infobrd"]', first=True)
            if brand_element:
                brand = brand_element.text.strip()
                description = title_element.text.strip()
                title = f"{brand} {description}"
                product_details['title'] = title

        image_element = response.html.find('img[data-component="Img"]', first=True)
        if image_element:
            image_alt = image_element.attrs.get('alt')
            if image_alt:
                image_link = image_element.attrs.get('src')
                product_details['image_link'] = image_link

        link_element = response.html.find('meta[property="og:url"]', first=True)
        if link_element:
            link = link_element.attrs.get('content')
            product_details['link'] = link

        product_details['gender'] = 'female'

        brand_element = response.html.find('a[data-ffref="pp_infobrd"]', first=True)
        if brand_element:
            brand = brand_element.text.strip()
            product_details['brand'] = brand

        availability_element = response.html.find('meta[property="og:availability"]', first=True)
        if availability_element:
            availability = availability_element.attrs.get('content')
            product_details['availability'] = availability

        color_element = response.html.find('div.ltr-fzg9du.e1yiqd0 li.ltr-4y8w0i-Body', first=True)
        if color_element:
            color = color_element.text.strip()
            product_details['color'] = color

        price_element = response.html.find('div[data-component="PriceCallout"] p[data-component="PriceLarge"]',
                                           first=True)
        if price_element:
            price = price_element.text.strip()
            product_details['price'] = price

        breadcrumbs_element = response.html.find(
            'nav[data-component="BreadcrumbsNavigation"] ol[data-component="Breadcrumbs"]', first=True)
        if breadcrumbs_element:
            breadcrumbs_list = breadcrumbs_element.find('li[data-component="BreadcrumbWrapper"]')
            product_type = ""
            for crumb in breadcrumbs_list:
                category = crumb.find('a', first=True).text.strip()
                if product_type:
                    product_type += " > "
                product_type += category
            product_details['product_type'] = product_type

        google_product_category = "2271"
        product_details['google_product_category'] = google_product_category

        return product_details

    except Exception as e:
        print(f"Ошибка при получении деталей товара {product_url}: {e}")
        return None


async def write_batch_to_file(batch_data, file, website_link):
    # Создаем новое дерево XML
    channel = ET.Element("channel")
    tree = ET.ElementTree(channel)

    # Добавляем элементы <title> и <description> в корневой элемент <channel>
    title = ET.SubElement(channel, "title")
    title.text = "FARFETCH"
    description = ET.SubElement(channel, "description")
    description.text = "FARFETCH UK"
    link = ET.SubElement(channel, "link")
    link.text = website_link

    # Создаем элемент <item> для каждой записи в пакете
    for product_data in batch_data:
        item = ET.SubElement(channel, "item")
        # Добавляем элементы <item>
        for key, value in product_data.items():
            sub_element = ET.SubElement(item, key)
            sub_element.text = value

    tree.write(file, encoding="utf-8", xml_declaration=True, method="xml")


async def process_parsing(category_url, file, website_link,  batch_size=120):
    session = AsyncHTMLSession()
    print('Сайт:', category_url)
    current_page = 1
    product_links = await get_product_links(session, category_url, current_page)
    print(f"Найдено товаров: {len(product_links)}")
    products_data = []
    for product_url in product_links:
        product_data = await get_product_details(session, product_url)
        if product_data:
            products_data.append(product_data)
            # Проверяем, достигли ли мы размера пакета для записи в файл
            if len(products_data) >= batch_size:
                await write_batch_to_file(products_data, file, website_link)
                print(f"Записано {len(products_data)} товаров в файл.")
                # Очищаем список для следующего пакета
                products_data = []
    # Записываем оставшиеся данные, если они есть
    if products_data:
        await write_batch_to_file(products_data, file, website_link)
        print(f"Записано {len(products_data)} товаров в файл.")
    print("Все данные записаны в файл.")


if __name__ == '__main__':
    category_url = 'https://www.farfetch.com/ca/shopping/women/dresses-1/items.aspx'
    file_name = "products.xml"
    website_link = 'https://www.farfetch.com/'
    asyncio.run(process_parsing(category_url, file=file_name, website_link=website_link))
