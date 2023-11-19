import scrapy
import json
import re


class DouglasSpider(scrapy.Spider):
    name = "douglas"
    start_urls = [
        "https://www.douglas.de/de/c/make-up/03",
    ]

    def parse(self, response):
        with open('product_file.json', 'a', encoding='utf-8') as json_file:
            product_links = [f"https://www.douglas.de{link}" for link in response.css(
                '.link.link--no-decoration.product-tile__main-link::attr(href)').getall()]

            for link in product_links:
                yield response.follow(link, callback=self.parse_product)

        # next_page = response.css(
        #     'a.pagination__arrow:nth-child(3)::attr(href)').get()
        # if next_page:
        #     yield response.follow(next_page, callback=self.parse)

    def parse_product(self, response):
        categories = self.get_categories(response)

        layout = "Unknown"

        dropdown_option = response.css(
            '.jTgWzCuC1cZq8Y_oK1ex.dropdown__option')
        variant_rows = response.css(
            '.product-detail__variant-row.product-detail__variant-row--spread-content')

        if dropdown_option:
            layout = "Dropdown"
        elif variant_rows:
            if len(variant_rows) == 1:
                layout = "Single-Product"
            else:
                layout = "Checkbox"

        if layout in ["Checkbox", "Dropdown"]:
            product_type = "configurable"
        elif layout == "Single-Product":
            product_type = "simple"
        else:
            product_type = "Unknown"

        categories_url = response.url
        product_link = categories_url.split('?')[0]

        if layout == "Single-Product":
            variant_single_product_sku = response.css(
                '.classification__item::text').re_first(r'\d+')
            variant_checkbox_sku = ""
            variant_dropdown_sku = ""
        elif layout == "Checkbox":
            variant_single_product_sku = ""
            variant_checkbox_sku = response.css(
                '.product-detail__size-variants .radio-group__button input::attr(id)').getall()
            variant_dropdown_sku = ""
        elif layout == "Dropdown":
            variant_single_product_sku = ""
            variant_checkbox_sku = ""
            variant_dropdown_sku = response.css(
                '.variant-selector ul li::attr(data-code)').getall()

        variant_urls = []

        if layout == "Single-Product":
            for sku in variant_single_product_sku.split(","):
                variant_url = f"{product_link}?variant={sku.strip()}"
                variant_urls.append(variant_url)
        elif layout == "Checkbox":
            for sku in variant_checkbox_sku:
                variant_url = f"{product_link}?variant={sku}"
                variant_urls.append(variant_url)
        elif layout == "Dropdown":
            for sku in variant_dropdown_sku:
                variant_url = f"{product_link}?variant={sku}"
                variant_urls.append(variant_url)

        sku = []

        if layout == "Single-Product":
            if variant_single_product_sku:
                sku = [x.strip()
                       for x in variant_single_product_sku.split(",")]
        elif layout == "Checkbox":
            if variant_checkbox_sku:
                sku = [x.strip() for x in variant_checkbox_sku]
        elif layout == "Dropdown":
            if variant_dropdown_sku:
                sku = [x.strip() for x in variant_dropdown_sku]

        name = " ".join(response.css('.link--text.brand-line::text').getall() +
                        response.css('span.header-name::text').getall())

        description = response.css(
            '.product-details__description div').extract_first()

        if description:
            description = re.sub(r'[\n\s]+', ' ', description)
            description = re.sub(r'class="[^\"]+"', '', description)
            description = re.sub(r'id="[^\"]+"', '', description)
            description = re.sub(
                r'<[^>]*>', lambda x: re.sub(r'\s+', '', x.group()), description)
            description = description.replace("c-", '')
            description = description.replace(
                '<divdata-test><divclass=\"\"><div>', '')
            description = re.sub(r'</div>', '', description)
            description = description.replace(" <br> ", "<br>")
            description = description.replace(" </p>", "</p>")
            description = description.strip()

        manufacturer = response.css('.brand-logo__text.brand-logo__text--dynamic::text').get(
        ) or response.css('.brand-logo__text.brand-logo__text--fixed::text').get()
        manufacturer = manufacturer.replace("ô", "o")

        manufacturer_picture_url = response.css(
            ".brand-logo img::attr(src)").get()
        if not manufacturer_picture_url:
            manufacturer_picture_url = "Null"  # Set to "Null" if not found

        product_data = {
            "seller": 'Douglas',
            "categories_url": categories_url,
            "product_link": product_link,
            "categories": categories,
            "layout": layout,
            "product_type": product_type,
            "variant_single_product_sku": variant_single_product_sku,
            "variant_checkbox_sku": variant_checkbox_sku,
            "variant_dropdown_sku": variant_dropdown_sku,
            "variant_urls": variant_urls,
            "sku": sku,  # Speichern Sie die sku hier als Liste
            "price": [None] * len(variant_urls),
            "special_price": [None] * len(variant_urls),
            "manufacturer": manufacturer,
            "manufacturer_picture_url": manufacturer_picture_url,
            "name": name,
            "short_description": [None] * len(variant_urls),
            "description": description,
            "color": [None] * len(variant_urls),
            # Initialize an empty list for base image URLs
            "base_image": [None] * len(variant_urls),
            # Initialize an empty list for the picture URLs
            "additional_images": [None] * len(variant_urls),
            # visibility = 'Katalog, Suche' // visibility = 'Nicht individuell sichtbar'
            "visibility": "Catalog, Search",
            "additional_attributes": [],
            "configurable_variations": [],
            "configurable_variation_labels": [],  # New column for variation labels
            # Set the product_categories here
            "product_categories": response.css('.third-line::text').get(),
            "attribute_set_code": "Default",
            "product_websites": 'base',
            "product_online": '1',
            "tax_class_name": 'Vollbesteuerte Artikel',
            "color_picture_url": [None] * len(variant_urls),
            "display_product_options_in": 'Block nach Info-Spalte',
            "product_options_container": 'Block nach Info-Spalte',
            "msrp_display_actual_price_type": 'Konfiguration verwenden',
            "update_attribute_set": '1',

        }

        for i, variant_url in enumerate(variant_urls):
            try:
                yield scrapy.Request(
                    variant_url,
                    callback=self.parse_variant,
                    meta={'product_data': product_data, 'variant_index': i},
                    dont_filter=True  # Hier bleibt die Option erhalten
                )
            except Exception as e:
                self.handle_error(repr(e))

    def handle_error(self, error_message):
        # Hier wird aufgerufen, wenn ein Fehler auftritt
        self.logger.error(error_message)

        # Protokollieren Sie den Fehler in einer Datei
        with open('error.txt', 'a', encoding='utf-8') as error_file:
            error_file.write(error_message)
            error_file.write('\n')

    def parse_variant(self, response):
        product_data = response.meta['product_data']
        layout = product_data['layout']
        variant_index = response.meta['variant_index']

        base_image = response.css(
            '.douglas-swiper-carousel.douglas-swiper-carousel--vertical img[data-lazy-src^="https://media.douglas.de/medias/"]::attr(data-lazy-src)').extract_first()
        # Wenn kein base_image gefunden wird, verwende den nächsten Selector
        if not base_image:
            base_image = response.css("img.zoom-img::attr(src)").get()
        product_data.setdefault(
            "base_image", [None] * len(product_data["variant_urls"]))
        product_data["base_image"][variant_index] = base_image

        additional_images = response.css(
            '.douglas-swiper-carousel.douglas-swiper-carousel--vertical img[data-lazy-src^="https://media.douglas.de/medias/"]::attr(data-lazy-src)').extract()

        # Entferne doppelte Elemente aus additional_images, ohne die Reihenfolge zu ändern
        additional_images = list(dict.fromkeys(additional_images))

        # Entferne doppelte Elemente aus additional_images, die auch in base_image vorhanden sind
        additional_images = [
            url for url in additional_images if url not in base_image]

        # Stelle sicher, dass die Reihenfolge beibehalten wird
        product_data.setdefault("additional_images", [
                                None] * len(product_data["variant_urls"]))
        product_data["additional_images"][variant_index] = additional_images

    # Remove duplicates from additional_images
        # base_image_set = {base_image}
        # unique_additional_images = list(base_image_set.union(set(additional_images)))
        # product_data["additional_images"][variant_index] = unique_additional_images

        # Hinzufügen von "DG-" vor dem SKU-Wert
        sku_prefix = "DG-"
        modified_skus = [f"{sku_prefix}{sku}" if not sku.startswith(
            sku_prefix) else sku for sku in product_data['sku']]

        # Aktualisieren Sie die SKU-Werte im product_data
        product_data["sku"] = modified_skus

        new_prices = product_data.get(
            "special_price", [None] * len(product_data["variant_urls"]))
        price = product_data.get(
            "price", [None] * len(product_data["variant_urls"]))

        new_price, old_price = self.extract_prices(response, layout)

        if new_price:
            new_prices[variant_index] = self.clean_price(new_price)
        else:
            new_prices[variant_index] = 'None'
        if old_price:
            price[variant_index] = self.clean_price(old_price)
        else:
            price[variant_index] = 'None'

        # If "price" is None or 'None', use the value from "special_price"
        if price[variant_index] in (None, 'None') and new_prices[variant_index] is not None:
            price[variant_index] = new_prices[variant_index]

        # Compare "price" and "special_price" values
        if price[variant_index] == new_prices[variant_index]:
            new_prices[variant_index] = 'None'

        product_data["special_price"] = new_prices
        product_data["price"] = price

        size = response.css(
            ".product-detail__variant--selected .product-detail__variant-name::text").get() or "1 Stück"
        product_data.setdefault(
            "size", [None] * len(product_data["variant_urls"]))
        product_data["size"][variant_index] = size

        # Extract color_name
        color_name = response.css(
            ".jTgWzCuC1cZq8Y_oK1ex.dropdown__option span::text").get()
        product_data.setdefault(
            "color", [None] * len(product_data["variant_urls"]))
        product_data["color"][variant_index] = color_name

        # Extrahiere die Kurzbeschreibung (short_description)
        short_description_items = response.css(
            "div.product-detail-content ul.bullet-points li::text").getall()
        short_description = ''.join(
            [f'<li>{item}</li>' for item in short_description_items])

        product_data.setdefault("short_description", [
                                None] * len(product_data["variant_urls"]))
        product_data["short_description"][variant_index] = short_description

        # Scrape stock availability
        is_in_stock = self.scrape_variant_availability(response, layout)
        product_data.setdefault(
            "is_in_stock", [None] * len(product_data["variant_urls"]))
        product_data["is_in_stock"][variant_index] = is_in_stock

        qty = "1000000" if is_in_stock == "1" else "0.0000"
        product_data.setdefault(
            "qty", [None] * len(product_data["variant_urls"]))
        product_data["qty"][variant_index] = qty

        # Scrape and clean color picture URLs for variants
        color_picture_url = response.css(
            'div.variant-selector__color-blobs-expandable ul._GHuURijTCPQoJc35YaG li[aria-selected="true"] img.image.bRQvLdW_iB_MVZ4XN4mZ::attr(src)').extract()

        if len(color_picture_url) > 0:
            product_data.setdefault("color_picture_url", [
                                    None] * len(product_data["variant_urls"]))
            product_data["color_picture_url"][variant_index] = color_picture_url[0]
        else:
            product_data.setdefault("color_picture_url", [
                                    None] * len(product_data["variant_urls"]))
            product_data["color_picture_url"][variant_index] = None

        # Hinzufügen von "additional_attributes" basierend auf dem "layout"
        additional_attributes = []

        if layout == "Single-Product":
            if product_data['variant_single_product_sku']:
                for sku, size in zip(product_data['variant_single_product_sku'].split(','), product_data['size']):
                    if sku and size:
                        additional_attributes.append(
                            f"sku=DG-{sku},size={size}")

        elif layout == "Checkbox":
            if product_data['variant_checkbox_sku']:
                for sku, size in zip(product_data['variant_checkbox_sku'], product_data['size']):
                    if sku and size:
                        additional_attributes.append(
                            f"sku=DG-{sku},size={size}")

        elif layout == "Dropdown":
            if product_data['variant_dropdown_sku']:
                for sku, size, color in zip(product_data['variant_dropdown_sku'], product_data['size'], product_data['color']):
                    if sku and size and color:
                        additional_attributes.append(
                            f"sku=DG-{sku},size={size},color={color}")

        # Fügen Sie die "additional_attributes" Zeilen dem product_data hinzu
        product_data["additional_attributes"] = additional_attributes

        configurable_variations = []

        if layout == "Single-Product":
            if product_data['variant_single_product_sku']:
                for sku, size in zip(product_data['variant_single_product_sku'].split(','), product_data['size']):
                    if sku and size:
                        configurable_variations.append(
                            f"sku=DG-{sku},size={size}")

        elif layout == "Checkbox":
            if product_data['variant_checkbox_sku']:
                for sku, size in zip(product_data['variant_checkbox_sku'], product_data['size']):
                    if sku and size:
                        configurable_variations.append(
                            f"sku=DG-{sku},size={size}")

        elif layout == "Dropdown":
            if product_data['variant_dropdown_sku']:
                for sku, size, color in zip(product_data['variant_dropdown_sku'], product_data['size'], product_data['color']):
                    if sku and size and color:
                        configurable_variations.append(
                            f"sku=DG-{sku},size={size},color={color}")

        # Konvertiere die Liste der Konfigurationen in eine einzelne Zeichenfolge mit "|" getrennten Werten
        configurable_variations_str = "|".join(configurable_variations)
        product_data["configurable_variations"] = configurable_variations_str

        configurable_variation_labels = []

        if layout == "Single-Product":
            configurable_variation_labels.append("size=Größe")

        elif layout == "Checkbox":
            configurable_variation_labels.append("size=Größe")

        elif layout == "Dropdown":
            configurable_variation_labels.append("size=Größe;color=Farbe")

        # Konvertiere die Liste der Konfigurationen in eine einzelne Zeichenfolge mit "|" getrennten Werten
        configurable_variations_str = "|".join(configurable_variation_labels)
        product_data["configurable_variation_labels"] = configurable_variations_str

        # Sortieren Sie die Schlüssel im product_data-Dictionary
        desired_order = [
            "seller",
            "categories_url",
            "product_link",
            "categories",
            "layout",
            "product_type",
            "variant_single_product_sku",
            "variant_checkbox_sku",
            "variant_dropdown_sku",
            "variant_urls",
            "sku",
            "price",
            "special_price",
            "manufacturer",
            "manufacturer_picture_url",
            "name",
            "description",
            "short_description",
            "size",  # Add "size" to the desired_order list
            "color",
            "is_in_stock",  # Füge diese Zeile hinzu
            "qty",  # Füge diese Zeile hinzu
            "base_image",
            "additional_images",
            "visibility",
            "additional_attributes",
            "configurable_variations",
            "configurable_variation_labels",  # Added "configurable_variation_labels"
            "product_categories",
            "attribute_set_code",
            "product_websites",
            "product_online",
            "tax_class_name",
            "color_picture_url",
            "display_product_options_in",
            "product_options_container",
            "msrp_display_actual_price_type",
            "update_attribute_set",
        ]

        sorted_product_data = {key: product_data[key] for key in desired_order}
        if None not in new_prices and None not in price:
            self.logger.info(product_data)

            with open('product_file.json', 'a', encoding='utf-8') as json_file:
                json.dump(sorted_product_data, json_file, ensure_ascii=False,
                          indent=4, separators=(',', ': '))
                json_file.write('\n')

    def scrape_variant_availability(self, response, layout):
        if layout == "Single-Product":
            availability_text = response.css(
                ".delivery-info__availability::text").get()
        elif layout == "Checkbox":
            availability_text = response.css(
                ".delivery-info__availability::text").get()
        elif layout == "Dropdown":
            availability_text = response.css(
                ".delivery-info__availability::text").get()

        # Check if "Online auf Lager" is in the availability text
        if "Online auf Lager" in availability_text:
            return "1"
        # Check if "Demnächst wieder lieferbar" is in the availability text
        elif "Demnächst wieder lieferbar" in availability_text:
            return "0"
        else:
            return availability_text  # Return the original text if neither is found

    def extract_prices(self, response, layout):
        new_price, old_price = None, None

        if layout == "Single-Product":
            new_price = response.css(
                ".product-price__discount--discount-color .product-price__price::text").get()
            if not new_price:
                new_price = response.css(
                    ".product-price__discount span.product-price__price::text").get()
            old_price = response.css(
                ".product-price__original span.product-price__price::text").get()
            if not new_price:
                new_price = response.css(
                    ".product-detail__variant--selected .product-price__base span.product-price__price::text").get()

        elif layout == "Checkbox":
            new_price = response.css(
                ".product-detail__variant--selected .product-price__discount.product-price__discount--discount-color .product-price__price::text").get()
            if not new_price:
                new_price = response.css(
                    ".product-detail__variant--selected .product-price__base span.product-price__price::text").get()
            if not new_price:
                new_price = response.css(
                    ".product-detail__variant--selected .product-price__discount span.product-price__price::text").get()
            old_price = response.css(
                ".product-detail__variant--selected .product-price__strikethrough.product-price__original .product-price__price::text").get()

        elif layout == "Dropdown":
            new_price = response.css(
                ".product-detail__variant--selected .product-price__discount.product-price__discount--discount-color .product-price__price::text").get()
            if not new_price:
                new_price = response.css(
                    ".product-detail__variant--selected .product-price__base span.product-price__price::text").get()
            if not new_price:
                new_price = response.css(
                    ".product-detail__variant--selected .product-price__discount span.product-price__price::text").get()
            old_price = response.css(
                ".product-detail__variant--selected .product-price__strikethrough.product-price__original .product-price__price::text").get()

        # Replace commas with dots in the prices
        new_price = self.clean_price(new_price)
        old_price = self.clean_price(old_price)

        return new_price, old_price

    def clean_price(self, price):
        if price is not None:
            # Replace commas with dots and remove Euro symbol
            cleaned_price = price.replace(",", ".").replace("€", "").strip()
            return cleaned_price
        else:
            return None

    def get_categories(self, response):
        categories = "/".join(response.css(
            "span.breadcrumb__entry > a::text").getall())
        return categories

    def closed(self, reason):
        pass
