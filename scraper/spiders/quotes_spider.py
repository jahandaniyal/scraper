import scrapy
import dateparser
import json
import logging
import pandas as pd
import re
import requests

from bs4 import BeautifulSoup
from bs4.element import Comment

from transform import *


log = logging.getLogger(__name__)
file_out = open('data\\data_out.json', 'a+')  # Use file to refer to the file object
file_out.truncate(0)
# transform_data(df)
# file_out.close()

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    # start_urls = [
    #     'http://quotes.toscrape.com/page/1/',
    #     'http://quotes.toscrape.com/page/2/',
    # ]

    urls = []

    df = pd.read_csv("../data/raw/data_out.csv")

    # for row in df.itertuples(index=False):
    #     urls.append(row.claimReview_url)

    def start_requests(self):
        # urls = self.urls
        for row in self.df.to_dict(orient="records"):
            yield scrapy.Request(url=row['claimReview_url'], callback=self.parse, cb_kwargs=row)

    def parse(self, response, **kwargs):
        # log.info(response.cb_kwargs['extra_title'])
        transform_data(response)
        # page = response.url.split("/")[-2]
        # filename = 'data\\parsed-%s.html' % page
        # with open(filename, 'wb') as f:
        #     f.write(response.body)


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


def transform_data(response):
    """
    Extract page url, Title and date published from CSV
    and use the URL to fetch HTML.
    Match title and date published with HTML doc and save the indices of matched
    fields in data_out.json file
    :param:
        df: Type [Pandas dataframe] DF on input CSV file.
    :return:
        None
    """
    data_json = []
    file_out = open('data\\data_out.json', 'a+')
    row = Struct(**response.cb_kwargs)
    if row.language != 'en':
        pass
    else:
        df_labels = []
        soup = BeautifulSoup(response.body, 'html.parser')
        texts = soup.findAll(text=True)
        visible_texts = filter(tag_visible, texts)
        cleaned_body = u" ".join(t.strip() for t in visible_texts)
        cleaned_body = u" ".join(cleaned_body.split())

        # Use this only if we need to preserve HTML Tags
        # cleaned_body = str(soup.contents)

        match_title(cleaned_body, df_labels, row)

        match_date_published(cleaned_body, df_labels, row, soup)

        match_claim(cleaned_body, df_labels, row)

        match_tags(cleaned_body, df_labels, row)

        # data_json.append({'text': cleaned_body, 'labels': df_labels})
        file_out.write(json.dumps({'text': cleaned_body, 'labels': df_labels}) + "\n")
        file_out.close()
